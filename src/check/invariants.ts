import type { RaftLog } from '../raft/log.js';
import type { RaftNode, RaftObserver } from '../raft/node.js';
import type { LogEntry, NodeId, Role } from '../raft/types.js';

/**
 * Online checker for the safety properties in Figure 3 of the Raft paper.
 * Attach it as a RaftObserver to every node, then call `checkLogMatching`
 * periodically and inspect `violations`.
 *
 *  - Election Safety: at most one leader per term.
 *  - Leader Completeness: a new leader holds every entry committed so far.
 *  - State Machine Safety: no two nodes commit or apply different entries
 *    at the same index.
 *  - Log Matching: if two logs hold an entry with the same index and term,
 *    the logs are identical up to that index.
 */
export class SafetyChecker<C = unknown> implements RaftObserver<C> {
  readonly violations: string[] = [];
  private readonly leaders = new Map<number, NodeId>();
  private readonly committed = new Map<number, number>();
  private readonly applied = new Map<number, { term: number; payload: string; by: NodeId }>();
  leaderElections = 0;

  constructor(private readonly now: () => number = () => 0) {}

  onRoleChange(node: RaftNode<C>, role: Role, term: number): void {
    if (role !== 'leader') return;
    this.leaderElections++;
    const existing = this.leaders.get(term);
    if (existing !== undefined && existing !== node.id) {
      this.fail(`election safety: ${existing} and ${node.id} both leader in term ${term}`);
    }
    this.leaders.set(term, node.id);

    for (const [index, t] of this.committed) {
      if (index <= node.snapshotIndex) continue;
      const have = node.termAt(index);
      if (have !== t) {
        this.fail(
          `leader completeness: ${node.id} leads term ${term} but has term ${have} ` +
            `at committed index ${index} (expected ${t})`,
        );
      }
    }
  }

  onCommit(node: RaftNode<C>, index: number, term: number): void {
    const existing = this.committed.get(index);
    if (existing === undefined) this.committed.set(index, term);
    else if (existing !== term) {
      this.fail(`state machine safety: index ${index} committed with terms ${existing} and ${term} (${node.id})`);
    }
  }

  onApply(node: RaftNode<C>, entry: LogEntry<C>): void {
    const payload = JSON.stringify(entry.payload);
    const existing = this.applied.get(entry.index);
    if (!existing) {
      this.applied.set(entry.index, { term: entry.term, payload, by: node.id });
    } else if (existing.term !== entry.term || existing.payload !== payload) {
      this.fail(
        `state machine safety: ${existing.by} applied ${existing.payload}@t${existing.term} ` +
          `but ${node.id} applied ${payload}@t${entry.term} at index ${entry.index}`,
      );
    }
  }

  /** Pairwise Log Matching over the live (uncompacted) parts of the logs. */
  checkLogMatching(logs: ReadonlyMap<NodeId, RaftLog<C>>): void {
    const ids = [...logs.keys()];
    for (let i = 0; i < ids.length; i++) {
      for (let j = i + 1; j < ids.length; j++) {
        const a = logs.get(ids[i]!)!;
        const b = logs.get(ids[j]!)!;
        const lo = Math.max(a.firstIndex, b.firstIndex);
        const hi = Math.min(a.lastIndex, b.lastIndex);
        // Highest index in the overlap where the terms agree...
        let top = 0;
        for (let k = hi; k >= lo; k--) {
          if (a.term(k) === b.term(k)) {
            top = k;
            break;
          }
        }
        // ...everything at or below it (within the overlap) must be identical.
        for (let k = lo; k <= top; k++) {
          const ea = a.entry(k);
          const eb = b.entry(k);
          if (ea.term !== eb.term || JSON.stringify(ea.payload) !== JSON.stringify(eb.payload)) {
            this.fail(
              `log matching: ${ids[i]} and ${ids[j]} agree at index ${top} but differ at ${k}`,
            );
            break;
          }
        }
      }
    }
  }

  /** Number of distinct committed indices observed. */
  get committedCount(): number {
    return this.committed.size;
  }

  private fail(message: string): void {
    this.violations.push(`t=${this.now().toFixed(1)} ${message}`);
  }
}
