/**
 * Scripted single-node tests: messages are captured and fed by hand, so each
 * test pins down one rule from the paper without a network in the way.
 */
import { describe, expect, it } from 'vitest';
import { RaftNode } from '../src/raft/node.js';
import type { StateMachine } from '../src/raft/state-machine.js';
import { MemoryStorage } from '../src/raft/storage.js';
import type { LogEntry, NodeId, RaftMessage } from '../src/raft/types.js';
import { VirtualClock } from '../src/sim/virtual-clock.js';

class ListMachine implements StateMachine<string> {
  items: string[] = [];
  apply(cmd: string): number {
    this.items.push(cmd);
    return this.items.length;
  }
  query(): string[] {
    return this.items.slice();
  }
  snapshot(): unknown {
    return this.items.slice();
  }
  restore(data: unknown): void {
    this.items = (data as string[]).slice();
  }
}

const entry = (index: number, term: number): LogEntry<string> => ({
  index,
  term,
  payload: { type: 'command', command: `c${index}` },
});

function harness(opts: { term?: number; log?: Array<[number, number]>; jitter?: number } = {}) {
  const clock = new VirtualClock();
  const storage = new MemoryStorage<string>();
  storage.saveHardState({ term: opts.term ?? 0, votedFor: null });
  storage.append((opts.log ?? []).map(([i, t]) => entry(i, t)));
  const outbox: Array<{ to: NodeId; msg: RaftMessage<string> }> = [];
  const sm = new ListMachine();
  const node = new RaftNode<string>({
    id: 'a',
    members: ['a', 'b', 'c'],
    storage,
    stateMachine: sm,
    clock,
    transport: { send: (to, msg) => outbox.push({ to, msg }) },
    random: () => opts.jitter ?? 0,
  });
  node.start();
  const take = <T extends RaftMessage<string>['type']>(type: T) => {
    const out = outbox.filter((o) => o.msg.type === type);
    outbox.length = 0;
    return out as Array<{ to: NodeId; msg: Extract<RaftMessage<string>, { type: T }> }>;
  };
  return { clock, storage, node, sm, outbox, take };
}

describe('RaftNode elections', () => {
  it('times out, campaigns and wins with a majority', () => {
    const h = harness();
    h.clock.runFor(150);
    expect(h.node.role).toBe('candidate');
    expect(h.node.term).toBe(1);
    const reqs = h.take('RequestVote');
    expect(reqs.map((r) => r.to).sort()).toEqual(['b', 'c']);
    h.node.receive('b', { type: 'RequestVoteReply', term: 1, voteGranted: true });
    expect(h.node.role).toBe('leader');
    // Vote and term were persisted before the node acted on them.
    expect(h.storage.load().hardState).toEqual({ term: 1, votedFor: 'a' });
  });

  it('denies votes to candidates with a less up-to-date log (election restriction)', () => {
    const h = harness({ term: 2, log: [[1, 1], [2, 2]] });
    h.clock.runFor(150); // vote guard from startup has expired
    h.take('RequestVote');
    h.node.receive('b', {
      type: 'RequestVote',
      term: 5,
      candidateId: 'b',
      lastLogIndex: 10,
      lastLogTerm: 1,
    });
    const [reply] = h.take('RequestVoteReply');
    expect(reply!.msg.voteGranted).toBe(false);
    expect(h.node.term).toBe(5); // still adopts the newer term
  });

  it('grants at most one vote per term', () => {
    const h = harness({ jitter: 0.99 }); // election timeout ~298ms
    h.clock.runFor(200); // past the startup vote guard, before our own timeout
    const ask = (id: string) =>
      h.node.receive(id, { type: 'RequestVote', term: 1, candidateId: id, lastLogIndex: 0, lastLogTerm: 0 });
    ask('b');
    ask('c');
    ask('b'); // duplicate request from the same candidate is fine
    const replies = h.take('RequestVoteReply').map((r) => [r.to, r.msg.voteGranted]);
    expect(replies).toEqual([
      ['b', true],
      ['c', false],
      ['b', true],
    ]);
  });

  it('ignores RequestVote from a disruptive candidate while a leader is alive', () => {
    const h = harness();
    h.node.receive('b', {
      type: 'AppendEntries',
      term: 1,
      leaderId: 'b',
      prevLogIndex: 0,
      prevLogTerm: 0,
      entries: [],
      leaderCommit: 0,
      seq: 1,
      sentAt: 0,
    });
    h.node.receive('c', { type: 'RequestVote', term: 9, candidateId: 'c', lastLogIndex: 5, lastLogTerm: 5 });
    expect(h.node.term).toBe(1);
    expect(h.take('RequestVoteReply')[0]!.msg.voteGranted).toBe(false);
  });
});

describe('RaftNode commit rule', () => {
  it('never commits a previous-term entry by counting replicas (Figure 8)', () => {
    // Node a has an entry from term 2 that never committed.
    const h = harness({ term: 3, log: [[1, 1], [2, 2]] });
    h.clock.runFor(150);
    h.node.receive('b', { type: 'RequestVoteReply', term: 4, voteGranted: false });
    expect(h.node.role).toBe('candidate');
    h.node.receive('c', { type: 'RequestVoteReply', term: 4, voteGranted: true });
    expect(h.node.role).toBe('leader');
    expect(h.node.lastLogIndex).toBe(3); // no-op appended in term 4

    const ack = (matchIndex: number) =>
      h.node.receive('c', {
        type: 'AppendEntriesReply',
        term: 4,
        success: true,
        matchIndex,
        conflictIndex: 0,
        conflictTerm: 0,
        seq: 1,
        sentAt: 0,
      });

    ack(2); // the term-2 entry is now on a majority...
    expect(h.node.commit).toBe(0); // ...but must not be committed by counting.
    ack(3); // the term-4 no-op reaches a majority
    expect(h.node.commit).toBe(3); // which commits index 2 indirectly
    expect(h.sm.items).toEqual(['c1', 'c2']);
  });
});

describe('RaftNode log consistency', () => {
  const ae = (fields: Partial<Extract<RaftMessage<string>, { type: 'AppendEntries' }>>) => ({
    type: 'AppendEntries' as const,
    term: 3,
    leaderId: 'b',
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [] as LogEntry<string>[],
    leaderCommit: 0,
    seq: 1,
    sentAt: 0,
    ...fields,
  });

  it('rejects when prevLogIndex is past the end and hints the next index', () => {
    const h = harness({ term: 3, log: [[1, 1]] });
    h.node.receive('b', ae({ prevLogIndex: 5, prevLogTerm: 3 }));
    const [r] = h.take('AppendEntriesReply');
    expect(r!.msg).toMatchObject({ success: false, conflictIndex: 2, conflictTerm: 0 });
  });

  it('reports the first index of a conflicting term', () => {
    const h = harness({ term: 3, log: [[1, 1], [2, 2], [3, 2], [4, 2]] });
    h.node.receive('b', ae({ prevLogIndex: 4, prevLogTerm: 3 }));
    const [r] = h.take('AppendEntriesReply');
    expect(r!.msg).toMatchObject({ success: false, conflictIndex: 2, conflictTerm: 2 });
  });

  it('truncates a conflicting suffix and appends the leader entries', () => {
    const h = harness({ term: 3, log: [[1, 1], [2, 2], [3, 2]] });
    h.node.receive('b', ae({ prevLogIndex: 1, prevLogTerm: 1, entries: [entry(2, 3)], leaderCommit: 2 }));
    expect(h.node.entries().map((e) => [e.index, e.term])).toEqual([
      [1, 1],
      [2, 3],
    ]);
    expect(h.storage.load().entries.map((e) => [e.index, e.term])).toEqual([
      [1, 1],
      [2, 3],
    ]);
    expect(h.node.commit).toBe(2);
  });

  it('does not truncate on a stale, shorter AppendEntries', () => {
    const h = harness({ term: 3 });
    h.node.receive('b', ae({ entries: [entry(1, 3), entry(2, 3)] }));
    h.node.receive('b', ae({ entries: [entry(1, 3)], leaderCommit: 5 }));
    expect(h.node.lastLogIndex).toBe(2);
    // commitIndex is capped at the last entry this message vouched for.
    expect(h.node.commit).toBe(1);
  });
});
