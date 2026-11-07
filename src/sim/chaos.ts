import { checkLinearizable, type LinearizabilityResult } from '../check/linearizability.js';
import { SafetyChecker } from '../check/invariants.js';
import type { KvCommand, KvOp, ReadMode } from '../kv/types.js';
import type { RaftLog } from '../raft/log.js';
import type { NodeId } from '../raft/types.js';
import { SimClient } from './client.js';
import { SimCluster } from './cluster.js';
import { Rng } from './rng.js';

export interface ChaosProfile {
  nodes: number;
  clients: number;
  keys: string[];
  /** Length of the fault-injection phase (virtual ms). */
  durationMs: number;
  /** Time allowed after healing for every client to finish (virtual ms). */
  recoveryMs: number;
  /** Mean gap between nemesis actions (virtual ms). */
  nemesisIntervalMs: number;
  /** Read modes handed out to clients round-robin. */
  readModes: ReadMode[];
  /** Snapshot thresholds to pick from per run (0 = never compact). */
  snapshotThresholds: number[];
  /** Batch sizes to pick from per run; 1 splits every replication step. */
  batchSizes: number[];
  maxDropRate: number;
  maxDuplicateRate: number;
  crashes: boolean;
}

export const DEFAULT_PROFILE: ChaosProfile = {
  nodes: 3,
  clients: 3,
  keys: ['x', 'y'],
  durationMs: 3_000,
  recoveryMs: 10_000,
  nemesisIntervalMs: 250,
  readModes: ['read_index', 'lease', 'log'],
  snapshotThresholds: [0, 8, 32],
  batchSizes: [1, 4, 64],
  maxDropRate: 0.1,
  maxDuplicateRate: 0.1,
  crashes: true,
};

export interface ChaosReport {
  seed: number;
  ok: boolean;
  /** Raft safety violations (election safety, log matching, ...). */
  violations: string[];
  linearizability: LinearizabilityResult;
  /** Every client finished its last operation after the network healed. */
  live: boolean;
  /** All nodes converged to the same state machine contents. */
  converged: boolean;
  stats: {
    operations: number;
    completed: number;
    leaderElections: number;
    committed: number;
    nemesisActions: number;
    messages: number;
    virtualMs: number;
  };
  /** Nemesis actions, for reproducing and debugging a failing seed. */
  trace: string[];
}

/**
 * One randomized run: a cluster with closed-loop clients, a nemesis that
 * partitions, crashes and restarts nodes over a lossy network, then a
 * recovery phase. Fully determined by `seed`.
 */
export function runChaos(seed: number, overrides: Partial<ChaosProfile> = {}): ChaosReport {
  const profile = { ...DEFAULT_PROFILE, ...overrides };
  const plan = new Rng(seed);
  const faults = {
    minDelay: 1,
    maxDelay: plan.float(3, 25),
    dropRate: plan.float(0, profile.maxDropRate),
    duplicateRate: plan.float(0, profile.maxDuplicateRate),
    reorderRate: plan.float(0, 0.2),
    reorderDelay: 60,
  };
  const snapshotThreshold = plan.pick(profile.snapshotThresholds);
  const maxEntriesPerMessage = plan.pick(profile.batchSizes);

  let cluster!: SimCluster;
  const checker = new SafetyChecker<KvCommand>(() => cluster.now);
  cluster = new SimCluster({
    nodes: profile.nodes,
    seed: plan.int(0, 2 ** 31),
    network: faults,
    raft: { snapshotThreshold, maxEntriesPerMessage },
    observers: [checker],
  });
  const trace: string[] = [
    `faults ${JSON.stringify(faults, (_k, v) => (typeof v === 'number' ? +v.toFixed(3) : v))}`,
    `snapshotThreshold ${snapshotThreshold} maxEntriesPerMessage ${maxEntriesPerMessage}`,
  ];
  const log = (s: string) => trace.push(`t=${cluster.now.toFixed(1)} ${s}`);

  // --- workload ---------------------------------------------------------------
  const rng = plan.fork();
  let stopping = false;
  const clients: SimClient[] = [];
  for (let i = 0; i < profile.clients; i++) {
    const readMode = profile.readModes[i % profile.readModes.length]!;
    const client = new SimClient(cluster, `c${i + 1}`, { readMode });
    clients.push(client);
    const lastSeen = new Map<string, string | null>();
    let n = 0;
    const next = (): void => {
      if (stopping) return;
      const key = rng.pick(profile.keys);
      const roll = rng.next();
      const value = `${client.id}.${++n}`;
      let op: KvOp;
      if (roll < 0.4) op = { type: 'get', key };
      else if (roll < 0.7) op = { type: 'put', key, value };
      else if (roll < 0.9) {
        const expected = rng.chance(0.7) ? (lastSeen.get(key) ?? null) : null;
        op = { type: 'cas', key, expected, value };
      } else op = { type: 'delete', key };
      client.submit(op, (result) => {
        lastSeen.set(key, op.type === 'get' ? result.value : null);
        if (op.type === 'put' || (op.type === 'cas' && result.swapped)) lastSeen.set(key, value);
        cluster.clock.setTimer(rng.float(0, 20), next);
      });
    };
    cluster.clock.setTimer(rng.float(0, 50), next);
  }

  // --- nemesis -----------------------------------------------------------------
  const nemesis = plan.fork();
  let actions = 0;
  const checkLogs = () => {
    const logs = new Map<NodeId, RaftLog<KvCommand>>();
    for (const id of cluster.ids) logs.set(id, cluster.durableLog(id));
    checker.checkLogMatching(logs);
  };
  const act = (): void => {
    if (stopping) return;
    actions++;
    const roll = nemesis.next();
    const ids = cluster.ids;
    if (roll < 0.25) {
      const shuffled = nemesis.shuffle(ids);
      const cut = nemesis.int(1, ids.length);
      const groups = [shuffled.slice(0, cut), shuffled.slice(cut)];
      cluster.partition(...groups);
      log(`partition ${groups.map((g) => g.join(',')).join(' | ')}`);
    } else if (roll < 0.35) {
      const leader = cluster.leader();
      if (leader) {
        cluster.isolate(leader.id);
        log(`isolate leader ${leader.id}`);
      }
    } else if (roll < 0.45) {
      const a = nemesis.pick(ids);
      const b = nemesis.pick(ids.filter((x) => x !== a));
      cluster.net.cut(a, b);
      log(`cut ${a}<->${b}`);
    } else if (roll < 0.6) {
      cluster.heal();
      log('heal');
    } else if (roll < 0.75 && profile.crashes) {
      const up = cluster.upIds();
      if (up.length > 0) {
        const victim = nemesis.pick(up);
        cluster.crash(victim);
        log(`crash ${victim}`);
      }
    } else if (roll < 0.9 && profile.crashes) {
      const down = cluster.downIds();
      if (down.length > 0) {
        const lucky = nemesis.pick(down);
        cluster.start(lucky);
        log(`restart ${lucky}`);
      }
    } else {
      const leader = cluster.leader();
      if (leader && profile.crashes) {
        cluster.restart(leader.id);
        log(`bounce leader ${leader.id}`);
      }
    }
    checkLogs();
    cluster.clock.setTimer(nemesis.float(0.5, 1.5) * profile.nemesisIntervalMs, act);
  };
  cluster.clock.setTimer(nemesis.float(0.5, 1.5) * profile.nemesisIntervalMs, act);

  // --- run ---------------------------------------------------------------------
  let live = false;
  let converged = false;
  try {
    cluster.runFor(profile.durationMs);

    // Recovery: stop issuing new work, heal everything, let in-flight ops finish.
    stopping = true;
    cluster.heal();
    cluster.net.faults = { ...cluster.net.faults, dropRate: 0 };
    for (const id of cluster.downIds()) cluster.start(id);
    log('recover: heal + restart all');
    live = cluster.runUntil(() => clients.every((c) => !c.busy), profile.recoveryMs);

    // Convergence: once everything is quiet, every node holds the same state.
    if (live) {
      converged = cluster.runUntil(() => {
        const leader = cluster.leader();
        if (!leader) return false;
        const target = leader.commit;
        const ref = JSON.stringify(cluster.stateMachine(leader.id).entries());
        return cluster.ids.every(
          (id) =>
            cluster.node(id).applied === target &&
            JSON.stringify(cluster.stateMachine(id).entries()) === ref,
        );
      }, 5_000);
    }
    checkLogs();
  } catch (err) {
    // Internal assertions in the node (e.g. truncating a committed entry)
    // surface here; report them like any other violation.
    checker.violations.push(`t=${cluster.now.toFixed(1)} exception: ${(err as Error).message}`);
  }

  const linearizability = checkLinearizable(cluster.history.operations());
  const violations = checker.violations;
  return {
    seed,
    ok: violations.length === 0 && linearizability.ok && live && converged,
    violations,
    linearizability,
    live,
    converged,
    stats: {
      operations: cluster.history.size,
      completed: clients.reduce((s, c) => s + c.completed, 0),
      leaderElections: checker.leaderElections,
      committed: checker.committedCount,
      nemesisActions: actions,
      messages: cluster.net.stats.sent,
      virtualMs: cluster.now,
    },
    trace,
  };
}

/** Human-readable failure summary for test output. */
export function formatReport(r: ChaosReport): string {
  const lines = [
    `seed ${r.seed}: ok=${r.ok} live=${r.live} converged=${r.converged} linearizable=${r.linearizability.ok}`,
    `stats ${JSON.stringify(r.stats)}`,
    ...r.violations.map((v) => `VIOLATION ${v}`),
  ];
  if (!r.linearizability.ok) {
    lines.push(`non-linearizable history for key ${r.linearizability.key}:`);
    lines.push(...(r.linearizability.operations ?? []).map((o) => `  ${o}`));
  }
  lines.push('trace:', ...r.trace.map((t) => `  ${t}`));
  return lines.join('\n');
}
