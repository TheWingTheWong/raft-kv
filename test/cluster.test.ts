/**
 * Scenario tests: whole clusters in the simulator, driven through specific
 * failure sequences from the paper.
 */
import { describe, expect, it } from 'vitest';
import type { KvOp, KvResult, ReadMode } from '../src/kv/types.js';
import { SimClient } from '../src/sim/client.js';
import { SimCluster } from '../src/sim/cluster.js';

/** Submit one operation and run the simulation until it completes. */
function exec(cluster: SimCluster, client: SimClient, op: KvOp, maxMs = 5_000): KvResult {
  let result: KvResult | undefined;
  client.submit(op, (r) => (result = r));
  if (!cluster.runUntil(() => result !== undefined, maxMs)) {
    throw new Error(`operation ${JSON.stringify(op)} did not complete in ${maxMs}ms`);
  }
  return result!;
}

/** Ask one specific node to read, without client retries. */
function readFrom(cluster: SimCluster, id: string, key: string, mode: ReadMode, waitMs = 1_000) {
  const node = cluster.node(id);
  let out: unknown = 'pending';
  if (mode === 'read_index' || mode === 'lease') {
    node.read({ type: 'get', key }, mode, (r) => (out = r));
  }
  cluster.runUntil(() => out !== 'pending', waitMs);
  return out;
}

describe('leader election', () => {
  it('elects exactly one leader in a healthy cluster', () => {
    const c = new SimCluster({ nodes: 5, seed: 1 });
    const leader = c.waitForLeader();
    c.runFor(2_000);
    const leaders = c.upIds().filter((id) => c.node(id).role === 'leader');
    expect(leaders).toEqual([leader.id]);
    expect(c.upIds().every((id) => c.node(id).term === leader.term)).toBe(true);
  });

  it('elects a new leader in a higher term after the leader crashes', () => {
    const c = new SimCluster({ nodes: 3, seed: 2 });
    const first = c.waitForLeader();
    c.crash(first.id);
    const second = c.waitForLeader();
    expect(second.id).not.toBe(first.id);
    expect(second.term).toBeGreaterThan(first.term);
  });

  it('a single-node cluster elects itself and commits alone', () => {
    const c = new SimCluster({ nodes: 1, seed: 6 });
    const leader = c.waitForLeader();
    const client = new SimClient(c, 'c1', { readMode: 'lease' });
    exec(c, client, { type: 'put', key: 'solo', value: '1' });
    expect(exec(c, client, { type: 'get', key: 'solo' })).toEqual({ value: '1' });
    expect(leader.commit).toBe(2);
  });

  it('cannot elect a leader without a majority', () => {
    const c = new SimCluster({ nodes: 5, seed: 3 });
    c.waitForLeader();
    for (const id of c.ids.slice(0, 3)) c.crash(id);
    c.runFor(3_000);
    expect(c.leader()).toBeUndefined();
  });

  it('a partitioned leader steps down (check-quorum) and the majority moves on', () => {
    const c = new SimCluster({ nodes: 5, seed: 4 });
    const old = c.waitForLeader();
    c.isolate(old.id);
    c.runFor(1_000);
    expect(old.role).not.toBe('leader');
    const fresh = c.leader();
    expect(fresh && fresh.id !== old.id).toBe(true);
  });

  it('does not let a rejoining node with an inflated term disrupt a healthy leader', () => {
    const c = new SimCluster({ nodes: 3, seed: 5 });
    const leader = c.waitForLeader();
    const follower = c.ids.find((id) => id !== leader.id)!;
    c.isolate(follower);
    c.runFor(2_000); // keeps campaigning alone, term climbs
    expect(c.node(follower).term).toBeGreaterThan(leader.term);
    c.heal();
    c.runFor(2_000);
    // The cluster converges on a single leader and the stale node catches up.
    const final = c.waitForLeader();
    expect(c.upIds().every((id) => c.node(id).term === final.term)).toBe(true);
    expect(c.node(follower).commit).toBe(final.commit);
  });
});

describe('log replication', () => {
  it('replicates writes to every node and applies them in order', () => {
    const c = new SimCluster({ nodes: 3, seed: 10 });
    c.waitForLeader();
    const client = new SimClient(c, 'c1');
    for (let i = 0; i < 20; i++) exec(c, client, { type: 'put', key: `k${i % 4}`, value: `${i}` });
    c.runFor(500);
    const states = c.ids.map((id) => c.stateMachine(id).entries());
    expect(states[0]).toEqual([
      ['k0', '16'],
      ['k1', '17'],
      ['k2', '18'],
      ['k3', '19'],
    ]);
    expect(states[1]).toEqual(states[0]);
    expect(states[2]).toEqual(states[0]);
  });

  it('commits through message loss, duplication and reordering', () => {
    const c = new SimCluster({
      nodes: 5,
      seed: 11,
      network: { dropRate: 0.2, duplicateRate: 0.2, reorderRate: 0.3, reorderDelay: 80, maxDelay: 20 },
    });
    const client = new SimClient(c, 'c1');
    for (let i = 0; i < 30; i++) exec(c, client, { type: 'put', key: 'x', value: `${i}` }, 30_000);
    expect(exec(c, client, { type: 'get', key: 'x' }, 30_000)).toEqual({ value: '29' });
  });

  it('discards uncommitted entries from a deposed minority leader', () => {
    const c = new SimCluster({ nodes: 5, seed: 12 });
    const old = c.waitForLeader();
    const client = new SimClient(c, 'c1');
    exec(c, client, { type: 'put', key: 'k', value: 'committed' });

    // Cut the leader off with one follower; it can append but not commit.
    const buddy = c.ids.find((id) => id !== old.id)!;
    c.partition([old.id, buddy], c.ids.filter((id) => id !== old.id && id !== buddy));
    c.net.faults.dropRate = 0;
    let lost: unknown;
    old.propose({ clientId: '', seq: 0, op: { type: 'put', key: 'k', value: 'lost' } }, (r) => (lost = r));
    c.runFor(100);
    const strandedIndex = old.lastLogIndex;
    expect(old.commit).toBeLessThan(strandedIndex);

    // The majority elects a new leader and commits a different value.
    const majorityClient = new SimClient(c, 'c2');
    c.runFor(1_000);
    exec(c, majorityClient, { type: 'put', key: 'k', value: 'winner' });

    c.heal();
    c.runFor(1_000);
    expect(lost).toEqual({ ok: false, error: 'dropped', leaderHint: expect.any(String) });
    for (const id of c.ids) {
      expect(c.stateMachine(id).entries()).toEqual([['k', 'winner']]);
    }
  });

  it('a restarted node recovers its log and state from storage', () => {
    const c = new SimCluster({ nodes: 3, seed: 13 });
    c.waitForLeader();
    const client = new SimClient(c, 'c1');
    for (let i = 0; i < 5; i++) exec(c, client, { type: 'put', key: `k${i}`, value: `${i}` });
    c.runFor(200);
    for (const id of c.ids) c.crash(id);
    for (const id of c.ids) c.start(id);
    c.waitForLeader();
    c.runFor(500);
    for (const id of c.ids) expect(c.stateMachine(id).entries()).toHaveLength(5);
  });

  it('applies a retried write exactly once', () => {
    const c = new SimCluster({ nodes: 3, seed: 14, network: { duplicateRate: 0.5 } });
    const leader = c.waitForLeader();
    const results: unknown[] = [];
    const cmd = { clientId: 'w', seq: 1, op: { type: 'put' as const, key: 'a', value: '1' } };
    leader.propose(cmd, (r) => results.push(r));
    leader.propose(cmd, (r) => results.push(r));
    c.runFor(200);
    leader.propose({ clientId: 'w', seq: 2, op: { type: 'put', key: 'a', value: '2' } }, () => {});
    leader.propose(cmd, () => {}); // a very late duplicate
    c.runFor(200);
    expect(results).toEqual([
      { ok: true, index: 2, result: { value: null } },
      { ok: true, index: 3, result: { value: null } },
    ]);
    expect(c.stateMachine(leader.id).entries()).toEqual([['a', '2']]);
  });
});

describe('snapshots', () => {
  it('compacts the log and brings a lagging follower up to date with InstallSnapshot', () => {
    const c = new SimCluster({ nodes: 3, seed: 20, raft: { snapshotThreshold: 10 } });
    const leader = c.waitForLeader();
    const lagger = c.ids.find((id) => id !== leader.id)!;
    c.crash(lagger);
    const client = new SimClient(c, 'c1');
    for (let i = 0; i < 40; i++) exec(c, client, { type: 'put', key: `k${i % 7}`, value: `${i}` });
    expect(leader.snapshotIndex).toBeGreaterThanOrEqual(30);

    c.start(lagger);
    c.runFor(1_000);
    expect(c.node(lagger).snapshotIndex).toBeGreaterThan(0);
    expect(c.node(lagger).commit).toBe(leader.commit);
    expect(c.stateMachine(lagger).entries()).toEqual(c.stateMachine(leader.id).entries());
  });

  it('restores state from a snapshot after restart', () => {
    const c = new SimCluster({ nodes: 3, seed: 21, raft: { snapshotThreshold: 5 } });
    c.waitForLeader();
    const client = new SimClient(c, 'c1');
    for (let i = 0; i < 23; i++) exec(c, client, { type: 'put', key: `k${i}`, value: `${i}` });
    c.runFor(200);
    const before = c.stateMachine('n1').entries();
    c.restart('n1');
    // The snapshot is restored immediately; entries after it are re-applied
    // once the node relearns the commit index from the leader.
    expect(c.node('n1').snapshotIndex).toBeGreaterThan(0);
    expect(c.node('n1').applied).toBe(c.node('n1').snapshotIndex);
    c.runFor(500);
    expect(c.stateMachine('n1').entries()).toEqual(before);
  });

  it('keeps client sessions across snapshots so retries stay exactly-once', () => {
    const c = new SimCluster({ nodes: 3, seed: 22, raft: { snapshotThreshold: 3 } });
    const leader = c.waitForLeader();
    const cmd = { clientId: 'w', seq: 1, op: { type: 'put' as const, key: 'a', value: 'first' } };
    leader.propose(cmd, () => {});
    for (let i = 0; i < 10; i++) {
      leader.propose({ clientId: 'x', seq: i + 1, op: { type: 'put', key: 'a', value: `x${i}` } }, () => {});
    }
    c.runFor(300);
    expect(leader.snapshotIndex).toBeGreaterThan(2);
    leader.propose(cmd, () => {});
    c.runFor(300);
    expect(c.stateMachine(leader.id).entries()).toEqual([['a', 'x9']]);
  });
});

describe('linearizable reads', () => {
  it.each(['read_index', 'lease'] as const)(
    '%s: a deposed leader in a minority partition never answers',
    (mode) => {
      const c = new SimCluster({ nodes: 5, seed: 30, raft: { checkQuorum: false } });
      const old = c.waitForLeader();
      const client = new SimClient(c, 'c1');
      exec(c, client, { type: 'put', key: 'k', value: 'v1' });
      c.runFor(100);

      c.partition([old.id], c.ids.filter((id) => id !== old.id));
      c.runFor(1_000); // majority elects a new leader
      exec(c, client, { type: 'put', key: 'k', value: 'v2' });

      // Without check-quorum the old node still believes it is leader.
      expect(old.role).toBe('leader');
      expect(readFrom(c, old.id, 'k', mode, 2_000)).toBe('pending');
    },
  );

  it('lease reads are answered without any network round trip', () => {
    const c = new SimCluster({ nodes: 3, seed: 31 });
    const leader = c.waitForLeader();
    c.runFor(200);
    const sent = c.net.stats.sent;
    let out: unknown;
    leader.read({ type: 'get', key: 'k' }, 'lease', (r) => (out = r));
    expect(out).toEqual({ ok: true, result: { value: null } });
    expect(c.net.stats.sent).toBe(sent);
  });

  it('read_index reflects every write acknowledged before the read', () => {
    const c = new SimCluster({ nodes: 3, seed: 32 });
    c.waitForLeader();
    const writer = new SimClient(c, 'w');
    const reader = new SimClient(c, 'r', { readMode: 'read_index' });
    for (let i = 0; i < 10; i++) {
      exec(c, writer, { type: 'put', key: 'k', value: `${i}` });
      expect(exec(c, reader, { type: 'get', key: 'k' })).toEqual({ value: `${i}` });
    }
  });
});
