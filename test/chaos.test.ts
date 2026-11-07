/**
 * Randomized fault-injection suite. Each seed builds a cluster, runs a
 * closed-loop workload while a nemesis partitions, cuts links, crashes and
 * restarts nodes over a lossy, duplicating, reordering network, then heals
 * and checks:
 *
 *   - Raft safety invariants (election safety, log matching, leader
 *     completeness, state machine safety), checked online and at the end;
 *   - linearizability of the full client history (WGL, per key);
 *   - liveness after healing (every client's last operation completes);
 *   - convergence (every node ends with identical state).
 *
 * A failure prints the seed and the nemesis trace; `runChaos(seed)` replays
 * it exactly. Scale the run count with CHAOS_SCALE (e.g. CHAOS_SCALE=10).
 */
import { afterAll, describe, expect, it } from 'vitest';
import { formatReport, runChaos, type ChaosProfile } from '../src/sim/chaos.js';

const SCALE = Number(process.env.CHAOS_SCALE ?? 1);
const BATCH = 250;

const totals = { runs: 0, operations: 0, completed: 0, elections: 0, nemesis: 0, messages: 0, virtualMs: 0 };

function sweep(name: string, seeds: number, firstSeed: number, profile: Partial<ChaosProfile>) {
  describe(name, () => {
    const count = Math.max(BATCH, Math.round(seeds * SCALE));
    for (let start = firstSeed; start < firstSeed + count; start += BATCH) {
      const end = Math.min(start + BATCH, firstSeed + count);
      it(`seeds ${start}..${end - 1}`, () => {
        for (let seed = start; seed < end; seed++) {
          const report = runChaos(seed, profile);
          totals.runs++;
          totals.operations += report.stats.operations;
          totals.completed += report.stats.completed;
          totals.elections += report.stats.leaderElections;
          totals.nemesis += report.stats.nemesisActions;
          totals.messages += report.stats.messages;
          totals.virtualMs += report.stats.virtualMs;
          if (!report.ok) expect.fail(formatReport(report));
        }
      });
    }
  });
}

sweep('chaos: 3 nodes', 2_000, 0, { nodes: 3 });
sweep('chaos: 5 nodes', 1_000, 100_000, { nodes: 5, clients: 4 });
sweep('chaos: aggressive nemesis, single hot key', 1_000, 200_000, {
  nodes: 3,
  clients: 5,
  keys: ['k'],
  nemesisIntervalMs: 80,
  durationMs: 4_000,
  snapshotThresholds: [4, 16],
  batchSizes: [1, 2],
  maxDropRate: 0.25,
  maxDuplicateRate: 0.25,
});

describe('chaos harness', () => {
  it('is deterministic: a seed replays to an identical report', () => {
    expect(runChaos(1234)).toEqual(runChaos(1234));
  });

  it('has teeth: stale local reads are flagged as non-linearizable', () => {
    let caught = 0;
    for (let seed = 0; seed < 200; seed++) {
      const r = runChaos(seed, { readModes: ['local', 'read_index', 'log'] });
      if (!r.linearizability.ok) caught++;
      // Local reads break linearizability, never Raft's own invariants.
      expect(r.violations).toEqual([]);
    }
    expect(caught).toBeGreaterThan(10);
  });
});

afterAll(() => {
  if (totals.runs === 0) return;
  const s = (totals.virtualMs / 1000).toFixed(0);
  process.stderr.write(
    `\nchaos: ${totals.runs} runs, ${totals.operations} client ops (${totals.completed} acknowledged), ` +
      `${totals.elections} leader elections, ${totals.nemesis} nemesis actions, ` +
      `${totals.messages} messages, ${s}s of virtual time\n`,
  );
});
