#!/usr/bin/env node
/**
 * Mutation check for the chaos suite: does it notice when Raft is broken?
 *
 * For each mutant, copy the project to a temp dir, apply one small bug, then
 * (1) run the randomized chaos workload over N seeds and (2) run the
 * deterministic unit and scenario tests. A mutant is killed if either one
 * fails. The working tree is never modified.
 *
 *   npm run build && node scripts/mutants.mjs [seeds=500] [mutant-name...]
 */
import { execFileSync } from 'node:child_process';
import { cpSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const root = resolve(import.meta.dirname, '..');
const NODE = 'src/raft/node.ts';
const KV = 'src/kv/state-machine.ts';

const MUTANTS = [
  {
    name: 'no-election-restriction',
    what: 'grant votes regardless of candidate log freshness (§5.4.1)',
    file: NODE,
    from: 'if (canVote && this.log.isUpToDate(m.lastLogIndex, m.lastLogTerm)) {',
    to: 'if (canVote) {',
  },
  {
    name: 'truncate-on-stale-append',
    what: 'truncate the log even when the first entry already matches',
    file: NODE,
    from: '      if (localTerm === e.term) continue;',
    to: '      if (localTerm === e.term && i > 0) continue;',
  },
  {
    name: 'accept-stale-leader',
    what: 'accept AppendEntries from an older term',
    file: NODE,
    from: '    if (m.term < this.currentTerm) return reply({});\n    this.acceptLeader(from);\n\n    let prevIndex',
    to: '    this.acceptLeader(from);\n\n    let prevIndex',
  },
  {
    name: 'no-session-dedup',
    what: 'apply retried client writes twice',
    file: KV,
    from: "    if (cmd.clientId !== '') {\n      const session",
    to: "    if (false as boolean) {\n      const session",
  },
  {
    name: 'lease-unchecked',
    what: 'serve lease reads without checking the lease',
    file: NODE,
    from: "    if (mode === 'lease' && this.currentTermCommitted() && this.leaseValid()) {",
    to: "    if (mode === 'lease') {",
  },
  {
    name: 'read-before-noop',
    what: 'serve read-index reads before the new term commits an entry',
    file: NODE,
    from: '    if (!this.currentTermCommitted()) return;\n    // Entries',
    to: '    // Entries',
  },
  {
    name: 'read-index-no-quorum',
    what: 'serve read-index reads without a confirming heartbeat round',
    file: NODE,
    from: '    while (n < this.pendingReads.length && this.pendingReads[n]!.seq <= confirmed) n++;',
    to: '    n = this.pendingReads.length;',
  },
  {
    name: 'commit-previous-term',
    what: 'commit entries from earlier terms by counting replicas (Figure 8)',
    file: NODE,
    from: 'if (n > this.commitIndex && this.log.term(n) === this.currentTerm) {',
    to: 'if (n > this.commitIndex) {',
  },
  {
    name: 'no-vote-guard',
    what: 'vote while a leader is known to be alive (breaks the lease argument)',
    file: NODE,
    from: "    return this._role === 'leader' || this.clock.now() < this.voteGuardUntil;",
    to: '    return false;',
  },
  {
    name: 'follower-commit-past-last-new',
    what: 'follower advances commitIndex past the entries the leader vouched for',
    file: NODE,
    from: '      this.setCommitIndex(Math.min(m.leaderCommit, lastNew));',
    to: '      this.setCommitIndex(Math.min(m.leaderCommit, this.log.lastIndex));',
  },
];

const args = process.argv.slice(2);
const seeds = /^\d+$/.test(args[0] ?? '') ? Number(args.shift()) : 500;
const selected = args.length ? MUTANTS.filter((m) => args.includes(m.name)) : MUTANTS;
const tsc = join(root, 'node_modules', 'typescript', 'bin', 'tsc');
const vitest = join(root, 'node_modules', 'vitest', 'vitest.mjs');

console.log(`${seeds} chaos seeds per mutant\n`);
let killed = 0;
for (const m of selected) {
  const work = mkdtempSync(join(tmpdir(), `raft-kv-mutant-${m.name}-`));
  try {
    cpSync(join(root, 'src'), join(work, 'src'), { recursive: true });
    cpSync(join(root, 'test'), join(work, 'test'), { recursive: true });
    cpSync(join(root, 'vitest.config.ts'), join(work, 'vitest.config.ts'));
    cpSync(join(root, 'tsconfig.json'), join(work, 'tsconfig.json'));
    cpSync(join(root, 'tsconfig.build.json'), join(work, 'tsconfig.build.json'));
    writeFileSync(join(work, 'package.json'), '{"type":"module"}');
    symlinkSync(join(root, 'node_modules'), join(work, 'node_modules'), 'dir');
    const path = join(work, m.file);
    const src = readFileSync(path, 'utf8');
    if (src.split(m.from).length !== 2) throw new Error(`${m.name}: patch site not found exactly once`);
    writeFileSync(path, src.replace(m.from, m.to));
    execFileSync(process.execPath, [tsc, '-p', join(work, 'tsconfig.build.json'), '--noUnusedLocals', 'false', '--noUnusedParameters', 'false'], { stdio: 'inherit' });

    const { runChaos } = await import(pathToFileURL(join(work, 'dist', 'sim', 'chaos.js')).href);
    let failed = 0;
    let first = null;
    for (let seed = 0; seed < seeds; seed++) {
      const r = runChaos(seed);
      if (!r.ok) {
        failed++;
        first ??= seed;
      }
    }
    let unitKilled = false;
    try {
      execFileSync(
        process.execPath,
        [vitest, 'run', '--exclude', 'test/chaos.test.ts', '--exclude', 'test/http.test.ts'],
        { cwd: work, stdio: 'ignore' },
      );
    } catch {
      unitKilled = true;
    }
    if (failed > 0 || unitKilled) killed++;
    const chaos = failed > 0 ? `${failed}/${seeds} seeds fail (first ${first})` : 'not detected';
    const unit = unitKilled ? 'fail' : 'pass';
    console.log(`${m.name.padEnd(30)} chaos: ${chaos.padEnd(30)} unit: ${unit.padEnd(5)} ${m.what}`);
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
}
console.log(`\n${killed}/${selected.length} mutants killed`);
