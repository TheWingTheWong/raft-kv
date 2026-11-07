# Contributing

## Running the checks

```
npm ci
npm run typecheck
npm test           # unit tests + 4,000 seeded chaos runs
npm run mutants    # injects known Raft bugs and confirms the suite catches each one
```

## Reproducing a chaos failure

Every chaos run is seeded. A failure prints its seed and the nemesis trace, and `runChaos(seed)` from the chaos harness replays it exactly: the simulator is deterministic, so the same seed produces the same schedule of partitions, crashes and message reorderings. Paste the seed into a focused test while you debug.

For a longer sweep before a release, scale the run count:

```
CHAOS_SCALE=10 npx vitest run test/chaos.test.ts
```

## Changes to the Raft core

Any change under `src/raft/` should come with either a targeted unit test or a new entry in the mutation suite showing the bug it prevents. Random chaos runs alone missed three of the ten seeded bugs.
