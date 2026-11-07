# raft-kv

[![ci](https://github.com/TheWingTheWong/raft-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/TheWingTheWong/raft-kv/actions/workflows/ci.yml)

A key-value store replicated with Raft, written in TypeScript for Node. The
protocol code is a single event-driven state machine with no I/O of its own,
so the same `RaftNode` runs inside a deterministic simulator (virtual clock,
seeded RNG, hostile network) and as three real processes talking HTTP.

Most of the effort went into testing. `npm test` runs 4,000 randomized
cluster histories with partitions, crashes, message loss, duplication and
reordering, and checks every one for Raft's safety properties and for
linearizability of what clients observed. It takes about 15 seconds.

```
$ npm test
chaos: 4000 runs, 139492 client ops (139492 acknowledged), 14782 leader elections,
       84278 nemesis actions, 2717566 messages, 14839s of virtual time
 Test Files  9 passed (9)
      Tests  91 passed (91)
```

## What is implemented

Following the Raft paper (Ongaro & Ousterhout, 2014) and the relevant
chapters of Ongaro's dissertation:

- **Leader election** with randomized timeouts, the election restriction
  (§5.4.1), and votes and terms persisted before any reply.
- **Log replication** with the consistency check, fast backtracking by
  conflicting term, and replies that are safe to receive late, twice, or out
  of order. A follower only truncates on a real conflict, so a stale, shorter
  AppendEntries can never erase entries it already acknowledged.
- **Commit rule**: the leader commits only entries from its own term by
  counting replicas (§5.4.2, Figure 8). Each new leader appends a no-op so
  earlier entries commit promptly.
- **Persistence** behind a `Storage` interface: an in-memory backend for the
  simulator and an fsynced file backend (JSON-lines log, atomic manifest swap
  for snapshot installs, torn-tail recovery).
- **Log compaction**: snapshots past a threshold; lagging followers get
  `InstallSnapshot` and keep any consistent suffix of their log.
- **Linearizable reads** without writing to the log, two ways:
  - *read-index* (§6.4): wait for a current-term commit, then for a majority
    to acknowledge a heartbeat round sent after the read arrived;
  - *leader lease* (§6.4.1): answer locally while the latest
    majority-acknowledged send is less than `electionTimeoutMin × (1 − drift)`
    old.
- **Vote guard** (§4.2.3): a node that heard from a leader within the minimum
  election timeout ignores RequestVote. This is what makes leases sound, and
  it also keeps a rejoining node with an inflated term from deposing a healthy
  leader. A restarting node starts with the guard armed, because it may have
  acknowledged a lease just before it crashed.
- **Check-quorum**: a leader that has not heard from a majority for
  `electionTimeoutMax` steps down instead of accepting writes it cannot commit.
- **Exactly-once client sessions** (§6.3): writes carry `(clientId, seq)`, and
  the state machine caches the last result per client. Sessions are part of
  the snapshot.
- **KV state machine**: `get`, `put`, `delete`, `cas`. Every operation returns
  the value it observed, which gives the linearizability checker more to work
  with.

Not implemented: membership changes, pre-vote, leadership transfer, chunked
snapshot transfer, session expiry. See the roadmap.

## Architecture

```
                     ┌───────────────────────── one server ─────────────────────────┐
  client ──request──▶│ KvServer ──propose / read──▶ RaftNode ──apply──▶ KvStateMachine│
         ◀─response──│                                │  ▲                            │
                     │                    Clock ◀─────┤  │                            │
                     │       Storage (Memory | File) ◀┤  │                            │
                     └────────────────────────────────┼──┼────────────────────────────┘
                                                      ▼  │ receive(from, msg)
                                                  Transport
                            ┌─────────────────────────┴──────────────────────────┐
                 simulator: SimNetwork + VirtualClock          real: HttpNode + RealClock
                 (drop, dup, delay, reorder, partition)        (POST /raft between processes)
```

`RaftNode` owns no timers, sockets or files. Messages arrive through
`receive()`, leave through `Transport.send()`, time comes from `Clock`, and
durable state goes through `Storage`, whose methods return only after the data
is durable. Callbacks rather than promises keep it usable inside a
synchronous discrete-event loop.

| Path | Purpose |
| --- | --- |
| `src/raft/node.ts` | The protocol: elections, replication, commit, snapshots, reads |
| `src/raft/log.ts` | Log indexed by absolute position, offset by the snapshot |
| `src/raft/storage.ts` | `MemoryStorage`, `FileStorage` |
| `src/kv/` | State machine with sessions, request handling |
| `src/sim/` | `VirtualClock`, `Rng`, `SimNetwork`, `SimCluster`, `SimClient`, chaos runner |
| `src/check/` | Client history, WGL linearizability checker, Raft safety checker |
| `src/net/` | `HttpNode` (peer RPC and KV API), `HttpKvClient` |
| `src/cli/` | `cluster` launcher, single `node` runner, `kv` client |
| `scripts/mutants.mjs` | Injects known bugs and checks that the tests catch them |

## How it is tested

### The simulator

`SimCluster` runs servers, clients and the network on one `VirtualClock`, a
binary-heap event queue in which time advances only when the simulation says
so. All randomness (election jitter, network latency, faults, workload,
nemesis) comes from one seeded `Rng` forked into independent streams, so a
seed reproduces a run event for event. The suite asserts this:
`runChaos(1234)` deep-equals `runChaos(1234)`.

`SimNetwork` clones every message (nodes share no references, as with real
serialization), and for each one decides drop, duplicate, latency, and an
optional long hold that reorders it behind later traffic. Partitions are
checked when a message is sent and again when it is delivered, so in-flight
messages die when a partition starts. A crash unregisters the node and
discards its `RaftNode`. The `MemoryStorage` outlives it, which is exactly
the durable state a restart recovers.

### The chaos runs (`test/chaos.test.ts`)

Each seed picks network fault rates (up to 10% loss, 10% duplication,
reordering), a snapshot threshold (off, 8, 32) and a replication batch size
(1, 4, 64). Batch size 1 splits every catch-up into single-entry steps, which
opens more interleavings. Closed-loop clients then issue random
`get`/`put`/`cas`/`delete` operations on two keys with unique values, each
client using a different read path (read-index, lease, through the log).
Every ~250 ms of virtual time a nemesis does one of: random bipartition,
isolate the leader, cut a single link, heal, crash a node, restart a node,
bounce the leader. After 3 s it heals the network, restarts everything, and
waits for every client to finish.

Each run then checks:

| Check | How |
| --- | --- |
| Election Safety | `SafetyChecker` records the leader of every term as elections happen |
| Leader Completeness | At each election, the new leader must hold every entry committed so far |
| State Machine Safety | Every commit and apply at an index must match across nodes (term and payload) |
| Log Matching | Pairwise over all durable logs, including crashed nodes, after every nemesis action |
| Linearizability | Full client history, Wing & Gong search with Lowe's memoization, split per key |
| Liveness | Every client's last operation completes within 10 s of healing |
| Convergence | All nodes reach the leader's commit index with identical state |

Internal assertions in the node (such as refusing to truncate a committed
entry) are reported the same way. Operations whose outcome a client never
learned may linearize at any point after their call, or never.

Three profiles run in `npm test`: 2,000 seeds on 3 nodes, 1,000 on 5 nodes,
and 1,000 with a nemesis every ~80 ms, one hot key, five clients, up to 25% loss and
duplication, and snapshots every 4 to 16 entries. `CHAOS_SCALE=10 npm run chaos`
runs 40,000: on an Apple M2 that takes 143 s and covers 1.38 million
client operations, 147,688 leader elections and 41 hours of virtual cluster
time, all clean. CI runs it nightly. A failing seed prints its fault parameters, nemesis trace and the
non-linearizable sub-history, and `runChaos(seed)` replays it.

### Do the checks have teeth?

A clean run only means something if the same harness fails a broken
implementation. `npm run mutants` copies the project to a temp directory,
injects one known bug at a time, and runs both the chaos sweep (1,000 seeds)
and the deterministic tests against it:

| Mutant | Chaos seeds failing | Unit/scenario tests |
| --- | --- | --- |
| Vote without the election restriction | 675 / 1000 | fail |
| Truncate even when the first entry matches | 950 / 1000 | fail |
| Accept AppendEntries from an older term | 27 / 1000 | pass |
| No session dedup (retries apply twice) | 640 / 1000 | fail |
| Lease reads without checking the lease | 13 / 1000 | fail |
| Read-index before the term's no-op commits | 11 / 1000 | pass |
| Read-index without a confirming round | 2 / 1000 | fail |
| Commit earlier-term entries by counting (Figure 8) | 0 / 1000 | fail |
| Vote while a leader is alive (no vote guard) | 0 / 1000 | fail |
| Follower commits past what the leader vouched for | 0 / 1000 | fail |

All ten are caught, but neither layer catches all ten on its own. The chaos
runs find bugs nobody wrote a test for. The last three need interleavings
too specific to hit at random: Figure 8 needs a leader to replicate an
old-term entry to a majority without the no-op that follows it, then crash at
exactly the wrong moment. For those, `test/raft-node.test.ts` feeds a single
node hand-written messages and checks the exact rule from the paper. The
mutant table is the reason both layers exist.

The suite also checks the checker: with the non-linearizable `local` read
mode turned on, it must flag stale reads in more than 10 of 200 seeds, and
Raft's own invariants must still hold in all of them.

### The other tests

- `raft-node.test.ts`: scripted single-node tests. Election restriction, one
  vote per term, vote guard, the Figure 8 commit rule, conflict hints,
  truncation, stale AppendEntries, read-index rounds, lease expiry.
- `cluster.test.ts`: named scenarios in the simulator. Re-election, no quorum,
  check-quorum step-down, a rejoining node with an inflated term, 20% loss and
  duplication, a deposed minority leader whose entries are discarded, restart
  from storage, InstallSnapshot catch-up, sessions surviving snapshots, stale
  leaders that never answer reads, and lease reads with zero messages sent.
- `storage.test.ts`: both backends against one contract, plus torn-write
  recovery and log generation cleanup for `FileStorage`.
- `http.test.ts`: three `HttpNode`s on real sockets with file storage: CRUD,
  421 redirects, failover after killing the leader, and a restart from disk on
  the same port.

## Quickstart

Requires Node 22.12+. Nothing needs network access after `npm install`.

```sh
npm install
npm test            # unit, scenario, HTTP and 4,000 chaos runs (~15 s)
npm run build       # tsc, emits dist/
```

### Run a local cluster

```sh
npm run cluster -- --fresh
```

This builds, then starts `n1`–`n3` as separate processes on ports
7001–7003, with data under `.raft-data/`. In the same terminal, `kill n2`,
`start n2` and `status` fail and recover nodes. From another terminal:

```sh
$ npm run -s kv -- put greeting hello
OK
$ npm run -s kv -- get greeting --mode lease
hello
$ npm run -s kv -- cas greeting hello hi
swapped
$ npm run -s kv -- status
n1   follower    term=2 leader=n2 commit=4 log=4 snapshot=0
n2   leader      term=2 leader=n2 commit=4 log=4 snapshot=0
n3   follower    term=2 leader=n2 commit=4 log=4 snapshot=0
```

Kill the leader in the cluster terminal (`kill n2`). The client finds the new
leader by itself, and `start n2` brings the node back from its files.

The HTTP API, if you prefer curl:

```sh
curl -X PUT  localhost:7001/kv/a -d '{"value":"1"}'          # 421 + leaderUrl if not leader
curl         'localhost:7002/kv/a?mode=read_index'           # log | read_index | lease | local
curl -X POST localhost:7002/kv/a/cas -d '{"expected":"1","value":"2"}'
curl -X DELETE localhost:7002/kv/a
curl         localhost:7003/status
```

Send `x-client-id` and `x-client-seq` headers on writes for exactly-once
retries. `HttpKvClient` does this for you.

### Use the simulator

```ts
import { SimCluster, SimClient, checkLinearizable } from 'raft-kv';

const cluster = new SimCluster({ nodes: 5, seed: 42, network: { dropRate: 0.05 } });
const leader = cluster.waitForLeader();

const client = new SimClient(cluster, 'c1', { readMode: 'lease' });
client.submit({ type: 'put', key: 'x', value: '1' });

cluster.isolate(leader.id);   // or crash(id), partition([...], [...]), heal()
cluster.runFor(2_000);        // virtual milliseconds

console.log(checkLinearizable(cluster.history.operations()).ok);
```

## Design notes

- **Replies carry their own context.** AppendEntries replies report
  `matchIndex` directly instead of relying on the leader remembering what it
  sent. A late or duplicated success can only raise `matchIndex`, and a stale
  rejection can only move `nextIndex` back toward `matchIndex + 1`.
- **Reads count acknowledgements by round.** Every leader message carries a
  heartbeat sequence number and send timestamp, and replies echo both. A
  read-index read registered at round *r* completes once a majority has
  acknowledged a message from round *r* or later. The lease starts at the
  send time of the latest message a majority acknowledged, not the time the
  acknowledgement arrived.
- **One election timer, moving deadline.** Heartbeats push the deadline
  forward instead of cancelling and rescheduling. In the simulator this keeps
  the event queue from filling with cancelled timers.
- **Proposals resolve on apply.** A proposal's callback fires when its index
  is applied, and only if the applied entry has the proposal's term.
  Otherwise the client gets `dropped` and retries with the same sequence
  number.

## Roadmap

- Joint-consensus membership changes (§6), with the chaos runner adding and
  removing nodes.
- Pre-vote (§9.6) as an alternative to the vote guard, and leadership transfer.
- Pipelined replication with flow control instead of resending from
  `nextIndex` each round.
- Chunked `InstallSnapshot` and snapshotting off the apply path.
- Session expiry so the dedup table does not grow without bound.
- Group commit: one fsync for many appends in `FileStorage`.
- Fault injection for the real-process mode (kill -9 schedules, `tc`-style
  delays) feeding the same linearizability checker.

## License

MIT. See [LICENSE](LICENSE).
