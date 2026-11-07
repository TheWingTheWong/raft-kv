import { KvServer, type KvRequest, type KvResponse } from '../kv/server.js';
import { KvStateMachine } from '../kv/state-machine.js';
import type { KvCommand } from '../kv/types.js';
import { RaftLog } from '../raft/log.js';
import { RaftNode, type RaftObserver } from '../raft/node.js';
import { MemoryStorage } from '../raft/storage.js';
import type { NodeId, RaftConfig, RaftMessage } from '../raft/types.js';
import { History } from '../check/history.js';
import { SimNetwork, type NetworkFaults } from './network.js';
import { Rng } from './rng.js';
import { VirtualClock } from './virtual-clock.js';

/** Everything that crosses the simulated wire. */
export type Envelope =
  | { kind: 'raft'; msg: RaftMessage<KvCommand> }
  | { kind: 'kv-req'; opId: number; req: KvRequest }
  | { kind: 'kv-res'; opId: number; res: KvResponse };

export interface SimClusterOptions {
  /** Number of servers (ids n1..nN) or explicit ids. */
  nodes: number | NodeId[];
  seed: number;
  network?: Partial<NetworkFaults>;
  raft?: Partial<RaftConfig>;
  observers?: RaftObserver<KvCommand>[];
}

interface LiveNode {
  raft: RaftNode<KvCommand>;
  kv: KvServer;
  sm: KvStateMachine;
}

/** Simulator defaults: perfect clocks, so no drift margin on the lease. */
const SIM_RAFT_CONFIG: Partial<RaftConfig> = { clockDriftBound: 0 };

/**
 * A whole cluster in one process: servers, clients and network share one
 * virtual clock and one seeded RNG, so a seed reproduces a run exactly.
 */
export class SimCluster {
  readonly clock = new VirtualClock();
  readonly rng: Rng;
  readonly net: SimNetwork<Envelope>;
  readonly ids: readonly NodeId[];
  readonly history = new History();

  private readonly storages = new Map<NodeId, MemoryStorage<KvCommand>>();
  private readonly live = new Map<NodeId, LiveNode>();
  private readonly nodeRng = new Map<NodeId, Rng>();
  private readonly raftConfig: Partial<RaftConfig>;
  private readonly observers: RaftObserver<KvCommand>[];

  constructor(options: SimClusterOptions) {
    this.rng = new Rng(options.seed);
    this.net = new SimNetwork(this.clock, this.rng.fork(), options.network);
    this.ids =
      typeof options.nodes === 'number'
        ? Array.from({ length: options.nodes }, (_, i) => `n${i + 1}`)
        : [...options.nodes];
    this.raftConfig = { ...SIM_RAFT_CONFIG, ...options.raft };
    this.observers = options.observers ?? [];
    for (const id of this.ids) {
      this.storages.set(id, new MemoryStorage());
      this.nodeRng.set(id, this.rng.fork());
      this.start(id);
    }
  }

  // ---- membership & faults -------------------------------------------------

  start(id: NodeId): void {
    if (this.live.has(id)) throw new Error(`${id} is already running`);
    const sm = new KvStateMachine();
    const rng = this.nodeRng.get(id)!;
    const raft = new RaftNode<KvCommand>({
      id,
      members: [...this.ids],
      storage: this.storages.get(id)!,
      stateMachine: sm,
      clock: this.clock,
      transport: { send: (to, msg) => this.net.send(id, to, { kind: 'raft', msg }) },
      random: () => rng.next(),
      config: this.raftConfig,
      observer: fanOut(this.observers),
    });
    const kv = new KvServer(raft, sm);
    this.live.set(id, { raft, kv, sm });
    this.net.register(id, (from, env) => {
      if (env.kind === 'raft') raft.receive(from, env.msg);
      else if (env.kind === 'kv-req') {
        kv.handle(env.req, (res) => {
          if (!raft.isStopped) this.net.send(id, from, { kind: 'kv-res', opId: env.opId, res });
        });
      }
    });
    raft.start();
  }

  /** Kill a server. Durable state survives in its storage; everything else is lost. */
  crash(id: NodeId): void {
    const node = this.live.get(id);
    if (!node) return;
    this.net.unregister(id);
    node.raft.stop();
    this.live.delete(id);
  }

  restart(id: NodeId): void {
    this.crash(id);
    this.start(id);
  }

  isUp(id: NodeId): boolean {
    return this.live.has(id);
  }

  upIds(): NodeId[] {
    return this.ids.filter((id) => this.live.has(id));
  }

  downIds(): NodeId[] {
    return this.ids.filter((id) => !this.live.has(id));
  }

  partition(...groups: NodeId[][]): void {
    this.net.partition(groups);
  }

  isolate(id: NodeId): void {
    this.net.partition([[id], this.ids.filter((x) => x !== id)]);
  }

  heal(): void {
    this.net.heal();
  }

  // ---- inspection ----------------------------------------------------------

  node(id: NodeId): RaftNode<KvCommand> {
    const n = this.live.get(id);
    if (!n) throw new Error(`${id} is down`);
    return n.raft;
  }

  stateMachine(id: NodeId): KvStateMachine {
    const n = this.live.get(id);
    if (!n) throw new Error(`${id} is down`);
    return n.sm;
  }

  /** The running leader with the highest term, if any. */
  leader(): RaftNode<KvCommand> | undefined {
    let best: RaftNode<KvCommand> | undefined;
    for (const { raft } of this.live.values()) {
      if (raft.role === 'leader' && (!best || raft.term > best.term)) best = raft;
    }
    return best;
  }

  /** Durable log of any node, up or down, as recorded in its storage. */
  durableLog(id: NodeId): RaftLog<KvCommand> {
    const state = this.storages.get(id)!.load();
    return new RaftLog(
      state.snapshot?.lastIncludedIndex ?? 0,
      state.snapshot?.lastIncludedTerm ?? 0,
      state.entries,
    );
  }

  // ---- time ----------------------------------------------------------------

  get now(): number {
    return this.clock.now();
  }

  runFor(ms: number): void {
    this.clock.runFor(ms);
  }

  runUntil(predicate: () => boolean, maxMs: number): boolean {
    return this.clock.runUntilTrue(predicate, maxMs);
  }

  /** Run until a leader exists whose term no other live node exceeds. */
  waitForLeader(maxMs = 5_000): RaftNode<KvCommand> {
    const ok = this.runUntil(() => {
      const l = this.leader();
      return !!l && this.upIds().every((id) => this.node(id).term <= l.term);
    }, maxMs);
    const leader = this.leader();
    if (!ok || !leader) throw new Error(`no stable leader within ${maxMs}ms`);
    return leader;
  }
}

function fanOut<C>(observers: RaftObserver<C>[]): RaftObserver<C> | undefined {
  if (observers.length === 0) return undefined;
  if (observers.length === 1) return observers[0];
  return {
    onRoleChange: (n, r, t) => observers.forEach((o) => o.onRoleChange?.(n, r, t)),
    onCommit: (n, i, t) => observers.forEach((o) => o.onCommit?.(n, i, t)),
    onApply: (n, e, r) => observers.forEach((o) => o.onApply?.(n, e, r)),
  };
}
