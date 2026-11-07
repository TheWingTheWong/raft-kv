import type { KvOp, KvResult, ReadMode } from '../kv/types.js';
import type { Timer } from '../raft/clock.js';
import type { NodeId } from '../raft/types.js';
import type { Envelope, SimCluster } from './cluster.js';

export interface SimClientOptions {
  /** Give up on an attempt and retry elsewhere after this long (ms). */
  attemptTimeout?: number;
  /** Pause before retrying after a definite "not leader" answer (ms). */
  retryBackoff?: number;
  readMode?: ReadMode;
}

interface InFlight {
  opId: number;
  seq: number;
  op: KvOp;
  historyId: number;
  callback: (result: KvResult) => void;
  timer: Timer | null;
}

/**
 * A closed-loop client: one operation at a time, retried until it succeeds,
 * with a stable (clientId, seq) so retries are deduplicated by the cluster.
 * Every operation is recorded in the cluster's History for later checking.
 */
export class SimClient {
  private seq = 0;
  private nextOpId = 0;
  private leaderHint: NodeId | null = null;
  private current: InFlight | null = null;
  private readonly attemptTimeout: number;
  private readonly retryBackoff: number;
  readonly readMode: ReadMode;
  completed = 0;

  constructor(
    private readonly cluster: SimCluster,
    readonly id: string,
    options: SimClientOptions = {},
  ) {
    this.attemptTimeout = options.attemptTimeout ?? 400;
    this.retryBackoff = options.retryBackoff ?? 20;
    this.readMode = options.readMode ?? 'read_index';
    cluster.net.register(id, (_from, env) => this.onMessage(env));
  }

  get busy(): boolean {
    return this.current !== null;
  }

  submit(op: KvOp, callback: (result: KvResult) => void = () => {}): void {
    if (this.current) throw new Error(`${this.id} already has an operation in flight`);
    this.current = {
      opId: this.nextOpId++,
      seq: ++this.seq,
      op,
      historyId: this.cluster.history.invoke(this.id, op, this.cluster.now),
      callback,
      timer: null,
    };
    this.attempt();
  }

  private attempt(): void {
    const cur = this.current;
    if (!cur) return;
    const target = this.leaderHint ?? this.cluster.rng.pick(this.cluster.ids);
    this.cluster.net.send(this.id, target, {
      kind: 'kv-req',
      opId: cur.opId,
      req: { clientId: this.id, seq: cur.seq, op: cur.op, readMode: this.readMode },
    });
    cur.timer?.cancel();
    cur.timer = this.cluster.clock.setTimer(this.attemptTimeout, () => {
      // No answer: the target may be dead or partitioned. Forget the hint.
      if (this.current !== cur) return;
      this.leaderHint = null;
      this.attempt();
    });
  }

  private onMessage(env: Envelope): void {
    if (env.kind !== 'kv-res') return;
    const cur = this.current;
    // Responses to earlier attempts of the current op are as good as any:
    // writes are deduplicated and every read was linearizable when served.
    if (!cur || env.opId !== cur.opId) return;
    const res = env.res;
    if (res.ok) {
      cur.timer?.cancel();
      this.current = null;
      this.completed++;
      this.cluster.history.complete(cur.historyId, res.result, this.cluster.now);
      cur.callback(res.result);
      return;
    }
    this.leaderHint = res.error === 'not_leader' ? res.leaderHint : null;
    cur.timer?.cancel();
    cur.timer = this.cluster.clock.setTimer(this.retryBackoff, () => {
      if (this.current === cur) this.attempt();
    });
  }
}
