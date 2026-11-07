export type KvOp =
  | { type: 'get'; key: string }
  | { type: 'put'; key: string; value: string }
  | { type: 'delete'; key: string }
  | { type: 'cas'; key: string; expected: string | null; value: string };

/**
 * Result of a KV operation. `value` is the value read for `get`, and the
 * value *before* the operation for put/delete/cas, which makes every
 * operation observable to the linearizability checker.
 */
export interface KvResult {
  value: string | null;
  /** Only set for `cas`. */
  swapped?: boolean;
}

/**
 * Commands carry a client session so retried writes apply exactly once
 * (Raft dissertation §6.3). `seq` increases by one per client operation and
 * is reused verbatim on retries. clientId '' opts out of deduplication.
 */
export interface KvCommand {
  clientId: string;
  seq: number;
  op: KvOp;
}

/** Read paths a client can ask for. */
export type ReadMode =
  /** Replicate the read through the log like a write. Slow, simple, safe. */
  | 'log'
  /** Leader confirms leadership with a heartbeat round, then reads locally. */
  | 'read_index'
  /** Leader reads locally while its lease holds; falls back to read_index. */
  | 'lease'
  /** Any node reads its own state. Fast, may be stale. NOT linearizable. */
  | 'local';
