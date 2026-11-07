/**
 * The deterministic application that Raft replicates. Every node applies the
 * same committed commands in the same order and must reach the same state.
 */
export interface StateMachine<C = unknown, Q = unknown> {
  /** Apply a committed command. The return value is handed to the proposer. */
  apply(command: C, index: number): unknown;
  /** Answer a read-only query against current state (never mutates). */
  query(query: Q): unknown;
  /** Serialise current state. Must be JSON-compatible and not alias live state. */
  snapshot(): unknown;
  /** Replace current state with a snapshot image. */
  restore(data: unknown): void;
}
