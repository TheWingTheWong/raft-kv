import type { StateMachine } from '../raft/state-machine.js';
import type { KvCommand, KvOp, KvResult } from './types.js';

interface Session {
  seq: number;
  result: KvResult;
}

interface KvSnapshot {
  data: Array<[string, string]>;
  sessions: Array<[string, Session]>;
}

/** Deterministic string map with exactly-once client sessions. */
export class KvStateMachine implements StateMachine<KvCommand, KvOp> {
  private data = new Map<string, string>();
  private sessions = new Map<string, Session>();

  apply(cmd: KvCommand): KvResult | null {
    if (cmd.clientId !== '') {
      const session = this.sessions.get(cmd.clientId);
      if (session && cmd.seq <= session.seq) {
        // Duplicate of an already-applied request: return the original
        // result. Anything older is a straggler nobody is waiting for.
        return cmd.seq === session.seq ? session.result : null;
      }
    }
    const result = this.execute(cmd.op);
    if (cmd.clientId !== '') this.sessions.set(cmd.clientId, { seq: cmd.seq, result });
    return result;
  }

  query(op: KvOp): KvResult {
    if (op.type !== 'get') throw new Error(`query only supports get, got ${op.type}`);
    return { value: this.data.get(op.key) ?? null };
  }

  snapshot(): KvSnapshot {
    return {
      data: [...this.data.entries()].sort(byKey),
      sessions: [...this.sessions.entries()].sort(byKey).map(([k, s]) => [k, structuredClone(s)]),
    };
  }

  restore(raw: unknown): void {
    const snap = raw as KvSnapshot;
    this.data = new Map(snap.data);
    this.sessions = new Map(snap.sessions.map(([k, s]) => [k, structuredClone(s)]));
  }

  /** Read-only view of the data, for tests and debugging. */
  entries(): Array<[string, string]> {
    return [...this.data.entries()].sort(byKey);
  }

  private execute(op: KvOp): KvResult {
    const prev = this.data.get(op.key) ?? null;
    switch (op.type) {
      case 'get':
        return { value: prev };
      case 'put':
        this.data.set(op.key, op.value);
        return { value: prev };
      case 'delete':
        this.data.delete(op.key);
        return { value: prev };
      case 'cas': {
        const swapped = prev === op.expected;
        if (swapped) this.data.set(op.key, op.value);
        return { value: prev, swapped };
      }
    }
  }
}

function byKey<T>(a: [string, T], b: [string, T]): number {
  return a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0;
}
