import type { KvOp, KvResult } from '../kv/types.js';

/**
 * One client operation as observed from outside the system. `call` and
 * `ret` are positions in a single global event order (not wall time), so
 * "returned before invoked" is unambiguous even for events at the same
 * virtual instant.
 */
export interface Operation {
  id: number;
  clientId: string;
  op: KvOp;
  call: number;
  /** null when the client never learned the outcome (crash, timeout, end of run). */
  ret: number | null;
  result: KvResult | null;
  /** Virtual times, for debugging output only. */
  callTime: number;
  retTime: number | null;
}

export class History {
  private readonly ops: Operation[] = [];
  private counter = 0;

  invoke(clientId: string, op: KvOp, time: number): number {
    const id = this.ops.length;
    this.ops.push({
      id,
      clientId,
      op,
      call: this.counter++,
      ret: null,
      result: null,
      callTime: time,
      retTime: null,
    });
    return id;
  }

  complete(id: number, result: KvResult, time: number): void {
    const op = this.ops[id];
    if (!op) throw new Error(`unknown operation ${id}`);
    if (op.ret !== null) throw new Error(`operation ${id} completed twice`);
    op.ret = this.counter++;
    op.result = result;
    op.retTime = time;
  }

  operations(): readonly Operation[] {
    return this.ops;
  }

  get size(): number {
    return this.ops.length;
  }
}

export function describeOp(o: Operation): string {
  const op = o.op;
  const args =
    op.type === 'get' || op.type === 'delete'
      ? `${op.key}`
      : op.type === 'put'
        ? `${op.key}=${op.value}`
        : `${op.key}: ${op.expected}->${op.value}`;
  const res = o.result === null ? '?' : JSON.stringify(o.result);
  const ret = o.retTime === null ? 'never' : `${o.retTime.toFixed(1)}`;
  return `#${o.id} ${o.clientId} ${op.type}(${args}) -> ${res} [${o.callTime.toFixed(1)}..${ret}]`;
}
