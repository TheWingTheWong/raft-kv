import type { KvOp, KvResult } from '../kv/types.js';
import { describeOp, type Operation } from './history.js';

/**
 * Linearizability checker for KV histories.
 *
 * Implements the Wing & Gong search with Lowe's memoisation (as used by
 * Knossos and Porcupine): repeatedly pick a pending operation whose call
 * precedes every outstanding return, check it against a sequential model,
 * and backtrack when stuck; (linearized-set, model-state) pairs already seen
 * are pruned. Every KV operation touches exactly one key, so the history is
 * split per key and each key is checked independently (P-compositionality),
 * which keeps the search small.
 *
 * Operations whose outcome is unknown (`ret === null`) may take effect at any
 * point after their call, or never; this is modelled by giving them a return
 * event after everything else.
 */

type KeyState = string | null;

export interface LinearizabilityResult {
  ok: boolean;
  /** Key whose sub-history has no valid linearization. */
  key?: string;
  /** That key's operations, for debugging. */
  operations?: string[];
  /** Search steps taken, summed over keys. */
  steps: number;
}

export interface CheckOptions {
  /** Abort (throw) if the search for a single key exceeds this many steps. */
  maxSteps?: number;
}

/** Sequential specification of a single key. */
function step(state: KeyState, op: KvOp, out: KvResult | null): [boolean, KeyState] {
  // With an unknown result any observation is acceptable; only the effect matters.
  const sees = (v: KeyState) => out === null || out.value === v;
  switch (op.type) {
    case 'get':
      return [sees(state), state];
    case 'put':
      return [sees(state), op.value];
    case 'delete':
      return [sees(state), null];
    case 'cas': {
      const swapped = state === op.expected;
      const ok = out === null || (out.value === state && out.swapped === swapped);
      return [ok, swapped ? op.value : state];
    }
  }
}

interface Node {
  op: Operation;
  isCall: boolean;
  /** For a call: its return node. */
  match: Node | null;
  prev: Node | null;
  next: Node | null;
}

export function checkLinearizable(
  history: readonly Operation[],
  options: CheckOptions = {},
): LinearizabilityResult {
  const byKey = new Map<string, Operation[]>();
  for (const op of history) {
    const list = byKey.get(op.op.key);
    if (list) list.push(op);
    else byKey.set(op.op.key, [op]);
  }
  let steps = 0;
  for (const [key, ops] of [...byKey.entries()].sort(([a], [b]) => (a < b ? -1 : 1))) {
    const r = checkKey(ops, options.maxSteps ?? 5_000_000);
    steps += r.steps;
    if (!r.ok) {
      return { ok: false, key, operations: ops.map(describeOp), steps };
    }
  }
  return { ok: true, steps };
}

function checkKey(ops: Operation[], maxSteps: number): { ok: boolean; steps: number } {
  // Build the event list ordered by global event position; unknown returns
  // go last.
  const events: Array<{ at: number; node: Node }> = [];
  for (const op of ops) {
    const call: Node = { op, isCall: true, match: null, prev: null, next: null };
    const ret: Node = { op, isCall: false, match: null, prev: null, next: null };
    call.match = ret;
    events.push({ at: op.call, node: call });
    events.push({ at: op.ret ?? Number.MAX_SAFE_INTEGER, node: ret });
  }
  events.sort((a, b) => a.at - b.at || Number(b.node.isCall) - Number(a.node.isCall));

  const head: Node = { op: null as unknown as Operation, isCall: false, match: null, prev: null, next: null };
  let tail = head;
  for (const { node } of events) {
    tail.next = node;
    node.prev = tail;
    tail = node;
  }

  // Dense ids for the bitset.
  const index = new Map<Operation, number>();
  ops.forEach((op, i) => index.set(op, i));
  const linearized = new Uint32Array(Math.ceil(ops.length / 32));
  const seen = new Set<string>();
  const stack: Array<{ node: Node; state: KeyState }> = [];

  const lift = (n: Node) => {
    n.prev!.next = n.next;
    if (n.next) n.next.prev = n.prev;
    const r = n.match!;
    r.prev!.next = r.next;
    if (r.next) r.next.prev = r.prev;
  };
  const unlift = (n: Node) => {
    const r = n.match!;
    r.prev!.next = r;
    if (r.next) r.next.prev = r;
    n.prev!.next = n;
    if (n.next) n.next.prev = n;
  };

  let state: KeyState = null;
  let entry = head.next;
  let steps = 0;
  while (head.next) {
    if (++steps > maxSteps) throw new Error(`linearizability search exceeded ${maxSteps} steps`);
    if (!entry) return { ok: false, steps }; // unreachable: a return always precedes the end
    if (entry.isCall) {
      const [ok, next] = step(state, entry.op.op, entry.op.ret === null ? null : entry.op.result);
      if (ok) {
        const id = index.get(entry.op)!;
        linearized[id >> 5]! |= 1 << (id & 31);
        const key = `${linearized.join(',')}|${next}`;
        if (!seen.has(key)) {
          seen.add(key);
          stack.push({ node: entry, state });
          state = next;
          lift(entry);
          entry = head.next;
          continue;
        }
        linearized[id >> 5]! &= ~(1 << (id & 31));
      }
      entry = entry.next;
    } else {
      // Reached a return whose operation is not linearized yet: backtrack.
      const top = stack.pop();
      if (!top) return { ok: false, steps };
      const id = index.get(top.node.op)!;
      linearized[id >> 5]! &= ~(1 << (id & 31));
      state = top.state;
      unlift(top.node);
      entry = top.node.next;
    }
  }
  return { ok: true, steps };
}
