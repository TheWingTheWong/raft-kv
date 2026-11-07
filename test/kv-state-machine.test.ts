import { describe, expect, it } from 'vitest';
import { KvStateMachine } from '../src/kv/state-machine.js';
import type { KvOp } from '../src/kv/types.js';

let seq = 0;
const cmd = (op: KvOp, clientId = 'c1', s = ++seq) => ({ clientId, seq: s, op });

describe('KvStateMachine', () => {
  it('supports get/put/delete returning the previous value', () => {
    const sm = new KvStateMachine();
    expect(sm.apply(cmd({ type: 'get', key: 'a' }))).toEqual({ value: null });
    expect(sm.apply(cmd({ type: 'put', key: 'a', value: '1' }))).toEqual({ value: null });
    expect(sm.apply(cmd({ type: 'put', key: 'a', value: '2' }))).toEqual({ value: '1' });
    expect(sm.query({ type: 'get', key: 'a' })).toEqual({ value: '2' });
    expect(sm.apply(cmd({ type: 'delete', key: 'a' }))).toEqual({ value: '2' });
    expect(sm.query({ type: 'get', key: 'a' })).toEqual({ value: null });
  });

  it('compare-and-swaps only on a match, including against absence', () => {
    const sm = new KvStateMachine();
    expect(sm.apply(cmd({ type: 'cas', key: 'k', expected: null, value: 'x' }))).toEqual({
      value: null,
      swapped: true,
    });
    expect(sm.apply(cmd({ type: 'cas', key: 'k', expected: 'y', value: 'z' }))).toEqual({
      value: 'x',
      swapped: false,
    });
    expect(sm.query({ type: 'get', key: 'k' })).toEqual({ value: 'x' });
  });

  it('applies a retried command exactly once and replays its result', () => {
    const sm = new KvStateMachine();
    sm.apply({ clientId: 'w', seq: 1, op: { type: 'put', key: 'n', value: 'a' } });
    sm.apply({ clientId: 'other', seq: 1, op: { type: 'put', key: 'n', value: 'b' } });
    // A duplicate delivery of w#1 must not clobber the later write.
    expect(sm.apply({ clientId: 'w', seq: 1, op: { type: 'put', key: 'n', value: 'a' } })).toEqual({
      value: null,
    });
    expect(sm.query({ type: 'get', key: 'n' })).toEqual({ value: 'b' });
    // An even older straggler is ignored.
    sm.apply({ clientId: 'w', seq: 2, op: { type: 'put', key: 'm', value: '1' } });
    expect(sm.apply({ clientId: 'w', seq: 1, op: { type: 'put', key: 'n', value: 'a' } })).toBeNull();
  });

  it('does not deduplicate anonymous commands', () => {
    const sm = new KvStateMachine();
    sm.apply({ clientId: '', seq: 0, op: { type: 'put', key: 'a', value: '1' } });
    expect(sm.apply({ clientId: '', seq: 0, op: { type: 'put', key: 'a', value: '2' } })).toEqual({
      value: '1',
    });
  });

  it('round-trips through a snapshot, sessions included', () => {
    const sm = new KvStateMachine();
    sm.apply({ clientId: 'w', seq: 4, op: { type: 'put', key: 'a', value: '1' } });
    const snap = JSON.parse(JSON.stringify(sm.snapshot()));
    const copy = new KvStateMachine();
    copy.restore(snap);
    expect(copy.entries()).toEqual([['a', '1']]);
    expect(copy.apply({ clientId: 'w', seq: 4, op: { type: 'put', key: 'a', value: '9' } })).toEqual({
      value: null,
    });
    expect(copy.entries()).toEqual([['a', '1']]);
  });
});
