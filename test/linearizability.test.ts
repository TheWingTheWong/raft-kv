import { describe, expect, it } from 'vitest';
import { History } from '../src/check/history.js';
import { checkLinearizable } from '../src/check/linearizability.js';
import type { KvOp, KvResult } from '../src/kv/types.js';

/**
 * Tiny DSL: build a history from a timeline of call/return events so tests
 * read like the diagrams in the linearizability literature.
 */
class Timeline {
  readonly h = new History();
  private ids = new Map<string, number>();
  call(name: string, client: string, op: KvOp): this {
    this.ids.set(name, this.h.invoke(client, op, 0));
    return this;
  }
  ret(name: string, result: KvResult): this {
    this.h.complete(this.ids.get(name)!, result, 0);
    return this;
  }
  check() {
    return checkLinearizable(this.h.operations());
  }
}

const put = (key: string, value: string): KvOp => ({ type: 'put', key, value });
const get = (key: string): KvOp => ({ type: 'get', key });

describe('checkLinearizable', () => {
  it('accepts a sequential history', () => {
    const t = new Timeline()
      .call('w', 'a', put('x', '1'))
      .ret('w', { value: null })
      .call('r', 'b', get('x'))
      .ret('r', { value: '1' });
    expect(t.check().ok).toBe(true);
  });

  it('rejects a stale read after a completed write', () => {
    const t = new Timeline()
      .call('w', 'a', put('x', '1'))
      .ret('w', { value: null })
      .call('r', 'b', get('x'))
      .ret('r', { value: null });
    const r = t.check();
    expect(r.ok).toBe(false);
    expect(r.key).toBe('x');
  });

  it('accepts either order for overlapping operations', () => {
    for (const seen of [null, '1']) {
      const t = new Timeline()
        .call('w', 'a', put('x', '1'))
        .call('r', 'b', get('x'))
        .ret('r', { value: seen })
        .ret('w', { value: null });
      expect(t.check().ok).toBe(true);
    }
  });

  it('rejects reads that observe values in an impossible order', () => {
    // Two writes overlap; reader 1 sees 2 then 1, reader 2 sees 1 then 2,
    // with all reads after both writes finished.
    const t = new Timeline()
      .call('w1', 'a', put('x', '1'))
      .call('w2', 'b', put('x', '2'))
      .ret('w1', { value: null })
      .ret('w2', { value: '1' })
      .call('r1', 'c', get('x'))
      .ret('r1', { value: '2' })
      .call('r2', 'd', get('x'))
      .ret('r2', { value: '1' });
    expect(t.check().ok).toBe(false);
  });

  it('lets an unacknowledged write take effect late or never', () => {
    const late = new Timeline()
      .call('w', 'a', put('x', '1')) // never returns
      .call('r1', 'b', get('x'))
      .ret('r1', { value: null })
      .call('r2', 'b', get('x'))
      .ret('r2', { value: '1' });
    expect(late.check().ok).toBe(true);

    const never = new Timeline()
      .call('w', 'a', put('x', '1'))
      .call('r1', 'b', get('x'))
      .ret('r1', { value: null });
    expect(never.check().ok).toBe(true);

    const flicker = new Timeline()
      .call('w', 'a', put('x', '1'))
      .call('r1', 'b', get('x'))
      .ret('r1', { value: '1' })
      .call('r2', 'b', get('x'))
      .ret('r2', { value: null });
    expect(flicker.check().ok).toBe(false);
  });

  it('checks compare-and-swap outcomes against the model', () => {
    const ok = new Timeline()
      .call('w', 'a', put('x', '1'))
      .ret('w', { value: null })
      .call('c', 'b', { type: 'cas', key: 'x', expected: '1', value: '2' })
      .ret('c', { value: '1', swapped: true });
    expect(ok.check().ok).toBe(true);

    const bad = new Timeline()
      .call('w', 'a', put('x', '1'))
      .ret('w', { value: null })
      .call('c', 'b', { type: 'cas', key: 'x', expected: '1', value: '2' })
      .ret('c', { value: '1', swapped: false });
    expect(bad.check().ok).toBe(false);
  });

  it('checks keys independently', () => {
    const t = new Timeline()
      .call('a', 'a', put('x', '1'))
      .ret('a', { value: null })
      .call('b', 'b', put('y', '1'))
      .ret('b', { value: null })
      .call('r', 'c', get('y'))
      .ret('r', { value: null });
    const r = t.check();
    expect(r.ok).toBe(false);
    expect(r.key).toBe('y');
  });

  it('handles wide concurrency without blowing up', () => {
    const t = new Timeline();
    for (let i = 0; i < 12; i++) t.call(`w${i}`, `c${i}`, put('x', `${i}`));
    let prev: string | null = null;
    for (let i = 0; i < 12; i++) {
      t.ret(`w${i}`, { value: prev });
      prev = `${i}`;
    }
    t.call('r', 'z', get('x')).ret('r', { value: '11' });
    const r = t.check();
    expect(r.ok).toBe(true);
    expect(r.steps).toBeLessThan(10_000);
  });
});
