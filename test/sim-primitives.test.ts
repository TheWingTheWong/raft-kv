import { describe, expect, it } from 'vitest';
import { Rng } from '../src/sim/rng.js';
import { VirtualClock } from '../src/sim/virtual-clock.js';

describe('Rng', () => {
  it('is deterministic for a given seed', () => {
    const a = new Rng(42);
    const b = new Rng(42);
    const xs = Array.from({ length: 100 }, () => a.next());
    const ys = Array.from({ length: 100 }, () => b.next());
    expect(xs).toEqual(ys);
  });

  it('produces different streams for different seeds', () => {
    expect(new Rng(1).next()).not.toEqual(new Rng(2).next());
  });

  it('keeps values in range', () => {
    const rng = new Rng(7);
    for (let i = 0; i < 10_000; i++) {
      const x = rng.next();
      expect(x).toBeGreaterThanOrEqual(0);
      expect(x).toBeLessThan(1);
      const n = rng.int(3, 9);
      expect(n).toBeGreaterThanOrEqual(3);
      expect(n).toBeLessThan(9);
    }
  });

  it('shuffles into a permutation', () => {
    const out = new Rng(3).shuffle([1, 2, 3, 4, 5, 6]);
    expect(out.slice().sort()).toEqual([1, 2, 3, 4, 5, 6]);
  });
});

describe('VirtualClock', () => {
  it('runs events in time order, ties broken by scheduling order', () => {
    const clock = new VirtualClock();
    const seen: string[] = [];
    clock.setTimer(20, () => seen.push('b'));
    clock.setTimer(10, () => seen.push('a'));
    clock.setTimer(20, () => seen.push('c'));
    clock.runFor(100);
    expect(seen).toEqual(['a', 'b', 'c']);
    expect(clock.now()).toBe(100);
  });

  it('skips cancelled timers', () => {
    const clock = new VirtualClock();
    let fired = false;
    const t = clock.setTimer(5, () => (fired = true));
    t.cancel();
    clock.runFor(10);
    expect(fired).toBe(false);
  });

  it('does not run events past the horizon', () => {
    const clock = new VirtualClock();
    const seen: number[] = [];
    clock.setTimer(5, () => {
      seen.push(clock.now());
      clock.setTimer(10, () => seen.push(clock.now()));
    });
    clock.runUntil(10);
    expect(seen).toEqual([5]);
    clock.runUntil(15);
    expect(seen).toEqual([5, 15]);
  });

  it('stops early when a predicate becomes true', () => {
    const clock = new VirtualClock();
    let n = 0;
    const tick = () => {
      n++;
      clock.setTimer(10, tick);
    };
    clock.setTimer(10, tick);
    expect(clock.runUntilTrue(() => n === 3, 1000)).toBe(true);
    expect(clock.now()).toBe(30);
  });
});
