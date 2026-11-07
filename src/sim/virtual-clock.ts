import type { Clock, Timer } from '../raft/clock.js';

interface ScheduledEvent {
  at: number;
  seq: number;
  fn: () => void;
  cancelled: boolean;
}

/**
 * Discrete-event clock. Time only moves when the simulation advances it, and
 * events scheduled for the same instant run in scheduling order, so a run is
 * a pure function of its inputs.
 */
export class VirtualClock implements Clock {
  private time = 0;
  private seq = 0;
  private readonly heap: ScheduledEvent[] = [];
  private executed = 0;

  now(): number {
    return this.time;
  }

  /** Number of events executed so far (useful for budgets and benchmarks). */
  get eventsExecuted(): number {
    return this.executed;
  }

  setTimer(delayMs: number, fn: () => void): Timer {
    const ev: ScheduledEvent = {
      at: this.time + Math.max(0, delayMs),
      seq: this.seq++,
      fn,
      cancelled: false,
    };
    this.push(ev);
    return {
      cancel: () => {
        ev.cancelled = true;
      },
    };
  }

  /** Run the next pending event. Returns false when nothing is scheduled. */
  step(): boolean {
    for (;;) {
      const ev = this.pop();
      if (!ev) return false;
      if (ev.cancelled) continue;
      this.time = ev.at;
      this.executed++;
      ev.fn();
      return true;
    }
  }

  /** Run every event scheduled at or before `time`, then set the clock to `time`. */
  runUntil(time: number): void {
    while (this.heap.length > 0 && this.heap[0]!.at <= time) {
      this.step();
    }
    if (time > this.time) this.time = time;
  }

  runFor(ms: number): void {
    this.runUntil(this.time + ms);
  }

  /**
   * Advance until `predicate` holds or `maxMs` of virtual time elapse.
   * Returns whether the predicate was satisfied.
   */
  runUntilTrue(predicate: () => boolean, maxMs: number): boolean {
    const deadline = this.time + maxMs;
    if (predicate()) return true;
    while (this.heap.length > 0 && this.heap[0]!.at <= deadline) {
      this.step();
      if (predicate()) return true;
    }
    if (deadline > this.time) this.time = deadline;
    return predicate();
  }

  private push(ev: ScheduledEvent): void {
    const h = this.heap;
    h.push(ev);
    let i = h.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (!before(h[i]!, h[parent]!)) break;
      [h[i], h[parent]] = [h[parent]!, h[i]!];
      i = parent;
    }
  }

  private pop(): ScheduledEvent | undefined {
    const h = this.heap;
    if (h.length === 0) return undefined;
    const top = h[0]!;
    const last = h.pop()!;
    if (h.length > 0) {
      h[0] = last;
      let i = 0;
      for (;;) {
        const l = 2 * i + 1;
        const r = l + 1;
        let m = i;
        if (l < h.length && before(h[l]!, h[m]!)) m = l;
        if (r < h.length && before(h[r]!, h[m]!)) m = r;
        if (m === i) break;
        [h[i], h[m]] = [h[m]!, h[i]!];
        i = m;
      }
    }
    return top;
  }
}

function before(a: ScheduledEvent, b: ScheduledEvent): boolean {
  return a.at < b.at || (a.at === b.at && a.seq < b.seq);
}
