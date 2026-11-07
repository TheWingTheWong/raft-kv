/** A cancellable scheduled callback. */
export interface Timer {
  cancel(): void;
}

/**
 * Source of time and timers for a Raft node. The node never touches
 * `Date.now()` or `setTimeout` directly, which is what lets the simulator
 * substitute a virtual clock and run thousands of cluster-seconds per second.
 */
export interface Clock {
  /** Monotonic milliseconds. */
  now(): number;
  setTimer(delayMs: number, fn: () => void): Timer;
}

/** Wall-clock implementation used by the HTTP deployment. */
export class RealClock implements Clock {
  now(): number {
    return performance.now();
  }

  setTimer(delayMs: number, fn: () => void): Timer {
    const handle = setTimeout(fn, Math.max(0, delayMs));
    return { cancel: () => clearTimeout(handle) };
  }
}
