import type { VirtualClock } from './virtual-clock.js';
import type { Rng } from './rng.js';

export interface NetworkFaults {
  /** Minimum one-way latency (ms). */
  minDelay: number;
  /** Maximum one-way latency (ms). Random latency alone already reorders. */
  maxDelay: number;
  /** Probability a message is silently lost. */
  dropRate: number;
  /** Probability a message is delivered twice. */
  duplicateRate: number;
  /** Probability a message is held back for up to `reorderDelay` extra ms. */
  reorderRate: number;
  reorderDelay: number;
}

export const RELIABLE_NETWORK: NetworkFaults = {
  minDelay: 1,
  maxDelay: 5,
  dropRate: 0,
  duplicateRate: 0,
  reorderRate: 0,
  reorderDelay: 0,
};

export type Handler<M> = (from: string, message: M) => void;

export interface NetworkStats {
  sent: number;
  delivered: number;
  dropped: number;
  duplicated: number;
}

/**
 * Deterministic in-process network. Messages are cloned on send (nodes never
 * share object references, just as with real serialisation) and delivered
 * through the virtual clock. Partitions are checked both at send time and at
 * delivery time, so messages in flight when a partition starts are lost.
 */
export class SimNetwork<M> {
  faults: NetworkFaults;
  readonly stats: NetworkStats = { sent: 0, delivered: 0, dropped: 0, duplicated: 0 };
  private readonly handlers = new Map<string, Handler<M>>();
  private readonly blocked = new Set<string>();

  constructor(
    private readonly clock: VirtualClock,
    private readonly rng: Rng,
    faults: Partial<NetworkFaults> = {},
  ) {
    this.faults = { ...RELIABLE_NETWORK, ...faults };
  }

  register(id: string, handler: Handler<M>): void {
    this.handlers.set(id, handler);
  }

  unregister(id: string): void {
    this.handlers.delete(id);
  }

  send(from: string, to: string, message: M): void {
    this.stats.sent++;
    if (!this.handlers.has(to) || this.isBlocked(from, to) || this.rng.chance(this.faults.dropRate)) {
      this.stats.dropped++;
      return;
    }
    const copies = this.rng.chance(this.faults.duplicateRate) ? 2 : 1;
    if (copies === 2) this.stats.duplicated++;
    for (let i = 0; i < copies; i++) {
      const payload = structuredClone(message);
      this.clock.setTimer(this.latency(), () => {
        const handler = this.handlers.get(to);
        if (!handler || this.isBlocked(from, to)) {
          this.stats.dropped++;
          return;
        }
        this.stats.delivered++;
        handler(from, payload);
      });
    }
  }

  /**
   * Split the listed nodes into groups that cannot talk across group
   * boundaries. Nodes not mentioned (e.g. clients) still reach everyone.
   */
  partition(groups: string[][]): void {
    this.blocked.clear();
    for (let i = 0; i < groups.length; i++) {
      for (let j = 0; j < groups.length; j++) {
        if (i === j) continue;
        for (const a of groups[i]!) for (const b of groups[j]!) this.blocked.add(link(a, b));
      }
    }
  }

  /** Cut a single bidirectional link. */
  cut(a: string, b: string): void {
    this.blocked.add(link(a, b));
    this.blocked.add(link(b, a));
  }

  heal(): void {
    this.blocked.clear();
  }

  isBlocked(from: string, to: string): boolean {
    return this.blocked.has(link(from, to));
  }

  private latency(): number {
    const f = this.faults;
    let d = this.rng.float(f.minDelay, f.maxDelay);
    if (this.rng.chance(f.reorderRate)) d += this.rng.float(0, f.reorderDelay);
    return d;
  }
}

function link(a: string, b: string): string {
  return `${a}->${b}`;
}
