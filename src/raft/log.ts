import type { LogEntry } from './types.js';

/**
 * In-memory view of the replicated log, offset by the last snapshot.
 *
 * Indices are 1-based as in the paper. Entries at or below `snapshotIndex`
 * have been compacted away; only their last term survives (`snapshotTerm`),
 * which is all AppendEntries consistency checks need.
 */
export class RaftLog<C = unknown> {
  private entries: LogEntry<C>[] = [];
  private _snapshotIndex = 0;
  private _snapshotTerm = 0;

  constructor(snapshotIndex = 0, snapshotTerm = 0, entries: LogEntry<C>[] = []) {
    this._snapshotIndex = snapshotIndex;
    this._snapshotTerm = snapshotTerm;
    for (const e of entries) this.append(e);
  }

  get snapshotIndex(): number {
    return this._snapshotIndex;
  }

  get snapshotTerm(): number {
    return this._snapshotTerm;
  }

  get firstIndex(): number {
    return this._snapshotIndex + 1;
  }

  get lastIndex(): number {
    return this._snapshotIndex + this.entries.length;
  }

  get lastTerm(): number {
    const last = this.entries[this.entries.length - 1];
    return last ? last.term : this._snapshotTerm;
  }

  /** Number of live (uncompacted) entries. */
  get length(): number {
    return this.entries.length;
  }

  /**
   * Term of the entry at `index`, or undefined if it is beyond the end of the
   * log or has been compacted (other than the snapshot boundary itself).
   */
  term(index: number): number | undefined {
    if (index === this._snapshotIndex) return this._snapshotTerm;
    if (index < this._snapshotIndex || index > this.lastIndex) return undefined;
    return this.entries[index - this._snapshotIndex - 1]!.term;
  }

  entry(index: number): LogEntry<C> {
    if (index <= this._snapshotIndex || index > this.lastIndex) {
      throw new RangeError(
        `log index ${index} out of range (${this.firstIndex}..${this.lastIndex})`,
      );
    }
    return this.entries[index - this._snapshotIndex - 1]!;
  }

  /** Entries in [from, toExclusive). `from` must be above the snapshot. */
  slice(from: number, toExclusive: number = this.lastIndex + 1): LogEntry<C>[] {
    if (from <= this._snapshotIndex) {
      throw new RangeError(`slice from ${from} reaches into snapshot ${this._snapshotIndex}`);
    }
    const lo = from - this._snapshotIndex - 1;
    const hi = Math.min(toExclusive, this.lastIndex + 1) - this._snapshotIndex - 1;
    return this.entries.slice(lo, Math.max(lo, hi));
  }

  append(entry: LogEntry<C>): void {
    if (entry.index !== this.lastIndex + 1) {
      throw new Error(`non-contiguous append: index ${entry.index} after ${this.lastIndex}`);
    }
    if (entry.term < this.lastTerm) {
      throw new Error(`term regression: entry term ${entry.term} after ${this.lastTerm}`);
    }
    this.entries.push(entry);
  }

  /** Remove `index` and everything after it. */
  truncateFrom(index: number): void {
    if (index <= this._snapshotIndex) {
      throw new Error(`cannot truncate at ${index}: compacted through ${this._snapshotIndex}`);
    }
    this.entries.length = Math.max(0, index - this._snapshotIndex - 1);
  }

  /** Drop everything up to and including `index`, which becomes the new snapshot boundary. */
  compactThrough(index: number, term: number): void {
    if (index <= this._snapshotIndex) return;
    const keepFrom = index - this._snapshotIndex;
    this.entries = keepFrom >= this.entries.length ? [] : this.entries.slice(keepFrom);
    this._snapshotIndex = index;
    this._snapshotTerm = term;
  }

  /** Discard the entire log and restart it after a snapshot. */
  resetToSnapshot(index: number, term: number): void {
    this.entries = [];
    this._snapshotIndex = index;
    this._snapshotTerm = term;
  }

  /** First index (above the snapshot) holding `term`, scanning back from `from`. */
  firstIndexOfTerm(term: number, from: number): number {
    let i = from;
    while (i - 1 > this._snapshotIndex && this.term(i - 1) === term) i--;
    return i;
  }

  /** Last index holding `term`, or 0 if no live entry has it. */
  lastIndexOfTerm(term: number): number {
    for (let i = this.entries.length - 1; i >= 0; i--) {
      const t = this.entries[i]!.term;
      if (t === term) return this._snapshotIndex + i + 1;
      if (t < term) break;
    }
    return 0;
  }

  /**
   * Election restriction (§5.4.1): would a candidate whose log ends at
   * (lastIndex, lastTerm) be at least as up-to-date as this log?
   */
  isUpToDate(lastIndex: number, lastTerm: number): boolean {
    if (lastTerm !== this.lastTerm) return lastTerm > this.lastTerm;
    return lastIndex >= this.lastIndex;
  }
}
