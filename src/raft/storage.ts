import {
  closeSync,
  existsSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  writeSync,
} from 'node:fs';
import { join } from 'node:path';
import type { HardState, LogEntry, Snapshot } from './types.js';

export interface PersistentState<C> {
  hardState: HardState;
  snapshot: Snapshot | null;
  /** Entries strictly after the snapshot, in index order. */
  entries: LogEntry<C>[];
}

/**
 * Durable state for one Raft node. All methods are synchronous and must not
 * return until the data is durable: the node answers RPCs right after calling
 * them, and answering before persisting is how Raft implementations lose
 * acknowledged writes.
 */
export interface Storage<C = unknown> {
  load(): PersistentState<C>;
  saveHardState(state: HardState): void;
  /** Append entries contiguous with the current last index. */
  append(entries: LogEntry<C>[]): void;
  /** Remove the entry at `index` and everything after it. */
  truncateFrom(index: number): void;
  /**
   * Atomically replace the snapshot and the log suffix that follows it.
   * Either the old (snapshot, log) pair or the new one survives a crash.
   */
  saveSnapshot(snapshot: Snapshot, remaining: LogEntry<C>[]): void;
}

const EMPTY_HARD_STATE: HardState = { term: 0, votedFor: null };

/**
 * Storage that lives in process memory. In the simulator it outlives the
 * RaftNode that uses it, which is how a "crash" keeps durable state while
 * losing everything volatile. Values are cloned at the boundary so a node
 * can never mutate what it persisted by accident.
 */
export class MemoryStorage<C = unknown> implements Storage<C> {
  private hardState: HardState = { ...EMPTY_HARD_STATE };
  private snapshot: Snapshot | null = null;
  private entries: LogEntry<C>[] = [];

  load(): PersistentState<C> {
    return structuredClone({
      hardState: this.hardState,
      snapshot: this.snapshot,
      entries: this.entries,
    });
  }

  saveHardState(state: HardState): void {
    this.hardState = { ...state };
  }

  append(entries: LogEntry<C>[]): void {
    for (const e of entries) {
      const expected = this.lastIndex() + 1;
      if (e.index !== expected) {
        throw new Error(`storage append out of order: got ${e.index}, expected ${expected}`);
      }
      this.entries.push(structuredClone(e));
    }
  }

  truncateFrom(index: number): void {
    this.entries = this.entries.filter((e) => e.index < index);
  }

  saveSnapshot(snapshot: Snapshot, remaining: LogEntry<C>[]): void {
    this.snapshot = structuredClone(snapshot);
    this.entries = structuredClone(remaining);
  }

  private lastIndex(): number {
    const last = this.entries[this.entries.length - 1];
    return last ? last.index : (this.snapshot?.lastIncludedIndex ?? 0);
  }
}

export interface FileStorageOptions {
  /** fsync files and directories. Disable only for throwaway test data. */
  fsync?: boolean;
}

interface Manifest {
  generation: number;
  snapshot: Snapshot | null;
}

/**
 * File-backed storage.
 *
 * Layout inside `dir`:
 *   state.json            current term and vote (atomic replace)
 *   manifest.json         snapshot + the generation of the live log file
 *   log.<gen>.jsonl       one JSON entry per line, appended and fsynced
 *
 * A snapshot install writes the new log generation first, then swaps the
 * manifest with a rename, so a crash leaves either the old or the new pair.
 * A torn final line in the log (crash mid-append) is discarded on load; the
 * entry it held was never acknowledged.
 */
export class FileStorage<C = unknown> implements Storage<C> {
  private readonly fsync: boolean;
  private manifest: Manifest = { generation: 0, snapshot: null };
  private lastIndex = 0;
  private entries: LogEntry<C>[] = [];

  constructor(
    private readonly dir: string,
    options: FileStorageOptions = {},
  ) {
    this.fsync = options.fsync ?? true;
    mkdirSync(dir, { recursive: true });
  }

  load(): PersistentState<C> {
    const statePath = join(this.dir, 'state.json');
    const hardState: HardState = existsSync(statePath)
      ? (JSON.parse(readFileSync(statePath, 'utf8')) as HardState)
      : { ...EMPTY_HARD_STATE };

    const manifestPath = join(this.dir, 'manifest.json');
    if (existsSync(manifestPath)) {
      this.manifest = JSON.parse(readFileSync(manifestPath, 'utf8')) as Manifest;
    }
    const snapshotIndex = this.manifest.snapshot?.lastIncludedIndex ?? 0;

    this.entries = [];
    const logPath = this.logPath(this.manifest.generation);
    if (existsSync(logPath)) {
      const lines = readFileSync(logPath, 'utf8').split('\n');
      for (const line of lines) {
        if (line.length === 0) continue;
        let entry: LogEntry<C>;
        try {
          entry = JSON.parse(line) as LogEntry<C>;
        } catch {
          break; // torn write at the tail
        }
        if (entry.index <= snapshotIndex) continue;
        this.entries.push(entry);
      }
    }
    // Rewrite to drop any torn tail so later appends start on a clean line.
    this.writeLogFile(this.manifest.generation, this.entries);
    this.lastIndex = this.entries.length
      ? this.entries[this.entries.length - 1]!.index
      : snapshotIndex;

    return structuredClone({ hardState, snapshot: this.manifest.snapshot, entries: this.entries });
  }

  saveHardState(state: HardState): void {
    this.atomicWrite(join(this.dir, 'state.json'), JSON.stringify(state));
  }

  append(entries: LogEntry<C>[]): void {
    if (entries.length === 0) return;
    let chunk = '';
    for (const e of entries) {
      if (e.index !== this.lastIndex + 1) {
        throw new Error(`storage append out of order: got ${e.index}, expected ${this.lastIndex + 1}`);
      }
      this.lastIndex = e.index;
      this.entries.push(e);
      chunk += JSON.stringify(e) + '\n';
    }
    const fd = openSync(this.logPath(this.manifest.generation), 'a');
    try {
      writeSync(fd, chunk);
      if (this.fsync) fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
  }

  truncateFrom(index: number): void {
    this.entries = this.entries.filter((e) => e.index < index);
    this.lastIndex = this.entries.length
      ? this.entries[this.entries.length - 1]!.index
      : (this.manifest.snapshot?.lastIncludedIndex ?? 0);
    this.writeLogFile(this.manifest.generation, this.entries);
  }

  saveSnapshot(snapshot: Snapshot, remaining: LogEntry<C>[]): void {
    const oldGen = this.manifest.generation;
    const newGen = oldGen + 1;
    this.writeLogFile(newGen, remaining);
    this.manifest = { generation: newGen, snapshot };
    this.atomicWrite(join(this.dir, 'manifest.json'), JSON.stringify(this.manifest));
    rmSync(this.logPath(oldGen), { force: true });
    this.entries = remaining.slice();
    this.lastIndex = remaining.length
      ? remaining[remaining.length - 1]!.index
      : snapshot.lastIncludedIndex;
  }

  private logPath(generation: number): string {
    return join(this.dir, `log.${generation}.jsonl`);
  }

  private writeLogFile(generation: number, entries: LogEntry<C>[]): void {
    this.atomicWrite(
      this.logPath(generation),
      entries.map((e) => JSON.stringify(e) + '\n').join(''),
    );
  }

  private atomicWrite(path: string, contents: string): void {
    const tmp = `${path}.tmp`;
    const fd = openSync(tmp, 'w');
    try {
      writeSync(fd, contents);
      if (this.fsync) fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    renameSync(tmp, path);
    if (this.fsync) {
      const dirFd = openSync(this.dir, 'r');
      try {
        fsyncSync(dirFd);
      } finally {
        closeSync(dirFd);
      }
    }
  }
}
