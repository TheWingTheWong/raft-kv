import { appendFileSync, mkdtempSync, readdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import { FileStorage, MemoryStorage, type Storage } from '../src/raft/storage.js';
import type { LogEntry } from '../src/raft/types.js';

const e = (index: number, term: number): LogEntry<string> => ({
  index,
  term,
  payload: { type: 'command', command: `v${index}` },
});

const dirs: string[] = [];
function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'raft-kv-storage-'));
  dirs.push(dir);
  return dir;
}
afterEach(() => {
  while (dirs.length) rmSync(dirs.pop()!, { recursive: true, force: true });
});

type Factory = () => { open: () => Storage<string> };

const backends: Array<[string, Factory]> = [
  [
    'MemoryStorage',
    () => {
      const s = new MemoryStorage<string>();
      return { open: () => s };
    },
  ],
  [
    'FileStorage',
    () => {
      const dir = tempDir();
      return { open: () => new FileStorage<string>(dir, { fsync: false }) };
    },
  ],
];

describe.each(backends)('%s', (_name, factory) => {
  it('starts empty', () => {
    const s = factory().open();
    expect(s.load()).toEqual({ hardState: { term: 0, votedFor: null }, snapshot: null, entries: [] });
  });

  it('round-trips hard state and entries across reopen', () => {
    const f = factory();
    const s = f.open();
    s.load();
    s.saveHardState({ term: 3, votedFor: 'n2' });
    s.append([e(1, 1), e(2, 1)]);
    s.append([e(3, 3)]);
    const loaded = f.open().load();
    expect(loaded.hardState).toEqual({ term: 3, votedFor: 'n2' });
    expect(loaded.entries.map((x) => [x.index, x.term])).toEqual([
      [1, 1],
      [2, 1],
      [3, 3],
    ]);
  });

  it('truncates a suffix durably', () => {
    const f = factory();
    const s = f.open();
    s.load();
    s.append([e(1, 1), e(2, 1), e(3, 2)]);
    s.truncateFrom(2);
    s.append([e(2, 4)]);
    expect(f.open().load().entries.map((x) => [x.index, x.term])).toEqual([
      [1, 1],
      [2, 4],
    ]);
  });

  it('replaces snapshot and log suffix together', () => {
    const f = factory();
    const s = f.open();
    s.load();
    s.append([e(1, 1), e(2, 1), e(3, 2), e(4, 2)]);
    s.saveSnapshot({ lastIncludedIndex: 2, lastIncludedTerm: 1, data: { x: '1' } }, [e(3, 2), e(4, 2)]);
    s.append([e(5, 2)]);
    const loaded = f.open().load();
    expect(loaded.snapshot).toEqual({ lastIncludedIndex: 2, lastIncludedTerm: 1, data: { x: '1' } });
    expect(loaded.entries.map((x) => x.index)).toEqual([3, 4, 5]);
  });

  it('rejects out-of-order appends', () => {
    const s = factory().open();
    s.load();
    s.append([e(1, 1)]);
    expect(() => s.append([e(3, 1)])).toThrow(/out of order/);
  });
});

describe('FileStorage crash behaviour', () => {
  it('drops a torn final log line', () => {
    const dir = tempDir();
    const s = new FileStorage<string>(dir, { fsync: false });
    s.load();
    s.append([e(1, 1), e(2, 1)]);
    appendFileSync(join(dir, 'log.0.jsonl'), '{"index":3,"term":1,"payl');
    const reopened = new FileStorage<string>(dir, { fsync: false });
    expect(reopened.load().entries.map((x) => x.index)).toEqual([1, 2]);
    reopened.append([e(3, 1)]);
    expect(new FileStorage<string>(dir).load().entries.map((x) => x.index)).toEqual([1, 2, 3]);
  });

  it('keeps only the live log generation after a snapshot', () => {
    const dir = tempDir();
    const s = new FileStorage<string>(dir, { fsync: false });
    s.load();
    s.append([e(1, 1), e(2, 1)]);
    s.saveSnapshot({ lastIncludedIndex: 2, lastIncludedTerm: 1, data: null }, []);
    expect(readdirSync(dir).sort()).toEqual(['log.1.jsonl', 'manifest.json']);
  });
});
