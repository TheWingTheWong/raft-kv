import { describe, expect, it } from 'vitest';
import { RaftLog } from '../src/raft/log.js';
import type { LogEntry } from '../src/raft/types.js';

const e = (index: number, term: number): LogEntry<string> => ({
  index,
  term,
  payload: { type: 'command', command: `c${index}` },
});

function logOf(...terms: number[]): RaftLog<string> {
  const log = new RaftLog<string>();
  terms.forEach((t, i) => log.append(e(i + 1, t)));
  return log;
}

describe('RaftLog', () => {
  it('starts empty with the index-0 sentinel', () => {
    const log = new RaftLog();
    expect(log.lastIndex).toBe(0);
    expect(log.lastTerm).toBe(0);
    expect(log.term(0)).toBe(0);
    expect(log.term(1)).toBeUndefined();
  });

  it('appends contiguously and rejects gaps and term regressions', () => {
    const log = logOf(1, 1, 2);
    expect(log.lastIndex).toBe(3);
    expect(log.lastTerm).toBe(2);
    expect(() => log.append(e(5, 2))).toThrow(/non-contiguous/);
    expect(() => log.append(e(4, 1))).toThrow(/term regression/);
  });

  it('truncates a suffix', () => {
    const log = logOf(1, 1, 2, 2);
    log.truncateFrom(3);
    expect(log.lastIndex).toBe(2);
    expect(log.lastTerm).toBe(1);
  });

  it('compacts a prefix and keeps addressing by absolute index', () => {
    const log = logOf(1, 1, 2, 3, 3);
    log.compactThrough(3, 2);
    expect(log.snapshotIndex).toBe(3);
    expect(log.firstIndex).toBe(4);
    expect(log.lastIndex).toBe(5);
    expect(log.term(3)).toBe(2);
    expect(log.term(2)).toBeUndefined();
    expect(log.entry(4).term).toBe(3);
    expect(() => log.entry(3)).toThrow(RangeError);
    expect(log.slice(4).map((x) => x.index)).toEqual([4, 5]);
    expect(() => log.truncateFrom(3)).toThrow();
  });

  it('compacting past the end leaves an empty log at the snapshot', () => {
    const log = logOf(1, 1);
    log.compactThrough(7, 4);
    expect(log.lastIndex).toBe(7);
    expect(log.lastTerm).toBe(4);
    expect(log.length).toBe(0);
  });

  it('finds term boundaries for fast backtracking', () => {
    const log = logOf(1, 1, 2, 2, 2, 4);
    expect(log.firstIndexOfTerm(2, 5)).toBe(3);
    expect(log.lastIndexOfTerm(2)).toBe(5);
    expect(log.lastIndexOfTerm(3)).toBe(0);
  });

  it('implements the election restriction comparison', () => {
    const log = logOf(1, 2, 2);
    expect(log.isUpToDate(3, 2)).toBe(true); // identical
    expect(log.isUpToDate(2, 2)).toBe(false); // shorter, same term
    expect(log.isUpToDate(1, 3)).toBe(true); // higher last term wins
    expect(log.isUpToDate(10, 1)).toBe(false); // longer but older term loses
  });
});
