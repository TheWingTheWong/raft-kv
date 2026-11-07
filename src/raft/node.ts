import type { Clock, Timer } from './clock.js';
import { RaftLog } from './log.js';
import type { StateMachine } from './state-machine.js';
import type { Storage } from './storage.js';
import {
  DEFAULT_CONFIG,
  type AppendEntries,
  type AppendEntriesReply,
  type InstallSnapshot,
  type InstallSnapshotReply,
  type LogEntry,
  type NodeId,
  type RaftConfig,
  type RaftMessage,
  type RequestVote,
  type RequestVoteReply,
  type Role,
  type Snapshot,
} from './types.js';

export interface Transport<C = unknown> {
  /** Fire-and-forget. Delivery, ordering and uniqueness are not assumed. */
  send(to: NodeId, message: RaftMessage<C>): void;
}

/** Hooks for tests, invariant checkers and logging. Called synchronously. */
export interface RaftObserver<C = unknown> {
  onRoleChange?(node: RaftNode<C>, role: Role, term: number): void;
  onCommit?(node: RaftNode<C>, index: number, term: number): void;
  onApply?(node: RaftNode<C>, entry: LogEntry<C>, result: unknown): void;
}

export type RaftError = 'not_leader' | 'dropped';

export type ProposeResult =
  | { ok: true; index: number; result: unknown }
  | { ok: false; error: RaftError; leaderHint: NodeId | null };

export interface RaftNodeOptions<C> {
  id: NodeId;
  /** Every member of the cluster, including this node. */
  members: NodeId[];
  storage: Storage<C>;
  stateMachine: StateMachine<C>;
  clock: Clock;
  transport: Transport<C>;
  /** Uniform [0,1) source for election jitter. Defaults to Math.random. */
  random?: () => number;
  config?: Partial<RaftConfig>;
  observer?: RaftObserver<C>;
}

interface PeerProgress {
  nextIndex: number;
  matchIndex: number;
  /** Highest heartbeat round this peer has acknowledged in our term. */
  ackedSeq: number;
  /** Latest leader-clock send time of a message this peer acknowledged. */
  ackedSentAt: number;
  /** Local receive time of the latest acknowledgement (check-quorum). */
  lastAckAt: number;
}

interface PendingProposal {
  term: number;
  callback: (result: ProposeResult) => void;
}

/**
 * A single Raft server, written as an event-driven state machine.
 *
 * The node owns no threads and no I/O: messages arrive through `receive`,
 * leave through the Transport, time comes from the Clock and durable state
 * goes to Storage. That keeps the protocol logic identical between the
 * deterministic simulator and the HTTP deployment.
 */
export class RaftNode<C = unknown> {
  readonly id: NodeId;
  readonly members: readonly NodeId[];
  readonly peers: readonly NodeId[];
  readonly config: RaftConfig;

  private readonly storage: Storage<C>;
  private readonly sm: StateMachine<C>;
  private readonly clock: Clock;
  private readonly transport: Transport<C>;
  private readonly random: () => number;
  private readonly observer: RaftObserver<C> | undefined;

  // Persistent state (mirrored in storage).
  private currentTerm = 0;
  private votedFor: NodeId | null = null;
  private readonly log: RaftLog<C>;
  private snapshot: Snapshot | null = null;

  // Volatile state.
  private _role: Role = 'follower';
  private _leaderId: NodeId | null = null;
  private commitIndex = 0;
  private lastApplied = 0;
  private started = false;
  private stopped = false;

  private electionTimer: Timer | null = null;
  private electionTimerAt = Infinity;
  private electionDeadline = 0;
  private heartbeatTimer: Timer | null = null;
  private flushTimer: Timer | null = null;

  /**
   * Until this time the node refuses to grant votes or adopt a higher term
   * from a RequestVote, because it has recently heard from a live leader
   * (Raft dissertation §4.2.3).
   */
  private voteGuardUntil = 0;

  // Candidate state.
  private votes = new Set<NodeId>();

  // Leader state.
  private progress = new Map<NodeId, PeerProgress>();
  private seq = 0;

  private readonly proposals = new Map<number, PendingProposal>();

  constructor(options: RaftNodeOptions<C>) {
    if (!options.members.includes(options.id)) {
      throw new Error(`members must include the node itself (${options.id})`);
    }
    this.id = options.id;
    this.members = [...options.members];
    this.peers = this.members.filter((m) => m !== options.id);
    this.config = { ...DEFAULT_CONFIG, ...options.config };
    if (this.config.heartbeatInterval >= this.config.electionTimeoutMin) {
      throw new Error('heartbeatInterval must be smaller than electionTimeoutMin');
    }
    if (this.config.electionTimeoutMax <= this.config.electionTimeoutMin) {
      throw new Error('electionTimeoutMax must exceed electionTimeoutMin');
    }
    this.storage = options.storage;
    this.sm = options.stateMachine;
    this.clock = options.clock;
    this.transport = options.transport;
    this.random = options.random ?? Math.random;
    this.observer = options.observer;
    this.log = new RaftLog<C>();
  }

  // -------------------------------------------------------------------------
  // Introspection
  // -------------------------------------------------------------------------

  get role(): Role {
    return this._role;
  }
  get term(): number {
    return this.currentTerm;
  }
  get leaderId(): NodeId | null {
    return this._leaderId;
  }
  get commit(): number {
    return this.commitIndex;
  }
  get applied(): number {
    return this.lastApplied;
  }
  get lastLogIndex(): number {
    return this.log.lastIndex;
  }
  get snapshotIndex(): number {
    return this.log.snapshotIndex;
  }
  get isStopped(): boolean {
    return this.stopped;
  }

  /** Term of the entry at `index`, if it is still held (or is the snapshot boundary). */
  termAt(index: number): number | undefined {
    return this.log.term(index);
  }

  /** Live log entries in [from, toExclusive). */
  entries(from = this.log.firstIndex, toExclusive = this.log.lastIndex + 1): LogEntry<C>[] {
    return this.log.slice(Math.max(from, this.log.firstIndex), toExclusive);
  }

  status() {
    return {
      id: this.id,
      role: this._role,
      term: this.currentTerm,
      leaderId: this._leaderId,
      commitIndex: this.commitIndex,
      lastApplied: this.lastApplied,
      lastLogIndex: this.log.lastIndex,
      snapshotIndex: this.log.snapshotIndex,
    };
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  /** Recover from storage and start the election timer. */
  start(): void {
    if (this.started) throw new Error('RaftNode already started');
    this.started = true;
    const state = this.storage.load();
    this.currentTerm = state.hardState.term;
    this.votedFor = state.hardState.votedFor;
    if (state.snapshot) {
      this.snapshot = state.snapshot;
      this.log.resetToSnapshot(state.snapshot.lastIncludedIndex, state.snapshot.lastIncludedTerm);
      this.sm.restore(structuredClone(state.snapshot.data));
      this.commitIndex = this.lastApplied = state.snapshot.lastIncludedIndex;
    }
    for (const e of state.entries) this.log.append(e);

    // A freshly (re)started node may have acknowledged a leader just before
    // crashing. Treat startup as leader contact so it cannot help elect a
    // new leader while that leader's read lease may still be running.
    this.voteGuardUntil = this.clock.now() + this.config.electionTimeoutMin;
    this.resetElectionTimer();
  }

  /** Simulate a crash or shut down: drop all volatile state and timers. */
  stop(): void {
    if (this.stopped) return;
    this.stopped = true;
    this.electionTimer?.cancel();
    this.heartbeatTimer?.cancel();
    this.flushTimer?.cancel();
    this.electionTimer = this.heartbeatTimer = this.flushTimer = null;
    for (const [index, p] of this.proposals) {
      this.proposals.delete(index);
      p.callback({ ok: false, error: 'dropped', leaderHint: null });
    }
  }

  // -------------------------------------------------------------------------
  // Client API
  // -------------------------------------------------------------------------

  /**
   * Append a command to the log if this node is leader. The callback fires
   * once the entry is applied, or with an error if the entry is lost.
   */
  propose(command: C, callback: (result: ProposeResult) => void): void {
    if (this.stopped || this._role !== 'leader') {
      callback({ ok: false, error: 'not_leader', leaderHint: this._leaderId });
      return;
    }
    const entry = this.appendAsLeader({ type: 'command', command });
    this.proposals.set(entry.index, { term: entry.term, callback });
    this.maybeAdvanceCommit();
    this.scheduleFlush();
  }

  // -------------------------------------------------------------------------
  // Message dispatch
  // -------------------------------------------------------------------------

  receive(from: NodeId, msg: RaftMessage<C>): void {
    if (!this.started || this.stopped) return;

    if (msg.term > this.currentTerm) {
      if (msg.type === 'RequestVote' && this.voteGuardActive()) {
        // Ignore disruptive candidates while a leader is known to be alive.
        this.send(from, { type: 'RequestVoteReply', term: this.currentTerm, voteGranted: false });
        return;
      }
      const leader = msg.type === 'AppendEntries' || msg.type === 'InstallSnapshot' ? from : null;
      this.becomeFollower(msg.term, leader);
    }

    switch (msg.type) {
      case 'RequestVote':
        return this.onRequestVote(from, msg);
      case 'RequestVoteReply':
        return this.onRequestVoteReply(from, msg);
      case 'AppendEntries':
        return this.onAppendEntries(from, msg);
      case 'AppendEntriesReply':
        return this.onAppendEntriesReply(from, msg);
      case 'InstallSnapshot':
        return this.onInstallSnapshot(from, msg);
      case 'InstallSnapshotReply':
        return this.onInstallSnapshotReply(from, msg);
      default:
        return;
    }
  }

  // -------------------------------------------------------------------------
  // Elections
  // -------------------------------------------------------------------------

  private voteGuardActive(): boolean {
    return this._role === 'leader' || this.clock.now() < this.voteGuardUntil;
  }

  private onRequestVote(from: NodeId, m: RequestVote): void {
    let granted = false;
    if (m.term === this.currentTerm) {
      const canVote = this.votedFor === null || this.votedFor === m.candidateId;
      if (canVote && this.log.isUpToDate(m.lastLogIndex, m.lastLogTerm)) {
        granted = true;
        if (this.votedFor !== m.candidateId) {
          this.votedFor = m.candidateId;
          this.persistHardState();
        }
        this.resetElectionTimer();
      }
    }
    this.send(from, { type: 'RequestVoteReply', term: this.currentTerm, voteGranted: granted });
  }

  private onRequestVoteReply(from: NodeId, m: RequestVoteReply): void {
    if (this._role !== 'candidate' || m.term !== this.currentTerm || !m.voteGranted) return;
    this.votes.add(from);
    if (this.votes.size >= this.quorum()) this.becomeLeader();
  }

  private becomeCandidate(): void {
    this.currentTerm += 1;
    this.votedFor = this.id;
    this.persistHardState();
    this.setRole('candidate');
    this._leaderId = null;
    this.votes = new Set([this.id]);
    this.resetElectionTimer();
    if (this.votes.size >= this.quorum()) {
      this.becomeLeader();
      return;
    }
    const req: RequestVote = {
      type: 'RequestVote',
      term: this.currentTerm,
      candidateId: this.id,
      lastLogIndex: this.log.lastIndex,
      lastLogTerm: this.log.lastTerm,
    };
    for (const p of this.peers) this.send(p, req);
  }

  private becomeLeader(): void {
    this.setRole('leader');
    this._leaderId = this.id;
    const now = this.clock.now();
    this.progress.clear();
    for (const p of this.peers) {
      this.progress.set(p, {
        nextIndex: this.log.lastIndex + 1,
        matchIndex: 0,
        ackedSeq: 0,
        ackedSentAt: -Infinity,
        lastAckAt: now,
      });
    }
    this.electionTimer?.cancel();
    this.electionTimer = null;
    this.electionTimerAt = Infinity;
    // A no-op in the new term lets the leader commit everything before it
    // (only current-term entries are committed by counting replicas).
    this.appendAsLeader({ type: 'noop' });
    this.maybeAdvanceCommit();
    this.heartbeat();
  }

  private becomeFollower(term: number, leaderId: NodeId | null): void {
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.votedFor = null;
      this.persistHardState();
    }
    const wasLeader = this._role === 'leader';
    this.setRole('follower');
    this._leaderId = leaderId;
    if (wasLeader) {
      this.heartbeatTimer?.cancel();
      this.heartbeatTimer = null;
      this.flushTimer?.cancel();
      this.flushTimer = null;
    }
    this.resetElectionTimer();
  }

  private setRole(role: Role): void {
    const changed = role !== this._role;
    this._role = role;
    if (changed || role === 'candidate') this.observer?.onRoleChange?.(this, role, this.currentTerm);
  }

  private resetElectionTimer(): void {
    const { electionTimeoutMin: lo, electionTimeoutMax: hi } = this.config;
    this.electionDeadline = this.clock.now() + lo + this.random() * (hi - lo);
    this.armElectionTimer();
  }

  /**
   * Election timeouts are reset on every heartbeat. Rather than cancel and
   * reschedule each time, keep one timer and push the deadline forward; the
   * timer re-arms itself if it fires early.
   */
  private armElectionTimer(): void {
    if (this.electionTimer && this.electionTimerAt <= this.electionDeadline) return;
    this.electionTimer?.cancel();
    this.electionTimerAt = this.electionDeadline;
    this.electionTimer = this.clock.setTimer(this.electionDeadline - this.clock.now(), () => {
      this.electionTimer = null;
      this.electionTimerAt = Infinity;
      if (this.stopped || this._role === 'leader') return;
      if (this.clock.now() < this.electionDeadline) {
        this.armElectionTimer();
      } else {
        this.becomeCandidate();
      }
    });
  }

  // -------------------------------------------------------------------------
  // Replication (leader side)
  // -------------------------------------------------------------------------

  private appendAsLeader(payload: LogEntry<C>['payload']): LogEntry<C> {
    const entry: LogEntry<C> = { index: this.log.lastIndex + 1, term: this.currentTerm, payload };
    this.log.append(entry);
    this.storage.append([entry]);
    return entry;
  }

  private heartbeat(): void {
    if (this._role !== 'leader' || this.stopped) return;
    this.heartbeatTimer?.cancel();
    this.seq += 1;
    for (const p of this.peers) this.sendAppend(p);
    this.heartbeatTimer = this.clock.setTimer(this.config.heartbeatInterval, () => {
      this.heartbeatTimer = null;
      if (this.config.checkQuorum && !this.hasRecentQuorum()) {
        this.becomeFollower(this.currentTerm, null);
        return;
      }
      this.heartbeat();
    });
  }

  /** Coalesce bursts of proposals into one replication round. */
  private scheduleFlush(): void {
    if (this.flushTimer || this.peers.length === 0) return;
    this.flushTimer = this.clock.setTimer(0, () => {
      this.flushTimer = null;
      if (this._role !== 'leader' || this.stopped) return;
      for (const p of this.peers) this.sendAppend(p);
    });
  }

  private sendAppend(peer: NodeId): void {
    const pr = this.progress.get(peer)!;
    const prevLogIndex = pr.nextIndex - 1;
    if (prevLogIndex < this.log.snapshotIndex) {
      // The entries this peer needs have been compacted away.
      const msg: InstallSnapshot = {
        type: 'InstallSnapshot',
        term: this.currentTerm,
        leaderId: this.id,
        snapshot: this.snapshot!,
        seq: this.seq,
        sentAt: this.clock.now(),
      };
      this.send(peer, msg);
      return;
    }
    const last = Math.min(this.log.lastIndex, prevLogIndex + this.config.maxEntriesPerMessage);
    const msg: AppendEntries<C> = {
      type: 'AppendEntries',
      term: this.currentTerm,
      leaderId: this.id,
      prevLogIndex,
      prevLogTerm: this.log.term(prevLogIndex)!,
      entries: this.log.slice(pr.nextIndex, last + 1),
      leaderCommit: this.commitIndex,
      seq: this.seq,
      sentAt: this.clock.now(),
    };
    this.send(peer, msg);
  }

  private onAppendEntriesReply(from: NodeId, m: AppendEntriesReply): void {
    if (this._role !== 'leader' || m.term !== this.currentTerm) return;
    const pr = this.progress.get(from);
    if (!pr) return;
    this.recordAck(pr, m.seq, m.sentAt);

    if (m.success) {
      if (m.matchIndex > pr.matchIndex) {
        pr.matchIndex = m.matchIndex;
        pr.nextIndex = Math.max(pr.nextIndex, m.matchIndex + 1);
        this.maybeAdvanceCommit();
        if (pr.nextIndex <= this.log.lastIndex) this.sendAppend(from);
      }
      return;
    }

    // Fast backtracking (§5.3): skip the follower's whole conflicting term.
    let next = m.conflictIndex;
    if (m.conflictTerm > 0) {
      const last = this.log.lastIndexOfTerm(m.conflictTerm);
      if (last > 0) next = last + 1;
    }
    next = Math.max(pr.matchIndex + 1, Math.min(next, this.log.lastIndex + 1));
    if (next !== pr.nextIndex) {
      pr.nextIndex = next;
      this.sendAppend(from);
    }
  }

  private onInstallSnapshotReply(from: NodeId, m: InstallSnapshotReply): void {
    if (this._role !== 'leader' || m.term !== this.currentTerm) return;
    const pr = this.progress.get(from);
    if (!pr) return;
    this.recordAck(pr, m.seq, m.sentAt);
    if (m.matchIndex > pr.matchIndex) {
      pr.matchIndex = m.matchIndex;
      pr.nextIndex = Math.max(pr.nextIndex, m.matchIndex + 1);
      this.maybeAdvanceCommit();
      if (pr.nextIndex <= this.log.lastIndex) this.sendAppend(from);
    }
  }

  private recordAck(pr: PeerProgress, seq: number, sentAt: number): void {
    pr.ackedSeq = Math.max(pr.ackedSeq, seq);
    pr.ackedSentAt = Math.max(pr.ackedSentAt, sentAt);
    pr.lastAckAt = this.clock.now();
  }

  /**
   * Commit rule (§5.3, §5.4.2): advance commitIndex to the highest index
   * stored on a majority, but only if that entry is from the current term.
   * Older entries become committed indirectly through it.
   */
  private maybeAdvanceCommit(): void {
    if (this._role !== 'leader') return;
    const matches = [this.log.lastIndex];
    for (const pr of this.progress.values()) matches.push(pr.matchIndex);
    matches.sort((a, b) => b - a);
    const n = matches[this.quorum() - 1]!;
    if (n > this.commitIndex && this.log.term(n) === this.currentTerm) {
      this.setCommitIndex(n);
    }
  }

  private hasRecentQuorum(): boolean {
    const now = this.clock.now();
    const times = [now];
    for (const pr of this.progress.values()) times.push(pr.lastAckAt);
    times.sort((a, b) => b - a);
    return now - times[this.quorum() - 1]! <= this.config.electionTimeoutMax;
  }

  // -------------------------------------------------------------------------
  // Replication (follower side)
  // -------------------------------------------------------------------------

  /** Shared handling for any valid message from the current term's leader. */
  private acceptLeader(from: NodeId): void {
    if (this._role === 'leader') {
      throw new Error(`election safety violated: two leaders in term ${this.currentTerm}`);
    }
    if (this._role === 'candidate') this.becomeFollower(this.currentTerm, from);
    this._leaderId = from;
    this.voteGuardUntil = this.clock.now() + this.config.electionTimeoutMin;
    this.resetElectionTimer();
  }

  private onAppendEntries(from: NodeId, m: AppendEntries<C>): void {
    const reply = (fields: Partial<AppendEntriesReply>): void =>
      this.send(from, {
        type: 'AppendEntriesReply',
        term: this.currentTerm,
        success: false,
        matchIndex: 0,
        conflictIndex: 0,
        conflictTerm: 0,
        seq: m.seq,
        sentAt: m.sentAt,
        ...fields,
      });

    if (m.term < this.currentTerm) return reply({});
    this.acceptLeader(from);

    let prevIndex = m.prevLogIndex;
    let entries = m.entries;
    const lastNew = m.prevLogIndex + m.entries.length;

    if (prevIndex < this.log.snapshotIndex) {
      // Everything up to our snapshot is committed and therefore matches.
      const skip = this.log.snapshotIndex - prevIndex;
      entries = entries.slice(skip);
      prevIndex = this.log.snapshotIndex;
    } else {
      const localTerm = this.log.term(prevIndex);
      if (localTerm === undefined) {
        return reply({ conflictIndex: this.log.lastIndex + 1, conflictTerm: 0 });
      }
      if (localTerm !== m.prevLogTerm) {
        return reply({
          conflictIndex: this.log.firstIndexOfTerm(localTerm, prevIndex),
          conflictTerm: localTerm,
        });
      }
    }

    // Append what we do not have; truncate only on a real conflict so that a
    // stale, shorter AppendEntries can never erase entries we already acked.
    for (let i = 0; i < entries.length; i++) {
      const e = entries[i]!;
      const localTerm = this.log.term(e.index);
      if (localTerm === e.term) continue;
      if (localTerm !== undefined) {
        if (e.index <= this.commitIndex) {
          throw new Error(`refusing to truncate committed entry ${e.index}`);
        }
        this.truncateLog(e.index);
      }
      const rest = entries.slice(i);
      for (const r of rest) this.log.append(r);
      this.storage.append(rest);
      break;
    }

    if (m.leaderCommit > this.commitIndex) {
      this.setCommitIndex(Math.min(m.leaderCommit, lastNew));
    }
    reply({ success: true, matchIndex: lastNew });
  }

  private onInstallSnapshot(from: NodeId, m: InstallSnapshot): void {
    const reply = (matchIndex: number): void =>
      this.send(from, {
        type: 'InstallSnapshotReply',
        term: this.currentTerm,
        matchIndex,
        seq: m.seq,
        sentAt: m.sentAt,
      });

    if (m.term < this.currentTerm) return reply(0);
    this.acceptLeader(from);

    const snap = m.snapshot;
    const index = snap.lastIncludedIndex;
    if (index <= this.commitIndex) {
      // We already hold this prefix (committed entries match the leader's).
      return reply(index);
    }

    if (this.log.term(index) === snap.lastIncludedTerm) {
      // Our log extends past the snapshot consistently: keep the suffix.
      this.log.compactThrough(index, snap.lastIncludedTerm);
    } else {
      this.log.resetToSnapshot(index, snap.lastIncludedTerm);
    }
    this.storage.saveSnapshot(snap, this.log.slice(index + 1));
    this.snapshot = snap;
    this.sm.restore(structuredClone(snap.data));
    this.commitIndex = this.lastApplied = index;
    // Proposals whose index is now inside the snapshot (or whose entries were
    // discarded with the old log) can no longer be matched to a result.
    for (const [i, p] of this.proposals) {
      if (i <= index || this.log.term(i) !== p.term) {
        this.proposals.delete(i);
        p.callback({ ok: false, error: 'dropped', leaderHint: this._leaderId });
      }
    }
    reply(index);
  }

  private truncateLog(from: number): void {
    this.log.truncateFrom(from);
    this.storage.truncateFrom(from);
    for (const [index, p] of this.proposals) {
      if (index >= from) {
        this.proposals.delete(index);
        p.callback({ ok: false, error: 'dropped', leaderHint: this._leaderId });
      }
    }
  }

  // -------------------------------------------------------------------------
  // Commit and apply
  // -------------------------------------------------------------------------

  private setCommitIndex(index: number): void {
    if (index <= this.commitIndex) return;
    const from = this.commitIndex + 1;
    this.commitIndex = index;
    if (this.observer?.onCommit) {
      for (let i = Math.max(from, this.log.firstIndex); i <= index; i++) {
        this.observer.onCommit(this, i, this.log.term(i)!);
      }
    }
    this.applyCommitted();
  }

  private applyCommitted(): void {
    while (this.lastApplied < this.commitIndex) {
      const index = this.lastApplied + 1;
      const entry = this.log.entry(index);
      const result =
        entry.payload.type === 'command' ? this.sm.apply(entry.payload.command, index) : undefined;
      this.lastApplied = index;
      this.observer?.onApply?.(this, entry, result);
      const pending = this.proposals.get(index);
      if (pending) {
        this.proposals.delete(index);
        pending.callback(
          pending.term === entry.term
            ? { ok: true, index, result }
            : { ok: false, error: 'dropped', leaderHint: this._leaderId },
        );
      }
    }
    this.maybeSnapshot();
  }

  /** Log compaction (§7): fold applied entries into a state machine snapshot. */
  private maybeSnapshot(): void {
    const threshold = this.config.snapshotThreshold;
    if (threshold <= 0 || this.lastApplied - this.log.snapshotIndex < threshold) return;
    const index = this.lastApplied;
    const snap: Snapshot = {
      lastIncludedIndex: index,
      lastIncludedTerm: this.log.term(index)!,
      data: this.sm.snapshot(),
    };
    this.log.compactThrough(index, snap.lastIncludedTerm);
    this.storage.saveSnapshot(snap, this.log.slice(index + 1));
    this.snapshot = snap;
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private quorum(): number {
    return Math.floor(this.members.length / 2) + 1;
  }

  private persistHardState(): void {
    this.storage.saveHardState({ term: this.currentTerm, votedFor: this.votedFor });
  }

  private send(to: NodeId, msg: RaftMessage<C>): void {
    this.transport.send(to, msg);
  }
}
