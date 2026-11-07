export type NodeId = string;

export type Role = 'follower' | 'candidate' | 'leader';

/** What a log entry carries. Leaders append a no-op at the start of each term. */
export type EntryPayload<C> = { type: 'noop' } | { type: 'command'; command: C };

export interface LogEntry<C = unknown> {
  index: number;
  term: number;
  payload: EntryPayload<C>;
}

/** State that must hit stable storage before a node answers any RPC. */
export interface HardState {
  term: number;
  votedFor: NodeId | null;
}

export interface Snapshot {
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  /** Opaque, JSON-serialisable state machine image. */
  data: unknown;
}

// ---------------------------------------------------------------------------
// RPCs. Requests and replies are one-way messages: the transport is allowed
// to drop, duplicate, delay and reorder them, so every reply carries enough
// context to be applied idempotently.
// ---------------------------------------------------------------------------

export interface RequestVote {
  type: 'RequestVote';
  term: number;
  candidateId: NodeId;
  lastLogIndex: number;
  lastLogTerm: number;
}

export interface RequestVoteReply {
  type: 'RequestVoteReply';
  term: number;
  voteGranted: boolean;
}

export interface AppendEntries<C = unknown> {
  type: 'AppendEntries';
  term: number;
  leaderId: NodeId;
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry<C>[];
  leaderCommit: number;
  /** Leader heartbeat round; echoed back to confirm leadership for read-index. */
  seq: number;
  /** Leader-local send time; echoed back to compute the read lease. */
  sentAt: number;
}

export interface AppendEntriesReply {
  type: 'AppendEntriesReply';
  term: number;
  success: boolean;
  /** On success: highest index known to match the leader's log. */
  matchIndex: number;
  /** On failure: where the leader should retry from (fast log backtracking). */
  conflictIndex: number;
  /** On failure: term of the conflicting entry, or 0 if the follower's log was too short. */
  conflictTerm: number;
  seq: number;
  sentAt: number;
}

export interface InstallSnapshot {
  type: 'InstallSnapshot';
  term: number;
  leaderId: NodeId;
  snapshot: Snapshot;
  seq: number;
  sentAt: number;
}

export interface InstallSnapshotReply {
  type: 'InstallSnapshotReply';
  term: number;
  matchIndex: number;
  seq: number;
  sentAt: number;
}

export type RaftMessage<C = unknown> =
  | RequestVote
  | RequestVoteReply
  | AppendEntries<C>
  | AppendEntriesReply
  | InstallSnapshot
  | InstallSnapshotReply;

export interface RaftConfig {
  /** Lower bound of the randomized election timeout (ms). */
  electionTimeoutMin: number;
  /** Upper bound (exclusive) of the randomized election timeout (ms). */
  electionTimeoutMax: number;
  /** Leader heartbeat period (ms). Must be well below electionTimeoutMin. */
  heartbeatInterval: number;
  /** Max entries per AppendEntries message. */
  maxEntriesPerMessage: number;
  /** Take a snapshot once this many applied entries sit above the last one. 0 disables. */
  snapshotThreshold: number;
  /**
   * Bound on relative clock rate drift between nodes, used to shrink the read
   * lease. 0 is only appropriate for the simulator's perfect clocks.
   */
  clockDriftBound: number;
  /** Leader steps down if it cannot reach a quorum for electionTimeoutMax. */
  checkQuorum: boolean;
}

export const DEFAULT_CONFIG: RaftConfig = {
  electionTimeoutMin: 150,
  electionTimeoutMax: 300,
  heartbeatInterval: 50,
  maxEntriesPerMessage: 64,
  snapshotThreshold: 1000,
  clockDriftBound: 0.1,
  checkQuorum: true,
};
