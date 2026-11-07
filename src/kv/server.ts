import type { RaftNode } from '../raft/node.js';
import type { NodeId } from '../raft/types.js';
import type { KvStateMachine } from './state-machine.js';
import type { KvCommand, KvOp, KvResult, ReadMode } from './types.js';

export interface KvRequest {
  clientId: string;
  seq: number;
  op: KvOp;
  /** Only consulted for `get`. Defaults to read_index. */
  readMode?: ReadMode;
}

export type KvResponse =
  | { ok: true; result: KvResult }
  | { ok: false; error: 'not_leader' | 'dropped'; leaderHint: NodeId | null };

/**
 * Server-side glue between client requests and a Raft node: writes become
 * log proposals, reads take the requested read path. Callback-based so it
 * runs unchanged inside the synchronous simulator.
 */
export class KvServer {
  constructor(
    readonly raft: RaftNode<KvCommand>,
    private readonly sm: KvStateMachine,
  ) {}

  handle(req: KvRequest, respond: (res: KvResponse) => void): void {
    const { op } = req;
    if (op.type === 'get') {
      const mode = req.readMode ?? 'read_index';
      if (mode === 'local') {
        respond({ ok: true, result: this.sm.query(op) });
        return;
      }
      if (mode !== 'log') {
        this.raft.read(op, mode, (r) =>
          respond(r.ok ? { ok: true, result: r.result as KvResult } : r),
        );
        return;
      }
    }
    this.raft.propose({ clientId: req.clientId, seq: req.seq, op }, (r) => {
      if (!r.ok) return respond(r);
      if (r.result === null) return respond({ ok: false, error: 'dropped', leaderHint: null });
      respond({ ok: true, result: r.result as KvResult });
    });
  }

  handleAsync(req: KvRequest): Promise<KvResponse> {
    return new Promise((resolve) => this.handle(req, resolve));
  }
}
