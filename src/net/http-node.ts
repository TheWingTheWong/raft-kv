import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import type { AddressInfo } from 'node:net';
import { KvServer, type KvResponse } from '../kv/server.js';
import { KvStateMachine } from '../kv/state-machine.js';
import type { KvCommand, KvOp, ReadMode } from '../kv/types.js';
import { RealClock } from '../raft/clock.js';
import { RaftNode, type RaftObserver } from '../raft/node.js';
import type { Storage } from '../raft/storage.js';
import type { NodeId, RaftConfig, RaftMessage } from '../raft/types.js';

export interface HttpNodeOptions {
  id: NodeId;
  host?: string;
  /** 0 picks a free port. */
  port: number;
  storage: Storage<KvCommand>;
  config?: Partial<RaftConfig>;
  /** Give up on a client request after this long and answer 503 (ms). */
  requestTimeoutMs?: number;
  observer?: RaftObserver<KvCommand>;
}

const READ_MODES: readonly ReadMode[] = ['log', 'read_index', 'lease', 'local'];

/**
 * One Raft server exposed over HTTP/JSON.
 *
 *   POST   /raft                 peer RPC: {from, msg}, fire-and-forget
 *   GET    /status               role, term, indices
 *   GET    /kv/:key?mode=lease   read (log | read_index | lease | local)
 *   PUT    /kv/:key              {value}
 *   DELETE /kv/:key
 *   POST   /kv/:key/cas          {expected, value}
 *
 * Writes may carry x-client-id / x-client-seq headers for exactly-once
 * retries. A follower answers 421 with the leader's URL.
 */
export class HttpNode {
  readonly id: NodeId;
  private readonly server: Server;
  private readonly options: HttpNodeOptions;
  private raft: RaftNode<KvCommand> | null = null;
  private kv: KvServer | null = null;
  private peers: Record<NodeId, string> = {};
  private address: string | null = null;

  constructor(options: HttpNodeOptions) {
    this.id = options.id;
    this.options = options;
    this.server = createServer((req, res) => {
      this.route(req, res).catch((err: unknown) => {
        if (!res.headersSent) json(res, 500, { error: 'internal', message: String(err) });
      });
    });
    this.server.keepAliveTimeout = 5_000;
  }

  get url(): string {
    if (!this.address) throw new Error('not listening');
    return this.address;
  }

  get node(): RaftNode<KvCommand> | null {
    return this.raft;
  }

  /** Bind the socket. Separate from start() so ports can be chosen first. */
  async listen(): Promise<string> {
    const host = this.options.host ?? '127.0.0.1';
    await new Promise<void>((resolve, reject) => {
      this.server.once('error', reject);
      this.server.listen(this.options.port, host, () => resolve());
    });
    const { port } = this.server.address() as AddressInfo;
    this.address = `http://${host}:${port}`;
    return this.address;
  }

  /** Start Raft with the full membership (id -> base URL, including this node). */
  start(peers: Record<NodeId, string>): void {
    if (this.raft) throw new Error('already started');
    this.peers = { ...peers };
    const sm = new KvStateMachine();
    const raft = new RaftNode<KvCommand>({
      id: this.id,
      members: Object.keys(peers),
      storage: this.options.storage,
      stateMachine: sm,
      clock: new RealClock(),
      transport: { send: (to, msg) => this.sendToPeer(to, msg) },
      config: this.options.config,
      observer: this.options.observer,
    });
    this.raft = raft;
    this.kv = new KvServer(raft, sm);
    raft.start();
  }

  async close(): Promise<void> {
    this.raft?.stop();
    this.server.closeAllConnections();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }

  private sendToPeer(to: NodeId, msg: RaftMessage<KvCommand>): void {
    const url = this.peers[to];
    if (!url) return;
    fetch(`${url}/raft`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ from: this.id, msg }),
      signal: AbortSignal.timeout(1_000),
    })
      .then((res) => res.arrayBuffer())
      .catch(() => {
        // Lost messages are normal; Raft retries on the next heartbeat.
      });
  }

  private async route(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', 'http://localhost');
    const parts = url.pathname.split('/').filter(Boolean).map(decodeURIComponent);
    const raft = this.raft;

    if (req.method === 'POST' && url.pathname === '/raft') {
      const body = (await readJson(req)) as { from: NodeId; msg: RaftMessage<KvCommand> };
      res.writeHead(204).end();
      raft?.receive(body.from, body.msg);
      return;
    }
    if (!raft || !this.kv) return json(res, 503, { error: 'starting' });

    if (req.method === 'GET' && url.pathname === '/status') {
      return json(res, 200, { ...raft.status(), url: this.url, peers: this.peers });
    }

    if (parts[0] !== 'kv' || parts.length < 2 || parts.length > 3) {
      return json(res, 404, { error: 'not_found' });
    }
    const key = parts[1]!;
    const clientId = header(req, 'x-client-id') ?? '';
    const seq = Number(header(req, 'x-client-seq') ?? 0);
    let op: KvOp;
    let readMode: ReadMode | undefined;

    if (parts.length === 3) {
      if (req.method !== 'POST' || parts[2] !== 'cas') return json(res, 404, { error: 'not_found' });
      const body = (await readJson(req)) as { expected?: string | null; value?: string };
      if (typeof body.value !== 'string' || body.expected === undefined) {
        return json(res, 400, { error: 'cas needs {expected, value}' });
      }
      op = { type: 'cas', key, expected: body.expected, value: body.value };
    } else if (req.method === 'GET') {
      op = { type: 'get', key };
      const mode = (url.searchParams.get('mode') ?? 'read_index') as ReadMode;
      if (!READ_MODES.includes(mode)) return json(res, 400, { error: `unknown read mode ${mode}` });
      readMode = mode;
    } else if (req.method === 'PUT') {
      const body = (await readJson(req)) as { value?: unknown };
      if (typeof body.value !== 'string') return json(res, 400, { error: 'put needs {value: string}' });
      op = { type: 'put', key, value: body.value };
    } else if (req.method === 'DELETE') {
      op = { type: 'delete', key };
    } else {
      return json(res, 405, { error: 'method_not_allowed' });
    }

    const response = await withTimeout(
      this.kv.handleAsync({ clientId, seq, op, readMode }),
      this.options.requestTimeoutMs ?? 2_000,
    );
    if (response === 'timeout') return json(res, 503, { error: 'timeout' });
    this.respondKv(res, response);
  }

  private respondKv(res: ServerResponse, r: KvResponse): void {
    if (r.ok) return json(res, 200, r.result);
    if (r.error === 'not_leader') {
      const leaderUrl = r.leaderHint ? (this.peers[r.leaderHint] ?? null) : null;
      return json(res, 421, { error: 'not_leader', leader: r.leaderHint, leaderUrl });
    }
    json(res, 503, { error: r.error });
  }
}

function header(req: IncomingMessage, name: string): string | undefined {
  const v = req.headers[name];
  return Array.isArray(v) ? v[0] : v;
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const text = Buffer.concat(chunks).toString('utf8');
  return text.length === 0 ? {} : JSON.parse(text);
}

function json(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) });
  res.end(payload);
}

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T | 'timeout'> {
  return new Promise((resolve) => {
    const t = setTimeout(() => resolve('timeout'), ms);
    p.then((v) => {
      clearTimeout(t);
      resolve(v);
    });
  });
}
