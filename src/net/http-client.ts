import { randomUUID } from 'node:crypto';
import type { KvResult, ReadMode } from '../kv/types.js';

export interface HttpKvClientOptions {
  clientId?: string;
  /** Total attempts across nodes before giving up. */
  maxAttempts?: number;
  /** Per-attempt HTTP timeout (ms). */
  attemptTimeoutMs?: number;
  /** Pause between attempts that did not reach the leader (ms). */
  backoffMs?: number;
}

export class KvClientError extends Error {}

/**
 * Client for an HttpNode cluster. Finds the leader by following 421
 * redirects, fails over on timeouts, and tags writes with (clientId, seq)
 * so a retried write is applied exactly once.
 */
export class HttpKvClient {
  readonly clientId: string;
  private seq = 0;
  private leader: string | null = null;
  private readonly maxAttempts: number;
  private readonly attemptTimeoutMs: number;
  private readonly backoffMs: number;

  constructor(
    private readonly urls: string[],
    options: HttpKvClientOptions = {},
  ) {
    if (urls.length === 0) throw new Error('need at least one node URL');
    this.clientId = options.clientId ?? randomUUID();
    this.maxAttempts = options.maxAttempts ?? 30;
    this.attemptTimeoutMs = options.attemptTimeoutMs ?? 3_000;
    this.backoffMs = options.backoffMs ?? 100;
  }

  get(key: string, mode: ReadMode = 'read_index'): Promise<string | null> {
    return this.call('GET', `/kv/${encodeURIComponent(key)}?mode=${mode}`).then((r) => r.value);
  }

  /** Returns the previous value. */
  put(key: string, value: string): Promise<string | null> {
    return this.call('PUT', `/kv/${encodeURIComponent(key)}`, { value }).then((r) => r.value);
  }

  /** Returns the previous value. */
  delete(key: string): Promise<string | null> {
    return this.call('DELETE', `/kv/${encodeURIComponent(key)}`).then((r) => r.value);
  }

  cas(key: string, expected: string | null, value: string): Promise<KvResult> {
    return this.call('POST', `/kv/${encodeURIComponent(key)}/cas`, { expected, value });
  }

  /** Status of every reachable node. */
  async status(): Promise<Array<Record<string, unknown>>> {
    return Promise.all(
      this.urls.map(async (url) => {
        try {
          const res = await fetch(`${url}/status`, { signal: AbortSignal.timeout(1_000) });
          return (await res.json()) as Record<string, unknown>;
        } catch {
          return { url, role: 'unreachable' };
        }
      }),
    );
  }

  private async call(method: string, path: string, body?: unknown): Promise<KvResult> {
    const headers: Record<string, string> = { 'content-type': 'application/json' };
    if (method !== 'GET') {
      headers['x-client-id'] = this.clientId;
      headers['x-client-seq'] = String(++this.seq);
    }
    let next = 0;
    let lastError = 'no attempts made';
    for (let attempt = 0; attempt < this.maxAttempts; attempt++) {
      const url = this.leader ?? this.urls[next++ % this.urls.length]!;
      try {
        const res = await fetch(url + path, {
          method,
          headers,
          body: body === undefined ? undefined : JSON.stringify(body),
          signal: AbortSignal.timeout(this.attemptTimeoutMs),
        });
        const data = (await res.json()) as Record<string, unknown>;
        if (res.status === 200) {
          this.leader = url;
          return data as unknown as KvResult;
        }
        if (res.status === 421) {
          this.leader = typeof data.leaderUrl === 'string' && data.leaderUrl !== url ? data.leaderUrl : null;
          lastError = `${url}: not leader`;
          if (this.leader) continue; // follow the redirect immediately
        } else if (res.status >= 400 && res.status < 500) {
          throw new KvClientError(`${res.status}: ${String(data.error)}`);
        } else {
          this.leader = null;
          lastError = `${url}: ${String(data.error)}`;
        }
      } catch (err) {
        if (err instanceof KvClientError) throw err;
        this.leader = null;
        lastError = `${url}: ${(err as Error).message}`;
      }
      await new Promise((r) => setTimeout(r, this.backoffMs));
    }
    throw new KvClientError(`gave up after ${this.maxAttempts} attempts (last: ${lastError})`);
  }
}
