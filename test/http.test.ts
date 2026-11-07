/**
 * End-to-end over real sockets and real timers: three HttpNodes on
 * ephemeral localhost ports with file-backed storage.
 */
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { HttpKvClient } from '../src/net/http-client.js';
import { HttpNode } from '../src/net/http-node.js';
import { FileStorage } from '../src/raft/storage.js';

const config = { electionTimeoutMin: 150, electionTimeoutMax: 300, heartbeatInterval: 30 };

describe('HTTP cluster', () => {
  const root = mkdtempSync(join(tmpdir(), 'raft-kv-http-'));
  const ids = ['n1', 'n2', 'n3'];
  const nodes = new Map<string, HttpNode>();
  const peers: Record<string, string> = {};
  let client: HttpKvClient;

  async function boot(id: string, port = 0): Promise<HttpNode> {
    const node = new HttpNode({
      id,
      port,
      storage: new FileStorage(join(root, id), { fsync: false }),
      config,
    });
    peers[id] = await node.listen();
    nodes.set(id, node);
    return node;
  }

  async function waitFor<T>(fn: () => T | undefined, ms = 5_000): Promise<T> {
    const deadline = Date.now() + ms;
    for (;;) {
      const v = fn();
      if (v !== undefined) return v;
      if (Date.now() > deadline) throw new Error('timed out');
      await new Promise((r) => setTimeout(r, 20));
    }
  }

  const leader = () => [...nodes.values()].find((n) => n.node?.role === 'leader');

  beforeAll(async () => {
    for (const id of ids) await boot(id);
    for (const node of nodes.values()) node.start(peers);
    client = new HttpKvClient(Object.values(peers));
    await waitFor(leader);
  });

  afterAll(async () => {
    await Promise.all([...nodes.values()].map((n) => n.close()));
    rmSync(root, { recursive: true, force: true });
  });

  it('serves put/get/cas/delete through any node', async () => {
    expect(await client.put('greeting', 'hello')).toBeNull();
    expect(await client.get('greeting')).toBe('hello');
    expect(await client.get('greeting', 'lease')).toBe('hello');
    expect(await client.get('greeting', 'log')).toBe('hello');
    expect(await client.cas('greeting', 'nope', 'x')).toEqual({ value: 'hello', swapped: false });
    expect(await client.cas('greeting', 'hello', 'hi')).toEqual({ value: 'hello', swapped: true });
    expect(await client.delete('greeting')).toBe('hi');
    expect(await client.get('greeting')).toBeNull();
  });

  it('redirects followers to the leader', async () => {
    const l = await waitFor(leader);
    const follower = ids.find((id) => id !== l.id)!;
    const res = await fetch(`${peers[follower]}/kv/x`, { method: 'PUT', body: '{"value":"1"}' });
    expect(res.status).toBe(421);
    expect(await res.json()).toMatchObject({ error: 'not_leader', leader: l.id, leaderUrl: peers[l.id] });
  });

  it('survives the leader dying and keeps acknowledged writes after a full restart', async () => {
    await client.put('durable', 'yes');
    const old = await waitFor(leader);
    const port = Number(new URL(peers[old.id]!).port);
    await old.close();
    nodes.delete(old.id);

    // The remaining two elect a new leader and keep serving.
    expect(await client.put('after', 'failover')).toBeNull();
    expect(await client.get('durable')).toBe('yes');

    // Bring the old leader back on the same port from its files.
    const back = await boot(old.id, port);
    back.start(peers);
    await waitFor(() => {
      const l = leader();
      return l && back.node!.applied >= l.node!.commit ? true : undefined;
    });
    expect(back.node!.lastLogIndex).toBeGreaterThan(0);
  });
});
