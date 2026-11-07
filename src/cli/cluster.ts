/**
 * Start a local three-node cluster as separate processes.
 *
 *   npm run cluster              # keeps data in .raft-data/
 *   npm run cluster -- --fresh   # wipes it first
 *
 * While running, type `kill n2`, `start n2` or `status` to fail and recover
 * nodes by hand.
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { rmSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { HttpKvClient } from '../net/http-client.js';

const { values } = parseArgs({
  options: {
    fresh: { type: 'boolean', default: false },
    'base-port': { type: 'string', default: '7001' },
    data: { type: 'string', default: '.raft-data' },
  },
});

const here = dirname(fileURLToPath(import.meta.url));
const basePort = Number(values['base-port']);
const dataRoot = resolve(values.data);
const ids = ['n1', 'n2', 'n3'];
const urls = Object.fromEntries(ids.map((id, i) => [id, `http://127.0.0.1:${basePort + i}`]));
const peerSpec = ids.map((id) => `${id}=${urls[id]}`).join(',');
const colors: Record<string, string> = { n1: '\x1b[36m', n2: '\x1b[35m', n3: '\x1b[33m' };

if (values.fresh) rmSync(dataRoot, { recursive: true, force: true });

const children = new Map<string, ChildProcess>();

function startNode(id: string): void {
  if (children.has(id)) return console.log(`${id} is already running`);
  const child = spawn(
    process.execPath,
    [
      join(here, 'node.js'),
      '--id', id,
      '--port', String(new URL(urls[id]!).port),
      '--peers', peerSpec,
      '--data', join(dataRoot, id),
    ],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  );
  const prefix = `${colors[id] ?? ''}[${id}]\x1b[0m `;
  const relay = (stream: NodeJS.ReadableStream) =>
    createInterface({ input: stream }).on('line', (line) => console.log(prefix + line));
  relay(child.stdout!);
  relay(child.stderr!);
  child.on('exit', (code, signal) => {
    children.delete(id);
    console.log(`${prefix}exited (${signal ?? code})`);
  });
  children.set(id, child);
}

for (const id of ids) startNode(id);

console.log(`
raft-kv: 3 nodes on ${Object.values(urls).join(', ')}, data in ${dataRoot}
  in another terminal:  npm run kv -- put greeting hello
                        npm run kv -- get greeting
                        npm run kv -- status
  here:                 kill n1 | start n1 | status | quit
`);

const client = new HttpKvClient(Object.values(urls), { maxAttempts: 1 });
createInterface({ input: process.stdin }).on('line', (line) => {
  const [cmd, id] = line.trim().split(/\s+/);
  if (cmd === 'kill' && id) {
    const child = children.get(id);
    if (child) child.kill('SIGKILL');
    else console.log(`${id} is not running`);
  } else if (cmd === 'start' && id) {
    startNode(id);
  } else if (cmd === 'status') {
    void client.status().then((rows) => {
      for (const r of rows) {
        console.log(
          `  ${String(r.id ?? r.url).padEnd(4)} ${String(r.role).padEnd(11)} term=${r.term ?? '-'} ` +
            `commit=${r.commitIndex ?? '-'} log=${r.lastLogIndex ?? '-'} snapshot=${r.snapshotIndex ?? '-'}`,
        );
      }
    });
  } else if (cmd === 'quit' || cmd === 'exit') {
    shutdown();
  } else if (cmd) {
    console.log('commands: kill <id> | start <id> | status | quit');
  }
});

function shutdown(): void {
  for (const child of children.values()) child.kill('SIGTERM');
  setTimeout(() => process.exit(0), 300);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
