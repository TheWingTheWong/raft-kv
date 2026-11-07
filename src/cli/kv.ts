#!/usr/bin/env node
/**
 * Command-line client.
 *
 *   raft-kv put <key> <value>
 *   raft-kv get <key> [--mode read_index|lease|log|local]
 *   raft-kv del <key>
 *   raft-kv cas <key> <expected|-> <value>     ("-" means "key absent")
 *   raft-kv status
 *
 * --nodes url,url,... overrides the default 127.0.0.1:7001-7003.
 */
import { parseArgs } from 'node:util';
import { HttpKvClient } from '../net/http-client.js';
import type { ReadMode } from '../kv/types.js';

const { values, positionals } = parseArgs({
  allowPositionals: true,
  options: {
    nodes: { type: 'string', default: 'http://127.0.0.1:7001,http://127.0.0.1:7002,http://127.0.0.1:7003' },
    mode: { type: 'string', default: 'read_index' },
  },
});

const client = new HttpKvClient(values.nodes.split(','), { maxAttempts: 20 });
const [cmd, ...args] = positionals;
const show = (v: string | null) => console.log(v === null ? '(nil)' : v);

function need(n: number, usage: string): string[] {
  if (args.length !== n) {
    console.error(`usage: raft-kv ${usage}`);
    process.exit(2);
  }
  return args;
}

try {
  switch (cmd) {
    case 'get': {
      const [key] = need(1, 'get <key> [--mode read_index|lease|log|local]');
      show(await client.get(key!, values.mode as ReadMode));
      break;
    }
    case 'put': {
      const [key, value] = need(2, 'put <key> <value>');
      await client.put(key!, value!);
      console.log('OK');
      break;
    }
    case 'del':
    case 'delete': {
      const [key] = need(1, 'del <key>');
      const prev = await client.delete(key!);
      console.log(prev === null ? '(nil)' : `deleted ${prev}`);
      break;
    }
    case 'cas': {
      const [key, expected, value] = need(3, 'cas <key> <expected|-> <value>');
      const r = await client.cas(key!, expected === '-' ? null : expected!, value!);
      console.log(r.swapped ? 'swapped' : `not swapped (current: ${r.value ?? '(nil)'})`);
      break;
    }
    case 'status': {
      for (const r of await client.status()) {
        console.log(
          `${String(r.id ?? r.url).padEnd(4)} ${String(r.role).padEnd(11)} term=${r.term ?? '-'} ` +
            `leader=${r.leaderId ?? '-'} commit=${r.commitIndex ?? '-'} log=${r.lastLogIndex ?? '-'} ` +
            `snapshot=${r.snapshotIndex ?? '-'}`,
        );
      }
      break;
    }
    default:
      console.error('usage: raft-kv <get|put|del|cas|status> ...');
      process.exit(2);
  }
} catch (err) {
  console.error(`error: ${(err as Error).message}`);
  process.exit(1);
}
