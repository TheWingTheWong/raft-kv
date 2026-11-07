/**
 * Run a single raft-kv server.
 *
 *   node dist/cli/node.js --id n1 --port 7001 \
 *     --peers n1=http://127.0.0.1:7001,n2=http://127.0.0.1:7002,n3=http://127.0.0.1:7003 \
 *     --data .raft-data/n1
 */
import { parseArgs } from 'node:util';
import { HttpNode } from '../net/http-node.js';
import { FileStorage } from '../raft/storage.js';
import type { KvCommand } from '../kv/types.js';

const { values } = parseArgs({
  options: {
    id: { type: 'string' },
    port: { type: 'string' },
    host: { type: 'string', default: '127.0.0.1' },
    peers: { type: 'string' },
    data: { type: 'string' },
  },
});

if (!values.id || !values.port || !values.peers || !values.data) {
  console.error('usage: node.js --id <id> --port <port> --peers id=url,... --data <dir>');
  process.exit(2);
}

const peers: Record<string, string> = {};
for (const pair of values.peers.split(',')) {
  const [id, url] = pair.split('=');
  if (!id || !url) throw new Error(`bad peer spec: ${pair}`);
  peers[id] = url;
}

const id = values.id;
const say = (msg: string) => console.log(`${new Date().toISOString().slice(11, 23)} ${msg}`);

const server = new HttpNode({
  id,
  host: values.host,
  port: Number(values.port),
  storage: new FileStorage<KvCommand>(values.data),
  observer: {
    onRoleChange: (node, role, term) => {
      if (role === 'leader') say(`leader for term ${term} (log ${node.lastLogIndex})`);
      else if (role === 'candidate') say(`campaigning in term ${term}`);
      else say(`follower in term ${term}`);
    },
  },
});

const url = await server.listen();
server.start(peers);
const s = server.node!.status();
say(`listening on ${url}, recovered term ${s.term}, log ${s.lastLogIndex}, snapshot ${s.snapshotIndex}`);

const shutdown = () => {
  void server.close().then(() => process.exit(0));
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
