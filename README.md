# p2p-db

A browser-native P2P key-value store. Every write is broadcast to connected peers via GossipSub; persistence uses SQLite backed by IndexedDB (IDB). No bundler, no server-side logic, no build step — runs directly from static files.

```
Browser A ──ws──▶ Relay ──ws──▶ Browser B
               GossipSub mesh
                  SQLite/IDB ←sync→ SQLite/IDB
```

## Features

| | |
|---|---|
| **Storage** | SQLite via [wa-sqlite](https://github.com/rhashimoto/wa-sqlite) — IDB-backed (durable, ~1 GB) or in-memory + localStorage fallback |
| **Sync** | [libp2p](https://libp2p.io) v2 — WebSocket transport, circuit relay v2, GossipSub |
| **Secondary indexes** | In-memory bucket maps rebuilt from the persisted store on load |
| **Raw SQL** | `db.sql(query, params)` — full SQLite via the worker or main thread |
| **No bundler** | Browser importmap + esm.sh CDN; works as plain HTML + JS |

## Quick start

```bash
# 1. Start the relay (routes GossipSub between browsers)
cd relay && npm install
API_PORT=4010 WS_PORT=4012 node server.js

# 2. Serve the example
cd ..
node example/server.js 8099
# → open http://localhost:8099/example/
```

Open the URL in two browser tabs — notes written in one appear in the other in real time.

## Library API

```js
import { createP2PDB, DBIndex, IDBStorage, DB_WORKER_URL } from './src/index.js'

const { db, node } = await createP2PDB({
  name:      'my-app',           // IDB database name / localStorage prefix
  topic:     'my-app-v1',        // GossipSub topic — all peers on this topic sync
  workerUrl: DB_WORKER_URL,      // omit to fall back to in-memory + localStorage

  onSync(op, key, value) { … },  // fires after a remote write is applied locally
  onMessage(from, data)  { … },  // fires for non-db GossipSub messages
})

// Writes — async, persist to IDB, broadcast to peers
await db.set('note:1', { body: 'hello', tag: 'idea', ts: Date.now() })
await db.delete('note:1')

// Reads — sync, always from the in-memory Map (_store)
db.get('note:1')           // O(1)
db.all()                   // [{key, value}, …]
db.find(r => r.tag === 'idea')
db.has('note:1')
db.size

// Secondary indexes
db.index('tag', r => r.tag)             // build / rebuild
db.query('tag', 'idea')                 // O(1) bucket lookup → values

// Raw SQL (IDB mode: proxied to worker; mem mode: main thread)
await db.sql("SELECT key, json_extract(value,'$.tag') FROM kv WHERE …", [])

// Events
db.on('change', ({ op, key, value, remote, from }) => { … })

// P2P
node.peerId           // string
node.peers            // string[]
node.multiaddrs       // string[]
node.relayMultiaddrs  // circuit-relay addresses (reachable from internet via relay)
node.send(data)       // publish any JSON to the topic (not persisted)
node.on('peer:connect', peerId => { … })
node.on('peer:disconnect', peerId => { … })
await node.dialRelay('https://my-relay.fly.dev')  // connect to a relay at runtime
await node.stop()

await db.close()
```

### Storage modes

| Mode | How to enable | Durability | Limit |
|---|---|---|---|
| **IDB (Mode B)** | pass `workerUrl: DB_WORKER_URL` | survives reload | ~1 GB |
| **Memory + localStorage (Mode A)** | omit `workerUrl` | survives reload (JSON snapshot) | ~5 MB |

IDB mode requires the page to be served with the Web Locks API available (`navigator.locks`). All modern browsers support this without special HTTP headers.

## Example app

`example/` is a notes app that exercises the full API surface:

- **Add / delete notes** — `db.set` / `db.delete`
- **Inline edit** — `db.set` update-in-place, broadcast to peers
- **Mark done** — single-field update via `db.set`
- **Tag filter** — secondary index (`db.query`)
- **Text search** — `db.find` predicate scan
- **Sort** — `db.all()` + sort on `value.ts`
- **Stats** — `db.size`, `db.all()` aggregation
- **Raw SQL panel** — `db.sql()` with quick-example dropdown
- **Relay connect** — `node.dialRelay(url)` from the sidebar

## Relay

The relay is a minimal libp2p node that:

1. Accepts WebSocket connections from browsers
2. Makes circuit relay v2 reservations so browsers get a public address
3. Subscribes to the app's GossipSub topic and routes messages between peers

### Run locally

```bash
cd relay && npm install
API_PORT=4010 WS_PORT=4012 node server.js
```

Environment variables:

| Variable | Default | Description |
|---|---|---|
| `WS_PORT` | `8080` | Internal WebSocket port |
| `API_PORT` | `3000` | HTTP `/api/info` port |
| `HOSTNAME` | *(none)* | Public hostname for announced multiaddr (e.g. `my-relay.fly.dev`) |
| `WS_EXTERNAL_PORT` | `443` | External WSS port clients dial |
| `TOPICS` | `p2p-db-notes-v1` | Comma-separated GossipSub topics to subscribe to |
| `KEY_FILE` | `/data/relay.key` or `./relay.key` | Path to persist Ed25519 private key (stable peer ID) |

### Deploy to Fly.io (free)

```bash
cd relay
fly launch --no-deploy          # imports fly.toml, pick an app name
fly volumes create relay_data --region sin --size 1 --yes
fly secrets set HOSTNAME=<your-app>.fly.dev
fly deploy
```

The app is then reachable at `https://<your-app>.fly.dev`. Connect from the browser by appending `?relay=https://<your-app>.fly.dev` to the example URL, or by typing the URL into the relay connect field in the sidebar.

> Fly.io free hobby plan requires a credit card on file but charges $0/month for a single shared-cpu-1x machine.

## Source layout

```
src/
  index.js          — createP2PDB() + re-exports
  db.js             — DBIndex class (storage + indexes)
  db-worker.js      — Web Worker: SQLite engine + IDBBatchAtomicVFS
  idb-storage.js    — Main-thread proxy for db-worker (postMessage RPC)
  p2p.js            — P2PNode class (libp2p v2 + GossipSub)

example/
  index.html        — importmap + styles
  app.js            — notes app (annotated walkthrough of the full API)
  server.js         — minimal static file server for local dev

relay/
  server.js         — libp2p relay node
  package.json
  fly.toml          — Fly.io deployment config
```

## How sync works

1. `db.set(key, value)` writes to the local SQLite/IDB, then calls `node.send({ __type: 'db:set', key, value })`.
2. The relay (subscribed to the topic) receives the message and forwards it to all other connected browsers on the same topic.
3. Each peer's `onMessage` handler calls `db._applySet(key, value)` — writes to its own SQLite/IDB without re-broadcasting.
4. The `change` event fires with `remote: true` and the UI re-renders.

There is no conflict resolution — last write wins.

## Browser compatibility

Requires a modern browser with:
- `navigator.locks` (Web Locks API) — for IDB mode; available in Chrome 69+, Firefox 96+, Safari 15.4+
- ES modules + importmap — Chrome 89+, Firefox 108+, Safari 16.4+
- `SharedArrayBuffer` is **not** required (wa-sqlite ≥ 1.0 uses Web Locks instead)
