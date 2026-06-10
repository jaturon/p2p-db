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
| **SQL** | Full SQLite query surface — raw arrays, named-column objects, transactions, introspection |
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

## Standalone bundle (single `<script>`, no importmap)

For dropping into any HTML page — served from a generic web server, or
opened directly as a `file://` page — build the single-file bundle:

```bash
npm install
npm run build      # → dist/p2p-db.js, dist/p2p-db.standalone.html,
                   #   dist/wa-sqlite.wasm, dist/db-worker.js
```

Then include it as a plain classic script (no `type="module"`, no importmap):

```html
<script src="dist/p2p-db.js"></script>
<script>
  (async () => {
    const { db, node } = await P2PDB.connect({
      name:  'my-app',
      // The public relays only route the 'p2p-db-notes-v1' topic — see
      // "Relay" below if you want your own topic.
      topic: 'p2p-db-notes-v1',
    })

    console.log('peer id:', node.peerId)
    await db.set('hello', { from: node.peerId, ts: Date.now() })
    db.on('change', e => console.log('change', e))
  })()
</script>
```

`P2PDB` exposes `connect` (alias for `createP2PDB`), `createP2PDB`, `DBIndex`,
`P2PNode`, `IDBStorage`, `BOOTSTRAP_LIST`, `DB_WORKER_URL`, `DEFAULT_RELAY`
and `DEFAULT_RELAYS` (`['http://202.44.53.65:4010', 'http://199.241.138.174:4010']`
— both relays are peered with each other, so connecting to either reaches
the same network).

A full working example is in `standalone/` (`index.html` + `app.js`). It
loads `dist/p2p-db.js` via `<script src="...">`, so **serve it over
http(s)**:

```bash
npm run build
python3 -m http.server 8123
# → open http://localhost:8123/standalone/
```

### Opening directly from disk (`file://`, double-click)

Open `dist/p2p-db.standalone.html` directly in a browser — no server, no
flags. It's the same demo as `standalone/`, but built with the `P2PDB`
bundle and the app code inlined into a single `.html` file.

> Browsers (Firefox especially) block `<script src="...">` references to
> sibling files when a page is opened via `file://` — every `file://`
> document is treated as a unique security origin, so the multi-file
> `standalone/` example (and any multi-file setup of your own) only works
> when served over http(s). For the double-click case, inline everything
> into one `.html` file the way `dist/p2p-db.standalone.html` does.

What works when opened as `file://`:

- **P2P / relay connection works** — WebSocket connections aren't subject to
  the `file://` CORS restrictions, so `node.peers` and GossipSub sync work
  normally.
- **`db.get/set/delete/all/find/index/watch/export/import` work** — the
  localStorage-backed Map store doesn't need to fetch anything.
- **`db.sql()` / `db.sqlAll()` etc. are unavailable** — browsers block
  `fetch()` of `file://` resources, so `wa-sqlite.wasm` can't load. `DBIndex`
  detects this and falls back to memory-only + localStorage automatically
  (logged as a warning, not an error).
- **Mode B (`workerUrl: P2PDB.DB_WORKER_URL`, durable IndexedDB)** requires
  http(s) serving — module Workers and wasm fetches don't work under `file://`.

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
```

---

### Single-record reads (sync, O(1) from in-memory Map)

```js
db.get('note:1')           // value or undefined
db.has('note:1')           // boolean
db.size                    // total record count
```

### Multi-record reads (sync)

```js
db.all()                          // [{key, value}, …] — insertion order
db.keys()                         // ['note:1', 'note:2', …]
db.values()                       // [{body:'…'}, …]
db.find(v => v.tag === 'idea')    // [{key, value}, …] — predicate scan
db.count()                        // 42
db.count(v => v.done)             // 7  — filtered count
db.first()                        // {key, value} | undefined — first record
db.first(v => v.tag === 'idea')   // first matching
db.last()                         // last record
db.getMany(['a','b','c'])         // { a:{…}, c:{…} }  — missing keys omitted
```

### Writes (async — persist to IDB/localStorage, broadcast to peers)

```js
await db.set('note:1', { body: 'hello', tag: 'idea', ts: Date.now() })
await db.delete('note:1')
await db.update('note:1', v => ({ ...v, done: true }))   // atomic read-modify-write
await db.setMany([['k1', v1], ['k2', v2]])               // bulk (single IDB tx)
await db.deleteMany(['k1', 'k2'])                        // bulk delete
await db.clear()
```

### Secondary indexes

```js
db.index('tag',    r => r.tag)      // build / rebuild
db.index('author', r => r.author)

db.query('tag', 'idea')             // [value, …]           — O(1) bucket lookup
db.queryEntries('tag', 'idea')      // [{key, value}, …]    — includes primary key
db.queryKeys('tag', 'idea')         // ['note:1', …]         — keys only
db.indexValues('tag')               // ['general','work','idea','todo']  — all buckets
```

### Export / Import

```js
const snap = db.export()             // { 'note:1': {…}, … }  plain object
await db.import(snap)                // merge into existing data
await db.import(snap, { clear: true }) // replace all data
```

### Watch a key

```js
const stop = db.watch('note:1', ({ op, value, remote }) => {
  console.log(op, value)  // 'set' | 'delete'
})
stop()  // unsubscribe
```

### Events

```js
db.on('change', ({ op, key, value, remote, from }) => { … })
// op: 'set' | 'delete' | 'clear'
// remote: true when the change came from another peer
```

---

### SQL queries

kv table schema: `key TEXT PRIMARY KEY, value TEXT` (value is JSON).
Use `json_extract(value, '$.field')` for field-level access.

```js
// Raw rows — [[col, col, …], …]
await db.sql("SELECT key, json_extract(value,'$.tag') as tag FROM kv LIMIT 5")

// Named-column rows — [{col: val, …}, …]
await db.sqlAll("SELECT key, json_extract(value,'$.tag') as tag FROM kv")
// → [{key:'note:1', tag:'idea'}, …]

// First row as object — {col: val} or null
await db.sqlGet("SELECT * FROM kv WHERE key=?", ['note:1'])

// Scalar — first column of first row (COUNT, MAX, SUM …)
await db.sqlValue("SELECT COUNT(*) FROM kv WHERE json_extract(value,'$.done')=1")
// → 7

// Execute without returning rows — {changes, lastInsertRowId}
await db.sqlRun(
  "UPDATE kv SET value=json_patch(value,?) WHERE key=?",
  ['{"done":true}', 'note:1']
)
// → {changes: 1, lastInsertRowId: 0}

// Atomic multi-statement block — auto-rollback on error
await db.sqlTransaction(async db => {
  await db.sqlRun("DELETE FROM kv WHERE json_extract(value,'$.done')=1")
  await db.set('archive:' + Date.now(), { archived: true })
})

// PRAGMA
await db.pragma('journal_mode')       // 'memory' or 'wal'
await db.pragma('page_size')          // 4096
```

### SQLite introspection

```js
await db.tables()          // ['kv']
await db.schema()          // { kv: 'CREATE TABLE kv (key TEXT PRIMARY KEY …)' }
await db.schema('kv')      // 'CREATE TABLE kv (key TEXT PRIMARY KEY NOT NULL …)'
```

---

### P2P node

```js
node.peerId                     // string
node.peers                      // string[]
node.multiaddrs                 // string[]
node.relayMultiaddrs            // circuit-relay addresses
node.send(data)                 // publish any JSON (not persisted)
node.on('peer:connect',    id => { … })
node.on('peer:disconnect', id => { … })
await node.dialRelay('http://202.44.53.65:4010')  // connect relay at runtime
await node.stop()

await db.close()
```

---

### Storage modes

| Mode | How to enable | Durability | Limit |
|---|---|---|---|
| **IDB (Mode B)** | `workerUrl: DB_WORKER_URL` | survives reload | ~1 GB |
| **Memory + localStorage (Mode A)** | omit `workerUrl` | survives reload (JSON snapshot) | ~5 MB |

IDB mode requires `navigator.locks` (Web Locks API): Chrome 69+, Firefox 96+, Safari 15.4+.  
IDB falls back to memory mode automatically if the worker fails (e.g. Firefox < 114, non-localhost HTTP).

## Example app

`example/` is a notes app that exercises the full API:

- **Add / delete / edit notes** — `db.set` / `db.delete` / `db.update`
- **Mark done** — `db.set` update-in-place, broadcast to peers
- **Tag filter** — secondary index (`db.queryEntries`)
- **Text search** — `db.find` predicate scan
- **Sort** — `db.all()` + sort on `value.ts`
- **Stats** — `db.size`, `db.count()`, `db.all()` aggregation
- **SQL panel** — `db.sqlAll()` with quick-example dropdown
- **Export / Import** — `db.export()` / `db.import()`
- **Relay connect** — `node.dialRelay(url)` from the sidebar

## Relay

The relay is a minimal libp2p node that:

1. Accepts WebSocket connections from browsers
2. Makes circuit relay v2 reservations so browsers get a public address
3. Subscribes to the app's GossipSub topic and routes messages between peers
4. Default public relays: `http://202.44.53.65:4010` and `http://199.241.138.174:4010`
   (peered with each other, auto-used as fallback)

### Run locally

```bash
cd relay && npm install
API_PORT=4010 WS_PORT=4012 node server.js
```

The page auto-connects to `localhost:4010` when served from `localhost` or a `*.local` hostname.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `WS_PORT` | `8080` | Internal WebSocket port |
| `API_PORT` | `3000` | HTTP `/api/info` port |
| `HOSTNAME` | *(none)* | Public hostname for announced multiaddr |
| `WS_EXTERNAL_PORT` | `443` | External WSS port clients dial |
| `TOPICS` | `p2p-db-notes-v1,_peer-discovery._p2p._pubsub` | GossipSub topics to subscribe |
| `KEY_FILE` | `/data/relay.key` or `./relay.key` | Ed25519 key for stable peer ID |

### Deploy to Fly.io (free)

```bash
cd relay
fly launch --no-deploy
fly volumes create relay_data --region sin --size 1 --yes
fly secrets set HOSTNAME=<your-app>.fly.dev
fly deploy
```

> Fly.io free hobby plan requires a credit card on file but charges $0/month.

## Source layout

```
src/
  index.js          — createP2PDB() + re-exports
  standalone.js     — entry point for the bundled dist/p2p-db.js build
  db.js             — DBIndex class (storage + indexes + SQL helpers)
  db-worker.js      — Web Worker: SQLite engine + IDBBatchAtomicVFS
  idb-storage.js    — Main-thread proxy for db-worker (postMessage RPC)
  p2p.js            — P2PNode class (libp2p v2 + GossipSub)

build.js            — esbuild config; `npm run build` → dist/

dist/               — built by `npm run build` (gitignored)
  p2p-db.js         — single-file IIFE bundle, global `P2PDB`
  p2p-db.standalone.html — standalone/ demo with bundle + app inlined,
                           opens directly via file:// (no server, no flags)
  wa-sqlite.wasm    — copied next to p2p-db.js (Mode A SQL)
  db-worker.js      — copied as-is (Mode B, opt-in)

standalone/
  index.html        — <script src="../dist/p2p-db.js"> example, no importmap
                       (requires http(s) serving — see file:// section above)
  app.js            — same notes-app walkthrough using the global P2PDB

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

1. `db.set(key, value)` writes to the local SQLite/IDB, then broadcasts `{__type:'db:set', key, value}` via GossipSub.
2. The relay forwards the message to all connected browsers on the same topic.
3. Each peer's `onMessage` handler calls `db._applySet(key, value)` — writes locally without re-broadcasting.
4. The `change` event fires with `remote: true` and the UI re-renders.
5. When a new peer joins it sends `db:sync-request`; existing peers reply by flooding their full state.

There is no conflict resolution — last write wins.

## Browser compatibility

| Browser | IDB mode | Memory mode | Sync |
|---|---|---|---|
| Chrome 80+ (localhost) | ✅ | — | ✅ |
| Chrome (LAN IP, HTTP) | — | ✅ | ✅ |
| Firefox 114+ | ✅ | — | ✅ |
| Firefox 96–113 | — | ✅ (auto-fallback) | ✅ |
| Safari 15+ | ✅ | — | ✅ |

`SharedArrayBuffer` is **not** required — wa-sqlite ≥ 1.0 uses Web Locks instead.
