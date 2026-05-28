// ─────────────────────────────────────────────────────────────────────────────
// Imports — resolved via the importmap in index.html (no bundler needed)
// ─────────────────────────────────────────────────────────────────────────────
import { createP2PDB, DBIndex, IDBStorage, DB_WORKER_URL } from '../src/index.js'

// ─────────────────────────────────────────────────────────────────────────────
// UI helpers
// ─────────────────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id)

function log(msg, type = 'info') {
  const el = document.createElement('div')
  el.className = `log-line ${type}`
  el.textContent = `${new Date().toLocaleTimeString()}  ${msg}`
  $('log').prepend(el)
  // Keep log bounded
  while ($('log').children.length > 60) $('log').lastChild.remove()
}

function setStatus(msg) { $('status').textContent = msg }

// ─────────────────────────────────────────────────────────────────────────────
// Step 1 — detect IDB availability
//
// IDBStorage.isAvailable() checks for SharedArrayBuffer + Atomics.
// Both require the page to be served with:
//   Cross-Origin-Opener-Policy:   same-origin
//   Cross-Origin-Embedder-Policy: require-corp
//
// If those headers are missing the browser disables SharedArrayBuffer and
// IDBBatchAtomicVFS cannot be used — we fall back to Mode A (in-memory SQLite
// + localStorage snapshot).
// ─────────────────────────────────────────────────────────────────────────────
const idbAvailable = IDBStorage.isAvailable()

const badge = $('storage-badge')
if (idbAvailable) {
  badge.textContent = 'IDB (durable)'
  badge.className = 'idb'
} else {
  badge.textContent = 'Memory + localStorage'
  badge.className = 'mem'
  log('Web Locks API not available — using in-memory + localStorage fallback', 'peer')
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 2 — open the database + start the P2P node
//
// createP2PDB does in one call:
//   a) DBIndex.open(name, { workerUrl })
//      — Mode B (IDB): spawns db-worker.js, opens IDBBatchAtomicVFS-backed SQLite,
//                       loads all rows from IDB into _store
//      — Mode A (mem): loads wa-sqlite WASM in-memory, hydrates from localStorage
//   b) P2PNode.create({ topic, … })
//      — creates libp2p node, dials bootstrap nodes, makes circuit relay
//        reservation, joins GossipSub topic
//   c) patches db.set / db.delete to broadcast over GossipSub after writing
//   d) wires onMessage to apply incoming db ops via _applySet/_applyDelete
//
// workerUrl:
//   DB_WORKER_URL is new URL('./db-worker.js', import.meta.url) — resolves to
//   the absolute URL of db-worker.js relative to db.js at runtime.
//   In non-bundled mode this works automatically.
//   In Vite use: import dbWorkerUrl from '../src/db-worker.js?url'
// ─────────────────────────────────────────────────────────────────────────────
setStatus('opening database…')
log('opening database…')

const { db, node } = await createP2PDB({
  // Store name — IndexedDB database name in IDB mode,
  //             localStorage key prefix in memory mode
  name: 'notes-app',

  // GossipSub topic — all browser tabs/windows using the same topic
  // form one shared message mesh, whether on the same machine or across
  // the internet (as long as they connect via bootstrap/relay nodes)
  topic: 'p2p-db-notes-v1',

  // Pass the worker URL to enable IDB mode.
  // When undefined, falls back to Mode A (in-memory + localStorage).
  workerUrl: idbAvailable ? DB_WORKER_URL : undefined,

  // onSync fires when a *remote* peer's db mutation has been applied locally.
  // The local _store and SQLite are already updated by the time this fires.
  // op    — 'set' | 'delete'
  // key   — the record key
  // value — the new value (undefined on delete)
  onSync(op, key, value) {
    if (op === 'set') {
      log(`← remote set   ${key}  tag:${value?.tag}`, 'remote')
      renderNotes()
    } else if (op === 'delete') {
      log(`← remote delete ${key}`, 'remote')
      renderNotes()
    }
  },

  // onMessage fires for GossipSub messages on the same topic that are NOT
  // db sync ops (i.e. not __type:'db:set'|'db:delete').
  // Use this for ephemeral signals that should not be persisted.
  onMessage(from, data) {
    if (data?.type === 'ping') {
      log(`← ping from ${from.slice(0, 16)}…`, 'peer')
    }
  },
})

log(`database opened (${idbAvailable ? 'IDB mode' : 'memory mode'})`)
setStatus('connecting to peers…')

// ─────────────────────────────────────────────────────────────────────────────
// Step 3 — define secondary indexes
//
// index(name, keyFn) scans the current _store and builds an in-memory
// bucket map: indexedValue → Set<primaryKey>.
// Must be called *after* open() so the store is hydrated.
// Re-call with the same name any time to rebuild from current _store.
//
// Secondary indexes survive between steps — they are rebuilt from the
// persisted records each time the page loads.
// ─────────────────────────────────────────────────────────────────────────────
db.index('tag',    r => r.tag)     // query by tag
db.index('author', r => r.author)  // query by author peer ID

log(`indexes built — ${db.size} existing records loaded`)

// ─────────────────────────────────────────────────────────────────────────────
// Step 4 — P2P node events
//
// peer:connect fires when libp2p establishes a new connection.
// Initially this fires for the bootstrap/relay nodes; later for other
// app peers discovered via pubsubPeerDiscovery.
//
// peer:disconnect fires when a connection drops (relay reconnects automatically).
// ─────────────────────────────────────────────────────────────────────────────
node.on('peer:connect', peerId => {
  log(`→ peer connected   ${peerId.slice(0, 20)}…`, 'peer')
  renderPeers()
  setStatus(`connected — ${node.peers.length} peers`)
})

node.on('peer:disconnect', peerId => {
  log(`→ peer disconnected ${peerId.slice(0, 20)}…`, 'peer')
  renderPeers()
})

// ─────────────────────────────────────────────────────────────────────────────
// Step 5 — db change events
//
// 'change' fires after *every* write, both local and remote.
// detail: { op, key, value?, remote?, from? }
//   remote:true  — came from a peer via GossipSub (_applySet/_applyDelete)
//   remote:false — written locally via db.set / db.delete
//
// We use this to log local writes; remote writes are already logged by onSync.
// ─────────────────────────────────────────────────────────────────────────────
db.on('change', ({ op, key, remote }) => {
  if (!remote) {
    log(`✎ local ${op.padEnd(6)} ${key}`, 'local')
  }
})

// ─────────────────────────────────────────────────────────────────────────────
// Step 6 — show own peer ID
// ─────────────────────────────────────────────────────────────────────────────
$('peer-id').textContent = node.peerId
log(`my peer ID: ${node.peerId.slice(0, 20)}…`)

// ─────────────────────────────────────────────────────────────────────────────
// Step 7 — initial render from persisted data
// ─────────────────────────────────────────────────────────────────────────────
renderNotes()
renderPeers()

// ─────────────────────────────────────────────────────────────────────────────
// Step 8 — write: db.set
//
// db.set flow with IDB mode:
//   1. _store.set(key, value)         — update read cache immediately (sync)
//   2. _syncIndexes(key, value)       — update bucket maps (sync)
//   3. idb.call('set', key, value)    — Worker: INSERT OR REPLACE INTO kv
//      Worker calls SQLite → vfs_write → Atomics.wait → IDB.put → Atomics.notify
//   4. emit 'change' event            — { op:'set', key, value, remote:false }
//   5. node.send({__type:'db:set',…}) — GossipSub broadcast to all mesh peers
//
// With memory mode steps 3 becomes:
//   3a. sqRun INSERT OR REPLACE INTO kv   — in-memory SQLite
//   3b. localStorage.setItem(…)           — JSON snapshot
// ─────────────────────────────────────────────────────────────────────────────
$('add-btn').addEventListener('click', addNote)
$('note-input').addEventListener('keydown', e => { if (e.key === 'Enter') addNote() })

async function addNote() {
  const body = $('note-input').value.trim()
  if (!body) return

  // Primary key: 'note:' + timestamp keeps insertion order and prevents collisions
  const key   = `note:${Date.now()}`
  const value = {
    body,
    tag:    $('tag-select').value,
    author: node.peerId,   // store author for index + display
    ts:     Date.now(),
  }

  // set() is async — awaits the IDB write (or localStorage write in Mode A)
  // before the GossipSub broadcast fires
  await db.set(key, value)

  $('note-input').value = ''
  renderNotes()
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 9 — delete: db.delete
//
// db.delete flow with IDB mode:
//   1. _store.delete(key)           — remove from read cache (sync)
//   2. _purgeIndexes(key)           — remove from all bucket maps (sync)
//   3. idb.call('delete', key)      — Worker: DELETE FROM kv WHERE key=?
//   4. emit 'change' event          — { op:'delete', key, remote:false }
//   5. node.send({__type:'db:delete', key}) — broadcast to peers
// ─────────────────────────────────────────────────────────────────────────────
async function deleteNote(key) {
  await db.delete(key)
  renderNotes()
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 10 — sync reads (always from _store, never touch IDB or SQLite)
//
// db.get(key)                    — O(1) Map lookup
// db.all()                       — full scan of _store as [{key, value}]
// db.query(indexName, value)     — O(1) bucket lookup → O(n) key resolution
// db.find(predicate)             — full scan with predicate
// db.size                        — _store.size
// db.has(key)                    — Map.has
//
// All of these are synchronous and never block the event loop.
// ─────────────────────────────────────────────────────────────────────────────
let activeFilter = null

$('filter-btn').addEventListener('click', () => {
  activeFilter = $('tag-select').value
  log(`filtering by tag: ${activeFilter}`)
  renderNotes()
})

$('filter-clear').addEventListener('click', () => {
  activeFilter = null
  renderNotes()
})

function renderNotes() {
  const container = $('notes')
  container.innerHTML = ''

  // db.query uses the 'tag' secondary index — O(1) bucket lookup
  // db.all() returns every record — linear scan of _store
  const records = activeFilter
    ? db.query('tag', activeFilter).map(value => {
        // query() returns values only; find the key from all()
        const match = db.find(r => r === value)
        return match[0] ?? { key: '?', value }
      })
    : db.all()   // [{key, value}, …] sorted by insertion order (Map preserves it)

  // Sort newest first
  records.sort((a, b) => (b.value?.ts ?? 0) - (a.value?.ts ?? 0))

  for (const { key, value } of records) {
    if (!value?.body) continue

    const isLocal = value.author === node.peerId
    const card = document.createElement('div')
    card.className = `note-card${isLocal ? '' : ' remote'}`
    card.innerHTML = `
      <button class="del-btn" title="delete">×</button>
      <div class="note-tag">${value.tag ?? '—'}</div>
      <div class="note-body">${escHtml(value.body)}</div>
      <div class="note-meta">
        <span>${isLocal ? 'me' : value.author?.slice(0, 10) + '…'}</span>
        <span>${new Date(value.ts).toLocaleTimeString()}</span>
      </div>
    `
    card.querySelector('.del-btn').addEventListener('click', () => deleteNote(key))
    container.appendChild(card)
  }

  if (!records.length) {
    container.innerHTML = '<div style="color:#444;padding:20px;grid-column:1/-1">' +
      (activeFilter ? `No notes tagged '${activeFilter}'.` : 'No notes yet.') + '</div>'
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 11 — raw SQL via db.sql()
//
// db.sql(query, params) runs a parameterised query on the actual SQLite engine.
//
// IDB mode:  proxied via postMessage to db-worker.js, runs against the
//            IDB-backed SQLite database, returns rows as [[col, col, …], …]
// Mem mode:  runs directly in the main-thread in-memory SQLite instance
// Fallback:  returns null if SQLite was unavailable during init
//
// kv table schema:
//   key   TEXT PRIMARY KEY    — e.g. 'note:1717000000000'
//   value TEXT                — JSON string of the record object
//
// Use json_extract(value, '$.field') for field-level access in SQL.
// ─────────────────────────────────────────────────────────────────────────────
$('sql-run').addEventListener('click', runSQL)

async function runSQL() {
  const query = $('sql-input').value.trim()
  if (!query) return

  const out = $('sql-output')
  out.textContent = 'running…'

  try {
    // db.sql() returns [[col0, col1, …], …] or null
    const rows = await db.sql(query)

    if (rows === null) {
      out.textContent = 'SQLite not available in this environment.'
      return
    }
    if (!rows.length) {
      out.textContent = '(no rows)'
      return
    }
    out.textContent = rows.map(r => JSON.stringify(r)).join('\n')
  } catch (err) {
    out.textContent = `Error: ${err.message}`
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 12 — ephemeral P2P message (not persisted to DB)
//
// node.send(data) publishes JSON-serialised data to the GossipSub topic.
// Peers receive it in onMessage (the non-db branch).
// Nothing is written to SQLite or localStorage.
// ─────────────────────────────────────────────────────────────────────────────
setInterval(() => {
  if (node.peers.length > 0) {
    // Send a ping every 30 s — received by peers via onMessage above
    node.send({ type: 'ping', from: node.peerId, ts: Date.now() }).catch(() => {})
  }
}, 30_000)

// ─────────────────────────────────────────────────────────────────────────────
// Step 13 — peer list rendering
// ─────────────────────────────────────────────────────────────────────────────
function renderPeers() {
  const list = $('peer-list')
  list.innerHTML = ''

  // Own entry
  const self = document.createElement('div')
  self.className = 'peer-item self'
  self.title = node.peerId
  self.textContent = `▶ ${node.peerId.slice(0, 24)}… (me)`
  list.appendChild(self)

  // node.peers returns currently connected peer IDs (bootstrap + app peers)
  for (const peerId of node.peers) {
    const el = document.createElement('div')
    el.className = 'peer-item'
    el.title = peerId
    el.textContent = `○ ${peerId.slice(0, 24)}…`
    list.appendChild(el)
  }

  $('peer-count').textContent = node.peers.length
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 14 — clean shutdown
//
// db.close() in IDB mode:
//   1. idb.call('close') → Worker sends 'PRAGMA wal_checkpoint(TRUNCATE)'
//      then sqlite3.close(db) — flushes all WAL pages to IDB before exit
//   2. idb.terminate() — terminates the Worker
//
// db.close() in memory mode:
//   sqlite3.close(db) — releases WASM memory (localStorage snapshot already
//   written on each mutation, nothing extra to flush)
//
// node.stop() — closes all libp2p connections, stops listeners and discovery
// ─────────────────────────────────────────────────────────────────────────────
window.addEventListener('beforeunload', () => {
  // beforeunload must be synchronous; fire-and-forget is best we can do here.
  // The WAL checkpoint in db-worker.js close handler covers the IDB flush.
  db.close()
  node.stop()
})

// ─────────────────────────────────────────────────────────────────────────────
// Utility
// ─────────────────────────────────────────────────────────────────────────────
function escHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}
