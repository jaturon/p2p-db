// ─────────────────────────────────────────────────────────────────────────────
// Imports — resolved via the importmap in index.html (no bundler needed)
// ─────────────────────────────────────────────────────────────────────────────
import { createP2PDB, DBIndex, IDBStorage, DB_WORKER_URL, BOOTSTRAP_LIST } from '../src/index.js'

// ─────────────────────────────────────────────────────────────────────────────
// UI helpers
// ─────────────────────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id)

let activeFilter = null   // tag filter set by 'Filter tag' button, or null
let searchText   = ''     // live text filter from search-input
let sortOrder    = 'newest'  // 'newest' | 'oldest'
let editingKey   = null   // primary key of note being edited inline, or null

// Fetch relay peer IDs for sidebar labelling.
// Tries the configured ?relay= URLs first; falls back to local port 4010.
// All fetches are fire-and-forget so failures don't block startup.
const RELAY_PEER_IDS = new Set()
{
  const param = new URLSearchParams(location.search).get('relay')
  const urls = param
    ? param.split(',').map(u => `${u.trim().replace(/\/$/, '')}/api/info`).filter(Boolean)
    : [`http://${location.hostname}:4010/api/info`, 'http://localhost:4010/api/info']
  for (const url of urls) {
    fetch(url, { signal: AbortSignal.timeout(2000) })
      .then(r => r.json()).then(info => { if (info.peer_id) RELAY_PEER_IDS.add(info.peer_id) })
      .catch(() => {})
  }
}

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
// IDBStorage.isAvailable() checks for navigator.locks (Web Locks API).
// wa-sqlite ≥1.0 uses Web Locks instead of SharedArrayBuffer for the IDB VFS
// bridge, so no special COOP/COEP headers are required.
//
// If Web Locks is unavailable we fall back to Mode A (in-memory SQLite +
// localStorage snapshot).
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
//      — Mode B (IDB): spawns db-worker.js, opens IDBBatchAtomicVFS-backed SQLite
//      — Mode A (mem): loads wa-sqlite WASM in-memory, hydrates from localStorage
//   b) P2PNode.create({ topic, … })
//      — creates libp2p node, dials bootstrap nodes, makes circuit relay
//        reservation, joins GossipSub topic
//   c) patches db.set / db.delete to broadcast over GossipSub after writing
//   d) wires onMessage to apply incoming db ops via _applySet/_applyDelete
// ─────────────────────────────────────────────────────────────────────────────
setStatus('opening database…')
log('opening database…')

const { db, node } = await createP2PDB({
  // Store name — IndexedDB database name in IDB mode,
  //             localStorage key prefix in memory mode
  name: 'notes-app',

  // GossipSub topic — all browser tabs/windows using the same topic
  // form one shared message mesh
  topic: 'p2p-db-notes-v1',

  // Pass the worker URL to enable IDB mode.
  // When undefined, falls back to Mode A (in-memory + localStorage).
  workerUrl: idbAvailable ? DB_WORKER_URL : undefined,

  // onSync fires when a *remote* peer's db mutation has been applied locally.
  // The local _store and SQLite are already updated by the time this fires.
  onSync(op, key, value) {
    if (op === 'set') {
      log(`← remote set   ${key}  tag:${value?.tag}`, 'remote')
    } else if (op === 'delete') {
      log(`← remote delete ${key}`, 'remote')
    }
    render()
  },

  // onMessage fires for GossipSub messages that are NOT db sync ops.
  // Use this for ephemeral signals that should not be persisted.
  onMessage(from, data) {
    if (data?.type === 'ping') {
      log(`← ping from ${from.slice(0, 16)}…`, 'peer')
    }
  },
})

// _initIDB may have silently fallen back to memory mode (e.g. Firefox < 114).
// Detect this and update the badge so the user sees the real storage mode.
const actuallyIDB = idbAvailable && db._idb != null
if (!actuallyIDB && idbAvailable) {
  badge.textContent = 'Memory + localStorage'
  badge.className = 'mem'
  log('IDB worker unavailable — using in-memory + localStorage fallback', 'peer')
}
log(`database opened (${actuallyIDB ? 'IDB mode' : 'memory mode'})`)
setStatus('connecting to peers…')

// ─────────────────────────────────────────────────────────────────────────────
// Step 3 — define secondary indexes
//
// index(name, keyFn) scans the current _store and builds an in-memory
// bucket map: indexedValue → Set<primaryKey>.
// Must be called *after* open() so the store is hydrated.
//
// Secondary indexes survive between steps — they are rebuilt from the
// persisted records each time the page loads.
// ─────────────────────────────────────────────────────────────────────────────
db.index('tag',    r => r.tag)     // query by tag
db.index('author', r => r.author)  // query by author peer ID

log(`indexes built — ${db.size} existing records loaded`)

// ─────────────────────────────────────────────────────────────────────────────
// Step 4 — P2P node events
// ─────────────────────────────────────────────────────────────────────────────
node.on('peer:connect', peerId => {
  const label = RELAY_PEER_IDS.has(peerId) ? 'relay' : 'peer'
  log(`→ ${label} connected   ${peerId.slice(0, 20)}…`, 'peer')
  renderPeers()
  setStatus(`connected — ${node.peers.length} peers`)
  updateRelayAddr()
})

node.on('peer:disconnect', peerId => {
  log(`→ peer disconnected ${peerId.slice(0, 20)}…`, 'peer')
  renderPeers()
  updateRelayAddr()
})

node.on('self:update', updateRelayAddr)

function updateRelayAddr() {
  const relays = node.relayMultiaddrs
  console.debug('[relay] all multiaddrs:', node.multiaddrs)
  console.debug('[relay] circuit addrs:', relays)
  const el = $('relay-addr')
  if (relays.length) {
    // Show all circuit relay addresses (one per connected relay)
    el.textContent = relays.join('\n')
    el.title = `${relays.length} relay${relays.length > 1 ? 's' : ''} — click to copy first`
  } else {
    el.textContent = 'waiting for relay…'
    el.title = ''
  }
}

// Poll every 3 s as a fallback in case self:peer:update doesn't fire
setInterval(updateRelayAddr, 3000)

// Pre-fill relay input from ?relay= URL param (comma-separated).
// Only accept http/https URLs — reject libp2p multiaddrs (/ip4/…, /dns4/…).
const relayParam = new URLSearchParams(location.search).get('relay')
if (relayParam && relayParam.split(',').every(u => /^https?:\/\//.test(u.trim()))) {
  $('relay-url').value = relayParam
}

$('relay-connect-btn').addEventListener('click', async () => {
  const url = $('relay-url').value.trim()
  if (!url) return
  const parts = url.split(',').map(u => u.trim()).filter(Boolean)
  if (parts.some(u => !/^https?:\/\//.test(u))) {
    log('Each relay URL must start with http:// or https:// — e.g. http://192.168.1.160:4010', 'err')
    return
  }
  const btn = $('relay-connect-btn')
  btn.textContent = 'connecting…'
  btn.disabled = true
  const ok = await node.dialRelay(url)
  btn.textContent = ok ? 'connected ✓' : 'failed ✗'
  btn.disabled = false
  if (ok) {
    log(`relay connected: ${url}`, 'peer')
    // Update URL bar — keeps all relay URLs so sharing the link reconnects all
    const u = new URL(location.href)
    u.searchParams.set('relay', url)
    history.replaceState(null, '', u.toString())
  } else {
    log(`relay unreachable: ${url}`, 'err')
    setTimeout(() => { btn.textContent = 'Connect' }, 3000)
  }
})

// ─────────────────────────────────────────────────────────────────────────────
// Step 5 — db change events
//
// 'change' fires after *every* write, both local and remote.
// detail: { op, key, value?, remote?, from? }
//   remote:true  — came from a peer via GossipSub
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
// Step 6 — show own peer ID + initial render
// ─────────────────────────────────────────────────────────────────────────────
$('peer-id').textContent = node.peerId
log(`my peer ID: ${node.peerId.slice(0, 20)}…`)

render()
renderPeers()

$('relay-addr').addEventListener('click', () => {
  const addr = $('relay-addr').textContent
  if (!addr || addr.startsWith('waiting')) return
  navigator.clipboard.writeText(addr).then(() => log('relay addr copied to clipboard'))
})

// ─────────────────────────────────────────────────────────────────────────────
// Step 7 — write: db.set (add note)
//
// db.set flow with IDB mode:
//   1. _store.set(key, value)         — update read cache immediately (sync)
//   2. _syncIndexes(key, value)       — update bucket maps (sync)
//   3. idb.call('set', key, value)    — Worker: INSERT OR REPLACE INTO kv
//   4. emit 'change' event            — { op:'set', key, value, remote:false }
//   5. node.send({__type:'db:set',…}) — GossipSub broadcast to all mesh peers
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
    author: node.peerId,
    ts:     Date.now(),
    done:   false,
  }

  await db.set(key, value)
  $('note-input').value = ''
  render()
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 8 — delete: db.delete
// ─────────────────────────────────────────────────────────────────────────────
async function deleteNote(key) {
  if (editingKey === key) editingKey = null
  await db.delete(key)
  render()
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 9 — inline edit: db.set (update an existing record)
//
// Edit state is tracked by editingKey. renderNotes() renders the card
// with a <textarea> instead of a <div> when key === editingKey, so the
// entire UI is driven by state — no ad-hoc DOM mutation.
//
// saveEdit calls db.set() with the merged record, which broadcasts to peers
// just like a new note; remote peers see the body change applied via onSync.
// ─────────────────────────────────────────────────────────────────────────────
function startEdit(key) {
  editingKey = key
  renderNotes()  // only notes pane needs to re-render, not stats
}

function cancelEdit() {
  editingKey = null
  renderNotes()
}

async function saveEdit(key, newBody) {
  editingKey = null
  const value = db.get(key)
  if (!value) { renderNotes(); return }
  newBody = newBody.trim()
  if (newBody && newBody !== value.body) {
    // Spread to keep all existing fields; body is the only changed field
    await db.set(key, { ...value, body: newBody })
  }
  render()
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 10 — done toggle: db.set (update a boolean field)
//
// Toggling done re-uses the same db.set() path as adding — the full record is
// broadcast to peers so they apply the same { done: true/false } change.
// ─────────────────────────────────────────────────────────────────────────────
async function toggleDone(key) {
  const value = db.get(key)
  if (!value) return
  await db.set(key, { ...value, done: !value.done })
  render()
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 11 — search, sort, and filter controls
// ─────────────────────────────────────────────────────────────────────────────
$('search-input').addEventListener('input', e => {
  searchText = e.target.value.trim().toLowerCase()
  renderNotes()
})

$('sort-select').addEventListener('change', e => {
  sortOrder = e.target.value
  renderNotes()
})

$('filter-btn').addEventListener('click', () => {
  activeFilter = $('tag-select').value
  log(`filtering by tag: ${activeFilter}`)
  renderNotes()
})

$('filter-clear').addEventListener('click', () => {
  activeFilter = null
  renderNotes()
})

// ─────────────────────────────────────────────────────────────────────────────
// Step 12 — sync reads (always from _store, never touch IDB or SQLite)
//
// db.get(key)                    — O(1) Map lookup
// db.all()                       — full scan of _store as [{key, value}]
// db.query(indexName, value)     — O(1) bucket lookup → values
// db.find(predicate)             — full scan with predicate
// db.size                        — _store.size
// db.has(key)                    — Map.has
//
// All of these are synchronous and never block the event loop.
// ─────────────────────────────────────────────────────────────────────────────
function renderNotes() {
  const container = $('notes')
  container.innerHTML = ''

  // Start from full store — db.all() returns [{key, value}, …] in insertion order
  let records = db.all()

  // Tag filter — db.query('tag', activeFilter) gives O(1) bucket lookup for
  // large datasets; here we filter db.all() so we keep {key, value} pairs
  // without a secondary lookup step.
  if (activeFilter) {
    records = records.filter(({ value }) => value?.tag === activeFilter)
  }

  // Text search — db.find() equivalent applied after tag filter
  if (searchText) {
    records = records.filter(({ value }) =>
      value?.body?.toLowerCase().includes(searchText)
    )
  }

  // Sort
  if (sortOrder === 'oldest') {
    records.sort((a, b) => (a.value?.ts ?? 0) - (b.value?.ts ?? 0))
  } else {
    records.sort((a, b) => (b.value?.ts ?? 0) - (a.value?.ts ?? 0))
  }

  for (const { key, value } of records) {
    if (!value?.body) continue

    const isLocal   = value.author === node.peerId
    const isEditing = key === editingKey

    const card = document.createElement('div')
    card.className = `note-card${isLocal ? '' : ' remote'}${value.done ? ' done' : ''}`
    card.dataset.key = key

    if (isEditing) {
      // ── edit mode ──
      // Save on ✓ click, Ctrl+Enter, or blur; cancel on ✗ or Escape.
      card.innerHTML = `
        <div class="card-actions">
          <button class="save-btn"   title="save (Enter)">✓</button>
          <button class="cancel-btn" title="cancel (Esc)">✗</button>
          <button class="del-btn"    title="delete">×</button>
        </div>
        <div class="note-tag">${value.tag ?? '—'}</div>
        <textarea class="edit-textarea">${escHtml(value.body)}</textarea>
        <div class="note-meta">
          <span>${isLocal ? 'me' : value.author?.slice(0, 10) + '…'}</span>
          <span>${new Date(value.ts).toLocaleTimeString()}</span>
        </div>
      `
      const textarea = card.querySelector('.edit-textarea')
      const doSave   = () => saveEdit(key, textarea.value)

      card.querySelector('.save-btn').addEventListener('click', doSave)
      card.querySelector('.cancel-btn').addEventListener('click', cancelEdit)
      card.querySelector('.del-btn').addEventListener('click', () => deleteNote(key))
      textarea.addEventListener('keydown', e => {
        if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); doSave() }
        if (e.key === 'Escape') cancelEdit()
      })
      // Position cursor at the end after re-render
      requestAnimationFrame(() => {
        textarea.focus()
        textarea.setSelectionRange(textarea.value.length, textarea.value.length)
      })
    } else {
      // ── view mode ──
      card.innerHTML = `
        <div class="card-actions">
          <button class="done-btn${value.done ? ' active' : ''}"
                  title="${value.done ? 'unmark done' : 'mark done'}">✓</button>
          <button class="edit-btn" title="edit">✎</button>
          <button class="del-btn"  title="delete">×</button>
        </div>
        <div class="note-tag">${value.tag ?? '—'}</div>
        <div class="note-body">${escHtml(value.body)}</div>
        <div class="note-meta">
          <span>${isLocal ? 'me' : value.author?.slice(0, 10) + '…'}</span>
          <span>${new Date(value.ts).toLocaleTimeString()}</span>
        </div>
      `
      card.querySelector('.done-btn').addEventListener('click', () => toggleDone(key))
      card.querySelector('.edit-btn').addEventListener('click', () => startEdit(key))
      card.querySelector('.del-btn').addEventListener('click', () => deleteNote(key))
    }

    container.appendChild(card)
  }

  if (!records.length) {
    const msg = searchText
      ? `No notes match "${searchText}".`
      : activeFilter
        ? `No notes tagged '${activeFilter}'.`
        : 'No notes yet.'
    container.innerHTML = `<div style="color:#444;padding:20px;grid-column:1/-1">${msg}</div>`
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 13 — stats: db.size + db.all() aggregation
//
// Shows total note count, how many are done, and a breakdown per tag.
// db.size is O(1). db.all() is O(n) but runs synchronously from _store.
// ─────────────────────────────────────────────────────────────────────────────
function renderStats() {
  const all   = db.all()
  const total = all.length
  const done  = all.filter(({ value }) => value?.done).length
  const byTag = {}
  for (const { value } of all) {
    if (value?.tag) byTag[value.tag] = (byTag[value.tag] ?? 0) + 1
  }
  const tagLines = Object.entries(byTag)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([tag, n]) => `  ${tag}: ${n}`)
    .join('\n')

  $('stats-text').textContent = total === 0
    ? '—'
    : `${total} note${total !== 1 ? 's' : ''} (${done} done)\n${tagLines}`
}

// Convenience: render both panes together (notes + stats).
// Call render() whenever the store changes; call renderNotes() alone
// when only the view state (filter/sort/edit) changes.
function render() {
  renderNotes()
  renderStats()
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 14 — raw SQL via db.sql()
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
$('sql-input').addEventListener('keydown', e => { if (e.key === 'Enter') runSQL() })

// Quick-examples dropdown: selecting an option copies it to the input field
$('sql-example').addEventListener('change', e => {
  if (e.target.value) {
    $('sql-input').value = e.target.value
    e.target.value = ''  // reset so the same option can be re-selected
  }
})

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
// Step 15 — ephemeral P2P message (not persisted to DB)
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
// Step 16 — peer list rendering
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

  // Peers whose circuit relay addresses include this peer ID are acting as
  // a browser relay for us (they have circuitRelayServer running).
  const browserRelayIds = new Set(
    node.relayMultiaddrs
      .map(a => { const m = a.match(/\/p2p-circuit\/p2p\/([^/]+)\/p2p-circuit/); return m?.[1] })
      .filter(Boolean)
  )

  for (const peerId of node.peers) {
    const isServerRelay  = RELAY_PEER_IDS.has(peerId)
    const isBrowserRelay = browserRelayIds.has(peerId)
    const cls  = isServerRelay ? 'relay' : isBrowserRelay ? 'relay browser-relay' : 'app'
    const icon = isServerRelay ? '⟳' : isBrowserRelay ? '⟳' : '●'
    const lbl  = isServerRelay ? '(relay)' : isBrowserRelay ? '(browser relay)' : ''
    const el = document.createElement('div')
    el.className = `peer-item ${cls}`
    el.title = peerId
    el.textContent = `${icon} ${peerId.slice(0, 22)}… ${lbl}`
    list.appendChild(el)
  }

  $('peer-count').textContent = node.peers.length
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 17 — clean shutdown
//
// db.close() in IDB mode:
//   1. idb.call('close') → Worker sends 'PRAGMA wal_checkpoint(TRUNCATE)'
//      then sqlite3.close(db) — flushes all WAL pages to IDB before exit
//   2. idb.terminate() — terminates the Worker
//
// db.close() in memory mode:
//   sqlite3.close(db) — releases WASM memory
//
// node.stop() — closes all libp2p connections, stops listeners and discovery
// ─────────────────────────────────────────────────────────────────────────────
window.addEventListener('beforeunload', () => {
  // beforeunload must be synchronous; fire-and-forget is best we can do here.
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
