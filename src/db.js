// DBIndex — key-value store with named secondary indexes.
//
// Two storage backends, selected at open() time:
//
//   Mode A — in-memory SQLite + localStorage  (default)
//     _store (Map)    sync read cache
//     SQLite (wasm)   in-memory engine; enables db.sql() on main thread
//     localStorage    JSON snapshot written on every mutation (~5 MB limit)
//     No special setup needed.
//
//   Mode B — IDBBatchAtomicVFS via Worker  (pass workerUrl to open())
//     _store (Map)    sync read cache
//     Worker          owns the SQLite engine + IDBBatchAtomicVFS
//     IndexedDB       SQLite page store, ~1 GB+, survives hard reload
//     Requires: Cross-Origin-Opener-Policy: same-origin
//               Cross-Origin-Embedder-Policy: require-corp
//               (needed for SharedArrayBuffer / Atomics.wait in the Worker)
//
// Public API surface is identical in both modes.
// set / delete / clear / close are async. All reads are sync (from _store).

import SQLiteESMFactory from 'wa-sqlite/dist/wa-sqlite.mjs'
import * as SQLite from 'wa-sqlite'
import { IDBStorage } from './idb-storage.js'

// Convenience URL for the bundled worker — avoids hard-coding paths.
// Works in non-bundled ESM. For Vite/webpack use the ?worker or url import.
export const DB_WORKER_URL = new URL('./db-worker.js', import.meta.url)

// ── in-memory SQLite helpers (Mode A only) ────────────────────────────────────

// wa-sqlite ≥1.0 changed prepare_v2() to require a C-string pointer; JS strings
// must go through statements() which handles the str_new/str_value dance internally.
async function sqRun(sqlite3, db, sql, params = []) {
  for await (const stmt of sqlite3.statements(db, sql)) {
    if (params.length) sqlite3.bind_collection(stmt, params)
    await sqlite3.step(stmt)
  }
}

async function sqAll(sqlite3, db, sql, params = []) {
  const rows = []
  for await (const stmt of sqlite3.statements(db, sql)) {
    if (params.length) sqlite3.bind_collection(stmt, params)
    while (await sqlite3.step(stmt) === SQLite.SQLITE_ROW) {
      rows.push(sqlite3.row(stmt))
    }
  }
  return rows
}

// ── DBIndex ───────────────────────────────────────────────────────────────────

export class DBIndex {
  constructor(name) {
    this.name = name
    this._store = new Map()
    this._indexes = new Map()
    this._keyIndexValues = new Map() // key -> Map<indexName, indexedValue>
    this._emitter = new EventTarget()
    // Mode A handles
    this._sqlite3 = null
    this._db = null
    // Mode B handle — set by _initIDB(), signals IDB path throughout
    this._idb = null
    // Serial queue for Mode A SQLite calls.
    // wa-sqlite's prepare_v2 uses a shared tmpPtr buffer; concurrent awaited
    // calls race and corrupt each other's statement handles (SQLITE_MISUSE /
    // WASM memory-out-of-bounds). All Mode A writes go through this queue so
    // only one SQLite statement is alive at a time.
    this._sqlQ = Promise.resolve()
  }

  /**
   * Open a named store.
   *
   * @param {string} name        Store name (localStorage key prefix in Mode A;
   *                             IndexedDB database name in Mode B).
   * @param {object} [opts]
   * @param {string|URL} [opts.workerUrl]  Path/URL to db-worker.js. When provided,
   *   enables Mode B (IDBBatchAtomicVFS). Use the exported DB_WORKER_URL constant:
   *     DBIndex.open('myapp', { workerUrl: DB_WORKER_URL })
   *   Check IDBStorage.isAvailable() first to verify SharedArrayBuffer support.
   */
  static async open(name = 'dbindex', { workerUrl } = {}) {
    const idx = new DBIndex(name)
    if (workerUrl) {
      await idx._initIDB(workerUrl)
    } else {
      await idx._initMemory()
    }
    return idx
  }

  get size() { return this._store.size }

  async set(key, value) {
    await this._applySet(key, value)
    return this
  }

  get(key) { return this._store.get(key) }

  async delete(key) {
    await this._applyDelete(key)
    return this
  }

  has(key) { return this._store.has(key) }

  all() {
    return [...this._store.entries()].map(([key, value]) => ({ key, value }))
  }

  find(predicate) {
    const results = []
    for (const [key, value] of this._store) {
      if (predicate(value, key)) results.push({ key, value })
    }
    return results
  }

  /**
   * Define a named secondary index. keyFn maps a record to an index key.
   * Scans _store immediately to build the initial bucket map.
   * Re-call with the same name to rebuild after bulk changes.
   */
  index(name, keyFn) {
    const map = new Map()
    for (const [key, value] of this._store) {
      const idxVal = keyFn(value)
      if (idxVal == null) continue
      if (!map.has(idxVal)) map.set(idxVal, new Set())
      map.get(idxVal).add(key)
      this._trackKeyIndex(key, name, idxVal)
    }
    this._indexes.set(name, { fn: keyFn, map })
    return this
  }

  query(indexName, indexValue) {
    const idx = this._indexes.get(indexName)
    if (!idx) return []
    const keys = idx.map.get(indexValue)
    if (!keys) return []
    return [...keys].map(k => this._store.get(k)).filter(v => v !== undefined)
  }

  /**
   * Run a parameterized SQL query against the kv table.
   * Schema: kv(key TEXT PRIMARY KEY, value TEXT)  — value is JSON.
   * Use json_extract() for field-level filtering:
   *   db.sql("SELECT key FROM kv WHERE json_extract(value,'$.status')=?", ['unread'])
   *
   * Mode A: runs on main thread in-memory SQLite. Returns null if WASM unavailable.
   * Mode B: proxied to Worker, returns rows from IDB-backed SQLite.
   * Always returns: [[col0, col1, …], …]
   */
  async sql(query, params = []) {
    if (this._idb) return this._idb.call('sql', query, params)
    if (!this._db) return null
    return this._sqOp(() => sqAll(this._sqlite3, this._db, query, params))
  }

  on(event, handler) {
    this._emitter.addEventListener(event, e => handler(e.detail))
    return this
  }

  async clear() {
    this._store.clear()
    this._keyIndexValues.clear()
    for (const [, idx] of this._indexes) idx.map.clear()

    if (this._idb) {
      await this._idb.call('clear')
    } else {
      if (this._db) await this._sqOp(() => sqRun(this._sqlite3, this._db, 'DELETE FROM kv'))
      this._lsPersist()
    }

    this._emit('change', { op: 'clear' })
    return this
  }

  /**
   * Mode A: closes the in-memory SQLite connection.
   * Mode B: sends WAL checkpoint + close to Worker, then terminates it.
   */
  async close() {
    if (this._idb) {
      await this._idb.terminate()
      this._idb = null
    } else if (this._sqlite3 && this._db) {
      await this._sqlite3.close(this._db)
      this._db = null
    }
  }

  toJSON() { return [...this._store.entries()] }

  // ── init paths ────────────────────────────────────────────────────────────────

  async _initIDB(workerUrl) {
    // Firefox < 114 does not support { type:'module' } workers; the Worker
    // creation succeeds but the worker fires onerror, which rejects any
    // pending call.  Catch that and fall back to memory mode so the app
    // still initialises and GossipSub sync still works.
    try {
      this._idb = new IDBStorage(workerUrl)
      await this._idb.call('open', this.name)
      for (const [k, v] of await this._idb.call('load')) {
        this._store.set(k, v)
      }
    } catch (err) {
      console.warn('[DBIndex] IDB worker failed, falling back to memory mode:', err.message)
      try { this._idb?.terminate() } catch {}
      this._idb = null
      await this._initMemory()
    }
  }

  async _initMemory() {
    try {
      const module = await SQLiteESMFactory()
      this._sqlite3 = SQLite.Factory(module)
      this._db = await this._sqlite3.open_v2(':memory:')
      await this._sqlite3.exec(this._db, `
        CREATE TABLE IF NOT EXISTS kv (
          key   TEXT PRIMARY KEY NOT NULL,
          value TEXT NOT NULL
        );
      `)
    } catch (err) {
      console.warn('[DBIndex] wa-sqlite unavailable, using memory-only store', err)
    }

    // Hydrate from localStorage snapshot
    try {
      const raw = typeof localStorage !== 'undefined'
        ? localStorage.getItem(`dbindex:${this.name}`) : null
      if (!raw) return
      for (const [k, v] of JSON.parse(raw)) {
        this._store.set(k, v)
        if (this._db) {
          await sqRun(this._sqlite3, this._db,
            'INSERT OR IGNORE INTO kv (key, value) VALUES (?, ?)',
            [k, JSON.stringify(v)])
        }
      }
    } catch {}
  }

  // ── canonical write path ──────────────────────────────────────────────────────
  // Used by both the public API (set/delete) and the P2P sync layer (_applySet/
  // _applyDelete called directly from index.js to avoid re-broadcasting).

  // Serialize a Mode A SQLite call through _sqlQ so concurrent onMessage
  // handlers never overlap inside wa-sqlite's shared tmpPtr buffer.
  _sqOp(fn) {
    const next = this._sqlQ.then(fn)
    this._sqlQ = next.catch(() => {})  // keep queue alive on error
    return next
  }

  async _applySet(key, value, remote = false, from = null) {
    this._store.set(key, value)
    this._syncIndexes(key, value)

    if (this._idb) {
      await this._idb.call('set', key, value)
    } else {
      if (this._db) {
        await this._sqOp(() => sqRun(this._sqlite3, this._db,
          'INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)',
          [key, JSON.stringify(value)]))
      }
      this._lsPersist()
    }

    this._emit('change', { op: 'set', key, value, ...(remote && { remote: true, from }) })
  }

  async _applyDelete(key, remote = false, from = null) {
    const value = this._store.get(key)
    if (value === undefined) return

    this._store.delete(key)
    this._purgeIndexes(key)

    if (this._idb) {
      await this._idb.call('delete', key)
    } else {
      if (this._db) {
        await this._sqOp(() => sqRun(this._sqlite3, this._db,
          'DELETE FROM kv WHERE key = ?', [key]))
      }
      this._lsPersist()
    }

    this._emit('change', { op: 'delete', key, ...(remote && { remote: true, from }) })
  }

  // ── index internals ───────────────────────────────────────────────────────────

  _lsPersist() {
    try {
      if (typeof localStorage !== 'undefined') {
        localStorage.setItem(`dbindex:${this.name}`,
          JSON.stringify([...this._store.entries()]))
      }
    } catch {}
  }

  _trackKeyIndex(key, name, idxVal) {
    if (!this._keyIndexValues.has(key)) this._keyIndexValues.set(key, new Map())
    this._keyIndexValues.get(key).set(name, idxVal)
  }

  _syncIndexes(key, newValue) {
    for (const [name, idx] of this._indexes) {
      const prevVal = this._keyIndexValues.get(key)?.get(name)
      if (prevVal !== undefined) {
        const bucket = idx.map.get(prevVal)
        if (bucket) { bucket.delete(key); if (!bucket.size) idx.map.delete(prevVal) }
      }
      const newVal = idx.fn(newValue)
      if (newVal != null) {
        if (!idx.map.has(newVal)) idx.map.set(newVal, new Set())
        idx.map.get(newVal).add(key)
      }
      this._trackKeyIndex(key, name, newVal)
    }
  }

  _purgeIndexes(key) {
    for (const [name, idx] of this._indexes) {
      const idxVal = this._keyIndexValues.get(key)?.get(name)
      if (idxVal !== undefined) {
        const bucket = idx.map.get(idxVal)
        if (bucket) { bucket.delete(key); if (!bucket.size) idx.map.delete(idxVal) }
      }
    }
    this._keyIndexValues.delete(key)
  }

  _emit(event, detail) {
    this._emitter.dispatchEvent(new CustomEvent(event, { detail }))
  }
}
