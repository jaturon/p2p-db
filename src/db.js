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
// In bundled (iife) builds import.meta.url is empty, which would make
// `new URL('./db-worker.js', '')` throw at module-init time — fall back to
// a plain relative path string in that case (resolved by standalone.js).
export const DB_WORKER_URL = (() => {
  try { return new URL('./db-worker.js', import.meta.url) }
  catch { return './db-worker.js' }
})()

// Base URL used to locate wa-sqlite.wasm (Mode A). Defaults to
// import.meta.url (same directory as this module) for plain ESM usage.
// The standalone bundle overrides this via setWasmBaseUrl() since
// import.meta.url is empty in iife output.
let _wasmBaseUrl = import.meta.url || null

export function setWasmBaseUrl(url) {
  _wasmBaseUrl = url
}

function sqliteFactoryOptions() {
  if (!_wasmBaseUrl) return undefined
  return { locateFile: (path) => new URL(path, _wasmBaseUrl).href }
}

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

// Returns rows as [{colName: value, …}, …] using column_names()
async function sqAllObj(sqlite3, db, sql, params = []) {
  const rows = []
  for await (const stmt of sqlite3.statements(db, sql)) {
    if (params.length) sqlite3.bind_collection(stmt, params)
    let cols = null
    while (await sqlite3.step(stmt) === SQLite.SQLITE_ROW) {
      if (!cols) cols = sqlite3.column_names(stmt)
      const row = sqlite3.row(stmt)
      const obj = {}
      for (let i = 0; i < cols.length; i++) obj[cols[i]] = row[i]
      rows.push(obj)
    }
  }
  return rows
}

// Returns first row as {colName: value, …} or null
async function sqFirst(sqlite3, db, sql, params = []) {
  for await (const stmt of sqlite3.statements(db, sql)) {
    if (params.length) sqlite3.bind_collection(stmt, params)
    if (await sqlite3.step(stmt) === SQLite.SQLITE_ROW) {
      const cols = sqlite3.column_names(stmt)
      const row  = sqlite3.row(stmt)
      const obj  = {}
      for (let i = 0; i < cols.length; i++) obj[cols[i]] = row[i]
      return obj
    }
  }
  return null
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

  // ── SQLite query helpers ──────────────────────────────────────────────────────
  // kv table schema:  key TEXT PRIMARY KEY,  value TEXT (JSON)
  // json_extract(value, '$.field')  for field-level access in SQL.

  /**
   * sql(query, params?) → [[col,…],…]
   * Raw SQL — returns rows as arrays of column values.
   */
  async sql(query, params = []) {
    if (this._idb) return this._idb.call('sql', query, params)
    if (!this._db) return null
    return this._sqOp(() => sqAll(this._sqlite3, this._db, query, params))
  }

  /**
   * sqlGet(query, params?) → {col: val, …} | null
   * Returns the first row as a named-column object, or null if no rows.
   */
  async sqlGet(query, params = []) {
    if (this._idb) return this._idb.call('sqlGet', query, params)
    if (!this._db) return null
    return this._sqOp(() => sqFirst(this._sqlite3, this._db, query, params))
  }

  /**
   * sqlAll(query, params?) → [{col: val, …}, …]
   * Like sql() but rows are objects keyed by column name.
   */
  async sqlAll(query, params = []) {
    if (this._idb) return this._idb.call('sqlAll', query, params)
    if (!this._db) return null
    return this._sqOp(() => sqAllObj(this._sqlite3, this._db, query, params))
  }

  /**
   * sqlValue(query, params?) → scalar | null
   * Returns the first column of the first row — handy for COUNT(*), MAX, etc.
   */
  async sqlValue(query, params = []) {
    if (this._idb) return this._idb.call('sqlValue', query, params)
    if (!this._db) return null
    return this._sqOp(async () => {
      const rows = await sqAll(this._sqlite3, this._db, query, params)
      return rows.length ? rows[0][0] : null
    })
  }

  /**
   * sqlRun(query, params?) → { changes: n, lastInsertRowId: n }
   * Execute a statement that modifies data (INSERT / UPDATE / DELETE).
   */
  async sqlRun(query, params = []) {
    if (this._idb) return this._idb.call('sqlRun', query, params)
    if (!this._db) return null
    return this._sqOp(async () => {
      await sqRun(this._sqlite3, this._db, query, params)
      const [[changes]]      = await sqAll(this._sqlite3, this._db, 'SELECT changes()')
      const [[lastId]]       = await sqAll(this._sqlite3, this._db, 'SELECT last_insert_rowid()')
      return { changes, lastInsertRowId: lastId }
    })
  }

  /**
   * sqlTransaction(fn) → result of fn()
   * Run a callback inside a BEGIN / COMMIT block.
   * fn receives this db instance.  Rolls back automatically on error.
   *
   * Note: in IDB mode each individual write goes to the worker; the
   * BEGIN/COMMIT are still sent but the IDB durability boundary is the
   * WAL flush, not the SQLite transaction.
   */
  async sqlTransaction(fn) {
    if (this._idb) {
      await this._idb.call('sql', 'BEGIN', [])
      try {
        const result = await fn(this)
        await this._idb.call('sql', 'COMMIT', [])
        return result
      } catch (err) {
        await this._idb.call('sql', 'ROLLBACK', []).catch(() => {})
        throw err
      }
    }
    if (!this._db) throw new Error('SQLite not available')
    return this._sqOp(async () => {
      await sqRun(this._sqlite3, this._db, 'BEGIN')
      try {
        const result = await fn(this)
        await sqRun(this._sqlite3, this._db, 'COMMIT')
        return result
      } catch (err) {
        await sqRun(this._sqlite3, this._db, 'ROLLBACK').catch(() => {})
        throw err
      }
    })
  }

  /**
   * pragma(name, value?) → current value
   * Get or set a SQLite PRAGMA.  e.g. db.pragma('journal_mode') → 'memory'
   */
  async pragma(name, value) {
    const q = value !== undefined ? `PRAGMA ${name} = ${value}` : `PRAGMA ${name}`
    const rows = await this.sql(q)
    return rows?.length ? rows[0][0] : null
  }

  /**
   * tables() → string[]
   * All table names in the database.
   * For p2p-db there is always at least 'kv'; you may have added others.
   */
  async tables() {
    const rows = await this.sql(
      "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    return (rows ?? []).map(r => r[0])
  }

  /**
   * schema(tableName?) → string | { [table]: string }
   * Returns the CREATE TABLE SQL for one table, or an object mapping every
   * table name to its CREATE TABLE SQL when called with no argument.
   *
   *   await db.schema()         // { kv: 'CREATE TABLE kv (…)' }
   *   await db.schema('kv')     // 'CREATE TABLE kv (key TEXT PRIMARY KEY …)'
   */
  async schema(tableName) {
    const rows = await this.sql(
      `SELECT name, sql FROM sqlite_master WHERE type='table'${
        tableName ? ' AND name=?' : ' ORDER BY name'
      }`,
      tableName ? [tableName] : []
    )
    if (!rows?.length) return tableName ? null : {}
    if (tableName) return rows[0][1]
    return Object.fromEntries(rows.map(([n, s]) => [n, s]))
  }

  on(event, handler) {
    this._emitter.addEventListener(event, e => handler(e.detail))
    return this
  }

  // ── Extended read API ─────────────────────────────────────────────────────────

  /** All primary keys in insertion order. */
  keys() { return [...this._store.keys()] }

  /** All values in insertion order. */
  values() { return [...this._store.values()] }

  /** Count records, optionally matching predicate(value, key). */
  count(predicate) {
    if (!predicate) return this._store.size
    let n = 0
    for (const [key, value] of this._store) if (predicate(value, key)) n++
    return n
  }

  /** First record (optionally matching predicate). Returns {key,value} or undefined. */
  first(predicate) {
    for (const [key, value] of this._store)
      if (!predicate || predicate(value, key)) return { key, value }
    return undefined
  }

  /** Last record (optionally matching predicate). Returns {key,value} or undefined. */
  last(predicate) {
    let result
    for (const [key, value] of this._store)
      if (!predicate || predicate(value, key)) result = { key, value }
    return result
  }

  /**
   * Get multiple records by key. Returns a plain object { key: value, … }.
   * Missing keys are omitted.
   */
  getMany(keys) {
    const out = {}
    for (const k of keys) {
      const v = this._store.get(k)
      if (v !== undefined) out[k] = v
    }
    return out
  }

  // ── Index extensions ──────────────────────────────────────────────────────────

  /**
   * Like query() but returns [{key, value}, …] instead of values only.
   * Fixes the key-recovery problem in the original example code.
   */
  queryEntries(indexName, indexValue) {
    const idx = this._indexes.get(indexName)
    if (!idx) return []
    const keys = idx.map.get(indexValue)
    if (!keys) return []
    return [...keys]
      .map(k => ({ key: k, value: this._store.get(k) }))
      .filter(e => e.value !== undefined)
  }

  /** Like query() but returns only the primary keys. */
  queryKeys(indexName, indexValue) {
    const idx = this._indexes.get(indexName)
    if (!idx) return []
    return [...(idx.map.get(indexValue) ?? [])]
  }

  /**
   * All unique index values currently in the bucket map.
   * e.g. db.indexValues('tag') → ['general', 'work', 'idea']
   */
  indexValues(indexName) {
    const idx = this._indexes.get(indexName)
    return idx ? [...idx.map.keys()] : []
  }

  // ── Bulk write API ────────────────────────────────────────────────────────────

  /**
   * Set multiple records in one call.
   * In IDB mode the writes are issued as a batch; in Mode A they go through
   * the serial _sqlQ so no concurrent-tmpPtr crashes occur.
   *
   * @param {Array<[key, value]> | Iterable<[key, value]>} entries
   */
  async setMany(entries) {
    const pairs = [...entries]
    for (const [key, value] of pairs) {
      this._store.set(key, value)
      this._syncIndexes(key, value)
    }
    if (this._idb) {
      try {
        await this._idb.call('setMany', pairs)
      } catch (err) {
        await this._fallbackToMemory(err.message)
        if (this._db) for (const [k, v] of pairs)
          await this._sqOp(() => sqRun(this._sqlite3, this._db,
            'INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)', [k, JSON.stringify(v)]))
        this._lsPersist()
      }
    } else {
      if (this._db) for (const [k, v] of pairs)
        await this._sqOp(() => sqRun(this._sqlite3, this._db,
          'INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)', [k, JSON.stringify(v)]))
      this._lsPersist()
    }
    for (const [key, value] of pairs) this._emit('change', { op: 'set', key, value })
    return this
  }

  /**
   * Delete multiple records in one call.
   * @param {string[]} keys
   */
  async deleteMany(keys) {
    const present = keys.filter(k => this._store.has(k))
    for (const k of present) { this._store.delete(k); this._purgeIndexes(k) }
    if (this._idb) {
      try { await this._idb.call('deleteMany', present) }
      catch (err) { await this._fallbackToMemory(err.message) }
    } else {
      if (this._db) for (const k of present)
        await this._sqOp(() => sqRun(this._sqlite3, this._db, 'DELETE FROM kv WHERE key = ?', [k]))
      this._lsPersist()
    }
    for (const k of present) this._emit('change', { op: 'delete', key: k })
    return this
  }

  // ── Atomic update ─────────────────────────────────────────────────────────────

  /**
   * Atomic read-modify-write.
   * updater(currentValue) → newValue  — if newValue is undefined, the key is deleted.
   *
   * @param {string} key
   * @param {function} updater
   */
  async update(key, updater) {
    const next = updater(this._store.get(key))
    return next === undefined ? this.delete(key) : this.set(key, next)
  }

  // ── Subscription ──────────────────────────────────────────────────────────────

  /**
   * Watch a single key for changes.
   * Returns an unsubscribe function.
   *
   * @param {string} key
   * @param {function} handler  ({ op, value, remote, from }) => void
   */
  watch(key, handler) {
    const fn = e => { if (e.detail.key === key) handler(e.detail) }
    this._emitter.addEventListener('change', fn)
    return () => this._emitter.removeEventListener('change', fn)
  }

  // ── Export / Import ───────────────────────────────────────────────────────────

  /**
   * Export the full store as a plain object { key: value, … }.
   */
  export() {
    const out = {}
    for (const [k, v] of this._store) out[k] = v
    return out
  }

  /**
   * Bulk-import records.
   * @param {Object|Array<[key,value]>} data  plain object or [[key,value],…]
   * @param {{ clear?: boolean }} opts  clear=true wipes existing data first
   */
  async import(data, { clear = false } = {}) {
    if (clear) await this.clear()
    const entries = Array.isArray(data) ? data : Object.entries(data)
    return this.setMany(entries)
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
      const module = await SQLiteESMFactory(sqliteFactoryOptions())
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

  // Switch from IDB mode to memory+localStorage when the IDB worker crashes
  // (e.g. WASM abort from a corrupted database file).  Bulk-loads _store into
  // a fresh in-memory SQLite so db.sql() keeps working, then persists to localStorage.
  async _fallbackToMemory(reason) {
    console.warn('[DBIndex] IDB worker failed, falling back to memory mode:', reason)
    try { await this._idb?.terminate() } catch {}
    this._idb = null
    if (!this._db) {
      try {
        const module = await SQLiteESMFactory(sqliteFactoryOptions())
        this._sqlite3 = SQLite.Factory(module)
        this._db = await this._sqlite3.open_v2(':memory:')
        await this._sqlite3.exec(this._db, `
          CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL)
        `)
        for (const [k, v] of this._store) {
          await sqRun(this._sqlite3, this._db,
            'INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)',
            [k, JSON.stringify(v)])
        }
      } catch (e) {
        console.warn('[DBIndex] memory SQLite also failed:', e)
      }
    }
    this._lsPersist()
  }

  async _applySet(key, value, remote = false, from = null) {
    this._store.set(key, value)
    this._syncIndexes(key, value)

    if (this._idb) {
      try {
        await this._idb.call('set', key, value)
      } catch (err) {
        await this._fallbackToMemory(err.message)
        if (this._db) {
          await this._sqOp(() => sqRun(this._sqlite3, this._db,
            'INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)',
            [key, JSON.stringify(value)]))
        }
        this._lsPersist()
      }
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
      try {
        await this._idb.call('delete', key)
      } catch (err) {
        await this._fallbackToMemory(err.message)
      }
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
