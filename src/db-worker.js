// Web Worker — owns the SQLite engine and IDBBatchAtomicVFS.
// Must run in a Worker: IDBBatchAtomicVFS bridges async IndexedDB operations
// to SQLite's synchronous VFS interface via Atomics.wait(), which is only
// permitted off the main thread.
//
// Message protocol (both directions):
//   request:  { id: number, method: string, args: any[] }
//   response: { id: number, result: any }  |  { id: number, error: string }

// Importmaps don't apply inside Workers — use full CDN URLs here.
import SQLiteESMFactory from 'https://esm.sh/wa-sqlite/dist/wa-sqlite-async.mjs'
import * as SQLite from 'https://esm.sh/wa-sqlite'
import { IDBBatchAtomicVFS } from 'https://esm.sh/wa-sqlite/src/examples/IDBBatchAtomicVFS.js'

let sqlite3 = null
let db = null

async function open(name) {
  const module = await SQLiteESMFactory()
  sqlite3 = SQLite.Factory(module)

  // IDBBatchAtomicVFS stores SQLite page files in IndexedDB.
  // durability:'relaxed' — batches writes before flushing to IDB.
  //   Trades a tiny loss window (uncommitted batch on sudden crash) for speed.
  // durability:'strict'  — flushes on every SQLite commit; fully safe, slower.
  // wa-sqlite ≥1.0 dropped the static create() factory; plain constructor,
  // no module arg (Web Locks replaced the SharedArrayBuffer/Atomics bridge).
  const vfs = new IDBBatchAtomicVFS(name, { durability: 'relaxed' })
  sqlite3.vfs_register(vfs, true)

  db = await sqlite3.open_v2(name)

  // WAL mode: readers never block writers; pages accumulate in the WAL file
  // and are checkpointed into the main IDB store on close or when WAL grows large.
  // IDBBatchAtomicVFS uses SQLITE_IOCAP_BATCH_ATOMIC for durability — WAL mode
  // is incompatible with batch atomic writes and must not be set here.
  await sqlite3.exec(db, `
    CREATE TABLE IF NOT EXISTS kv (
      key   TEXT PRIMARY KEY NOT NULL,
      value TEXT NOT NULL
    );
  `)
}

// wa-sqlite ≥1.0: prepare_v2() requires a C-string pointer; use statements()
// which handles str_new/str_value internally and auto-finalizes each stmt.
async function exec(sql, params = []) {
  const rows = []
  for await (const stmt of sqlite3.statements(db, sql)) {
    if (params.length) sqlite3.bind_collection(stmt, params)
    while (await sqlite3.step(stmt) === SQLite.SQLITE_ROW) {
      rows.push(sqlite3.row(stmt))
    }
  }
  return rows
}

// rows as [{colName: val}, …]
async function execObj(sql, params = []) {
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

// first row as {colName: val} or null
async function execFirst(sql, params = []) {
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

self.addEventListener('message', async ({ data: { id, method, args } }) => {
  try {
    let result

    switch (method) {
      case 'open':
        await open(args[0])
        result = null
        break

      // Returns [[key, parsedValue], …] for _store hydration on the main thread
      case 'load':
        result = (await exec('SELECT key, value FROM kv'))
          .map(([k, v]) => [k, JSON.parse(v)])
        break

      case 'set':
        await exec(
          'INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)',
          [args[0], JSON.stringify(args[1])]
        )
        result = null
        break

      case 'delete':
        await exec('DELETE FROM kv WHERE key = ?', [args[0]])
        result = null
        break

      // Bulk set — args[0] = [[key, value], …]
      case 'setMany':
        await exec('BEGIN')
        try {
          for (const [k, v] of args[0])
            await exec('INSERT OR REPLACE INTO kv (key, value) VALUES (?, ?)', [k, JSON.stringify(v)])
          await exec('COMMIT')
        } catch (e) { await exec('ROLLBACK'); throw e }
        result = null
        break

      // Bulk delete — args[0] = [key, …]
      case 'deleteMany':
        await exec('BEGIN')
        try {
          for (const k of args[0]) await exec('DELETE FROM kv WHERE key = ?', [k])
          await exec('COMMIT')
        } catch (e) { await exec('ROLLBACK'); throw e }
        result = null
        break

      case 'clear':
        await exec('DELETE FROM kv')
        result = null
        break

      // Raw SQL passthrough — args[0]=query, args[1]=params[]
      case 'sql':
        result = await exec(args[0], args[1] ?? [])
        break

      // Named-column variants
      case 'sqlAll':
        result = await execObj(args[0], args[1] ?? [])
        break

      case 'sqlGet':
        result = await execFirst(args[0], args[1] ?? [])
        break

      case 'sqlValue': {
        const rows = await exec(args[0], args[1] ?? [])
        result = rows.length ? rows[0][0] : null
        break
      }

      case 'sqlRun':
        await exec(args[0], args[1] ?? [])
        result = {
          changes:          (await exec('SELECT changes()'))[0]?.[0] ?? 0,
          lastInsertRowId:  (await exec('SELECT last_insert_rowid()'))[0]?.[0] ?? 0,
        }
        break

      // Flush WAL checkpoint to IDB before the Worker is terminated
      case 'close':
        if (db !== null) {
          await sqlite3.exec(db, 'PRAGMA wal_checkpoint(TRUNCATE)')
          await sqlite3.close(db)
          db = null
        }
        result = null
        break
    }

    self.postMessage({ id, result })
  } catch (err) {
    self.postMessage({ id, error: err.message })
  }
})
