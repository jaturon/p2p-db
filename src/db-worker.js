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
  const vfs = await IDBBatchAtomicVFS.create(name, module, { durability: 'relaxed' })
  sqlite3.vfs_register(vfs, true)

  db = await sqlite3.open_v2(name)

  // WAL mode: readers never block writers; pages accumulate in the WAL file
  // and are checkpointed into the main IDB store on close or when WAL grows large.
  await sqlite3.exec(db, `
    CREATE TABLE IF NOT EXISTS kv (
      key   TEXT PRIMARY KEY NOT NULL,
      value TEXT NOT NULL
    );
    PRAGMA journal_mode = WAL;
  `)
}

// Single parameterized statement — returns rows as arrays.
async function exec(sql, params = []) {
  const rows = []
  const prepared = await sqlite3.prepare_v2(db, sql)
  if (!prepared) return rows
  try {
    if (params.length) sqlite3.bind_collection(prepared.stmt, params)
    while (await sqlite3.step(prepared.stmt) === SQLite.SQLITE_ROW) {
      rows.push(sqlite3.row(prepared.stmt))
    }
  } finally {
    await sqlite3.finalize(prepared.stmt)
  }
  return rows
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

      case 'clear':
        await exec('DELETE FROM kv')
        result = null
        break

      // Raw SQL passthrough — args[0]=query, args[1]=params[]
      case 'sql':
        result = await exec(args[0], args[1] ?? [])
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
