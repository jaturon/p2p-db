// Main-thread proxy for db-worker.js.
// Each call() sends a { id, method, args } message and waits for the matching
// { id, result | error } reply. Concurrent calls are safe — each gets its own id.

export class IDBStorage {
  /**
   * Returns true when the environment can run IDBBatchAtomicVFS.
   * Requires SharedArrayBuffer (needs COOP + COEP HTTP headers) and Atomics.
   */
  static isAvailable() {
    // wa-sqlite ≥1.0 uses Web Locks (not SharedArrayBuffer/Atomics) for the
    // IDB VFS bridge, so the only hard requirement is navigator.locks.
    return typeof navigator !== 'undefined'
      && typeof navigator.locks !== 'undefined'
  }

  /**
   * @param {string | URL} workerUrl  Path or URL to db-worker.js.
   *   Use the exported DB_WORKER_URL constant from db.js to avoid hard-coding:
   *     import { DB_WORKER_URL } from 'p2p-db/db'
   *     new IDBStorage(DB_WORKER_URL)
   */
  constructor(workerUrl) {
    const url = workerUrl instanceof URL
      ? workerUrl
      : new URL(workerUrl, import.meta.url)

    this._worker = new Worker(url, { type: 'module' })
    this._pending = new Map()
    this._seq = 0

    this._worker.addEventListener('message', ({ data: { id, result, error } }) => {
      const p = this._pending.get(id)
      if (!p) return
      this._pending.delete(id)
      error ? p.reject(new Error(error)) : p.resolve(result)
    })

    // If the Worker itself crashes, reject every in-flight call
    this._worker.addEventListener('error', (evt) => {
      const err = new Error(`db-worker error: ${evt.message ?? 'unknown'}`)
      for (const [, p] of this._pending) p.reject(err)
      this._pending.clear()
    })
  }

  call(method, ...args) {
    return new Promise((resolve, reject) => {
      const id = this._seq++
      this._pending.set(id, { resolve, reject })
      this._worker.postMessage({ id, method, args })
    })
  }

  // Send close message (flushes WAL checkpoint), then terminate the Worker.
  async terminate() {
    try { await this.call('close') } catch {}
    this._worker.terminate()
  }
}
