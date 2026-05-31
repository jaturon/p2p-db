export { DBIndex, DB_WORKER_URL } from './db.js'
export { IDBStorage } from './idb-storage.js'
export { P2PNode, BOOTSTRAP_LIST } from './p2p.js'

/**
 * createP2PDB — open a DBIndex and wire it to a GossipSub P2P node so that
 * every set/delete is automatically broadcast to all peers on the same topic.
 *
 * Storage backend is selected by whether workerUrl is supplied:
 *   No workerUrl → Mode A (in-memory SQLite + localStorage, default)
 *   workerUrl    → Mode B (IDBBatchAtomicVFS via Worker, durable IndexedDB)
 *
 * @param {object}     opts
 * @param {string}     [opts.name='p2p-db']      DBIndex store name
 * @param {string}     [opts.topic='p2p-db']     GossipSub topic
 * @param {string|URL} [opts.workerUrl]           Enable Mode B; pass DB_WORKER_URL
 * @param {string[]}   [opts.bootstrapList]       Override bootstrap peer list
 * @param {function}   [opts.onMessage]           Non-db GossipSub messages: (from, data)
 * @param {function}   [opts.onSync]              After remote db op applied: (op, key, value?)
 *
 * @returns {Promise<{ db: DBIndex, node: P2PNode }>}
 */
export async function createP2PDB(opts = {}) {
  const { DBIndex } = await import('./db.js')
  const { P2PNode } = await import('./p2p.js')

  const { name = 'p2p-db', topic = 'p2p-db', workerUrl, onMessage, onSync, ...p2pOpts } = opts

  // workerUrl is forwarded to DBIndex.open() to select Mode A or Mode B
  const db = await DBIndex.open(name, { workerUrl })

  const node = await P2PNode.create({
    topic,
    ...p2pOpts,
    onMessage: async (from, msg) => {
      if (msg?.__type === 'db:set') {
        // _applySet writes to the backend + updates _store + emits change{remote:true}
        // Called directly (not through the patched db.set) so it won't re-broadcast
        await db._applySet(msg.key, msg.value, true, from)
        onSync?.('set', msg.key, msg.value)
      } else if (msg?.__type === 'db:delete') {
        await db._applyDelete(msg.key, true, from)
        onSync?.('delete', msg.key)
      } else if (msg?.__type === 'db:sync-request') {
        // A newly joined peer asked for our full state. Broadcast everything we have.
        for (const { key, value } of db.all()) {
          node.send({ __type: 'db:set', key, value }).catch(() => {})
        }
      } else {
        onMessage?.(from, msg)
      }
    },
  })

  // Patch set/delete: write locally first, then broadcast
  const _applySet = db._applySet.bind(db)
  const _applyDelete = db._applyDelete.bind(db)

  db.set = async (key, value) => {
    await _applySet(key, value)
    node.send({ __type: 'db:set', key, value }).catch(() => {})
    return db
  }

  db.delete = async (key) => {
    await _applyDelete(key)
    node.send({ __type: 'db:delete', key }).catch(() => {})
    return db
  }

  // Broadcast a db:sync-request so peers with existing state will replay
  // their records to us.  Two triggers:
  //
  // 1. Startup timer (2 s) — handles the initial relay connection which fires
  //    peer:connect *inside* P2PNode.create(), before our listener is attached.
  //
  // 2. peer:connect listener — handles peers that connect *after* startup
  //    (e.g. a second browser opening later). Throttled to once per 10 s.
  const sendSyncRequest = () =>
    setTimeout(() => node.send({ __type: 'db:sync-request' }).catch(() => {}), 1500)

  setTimeout(() => node.send({ __type: 'db:sync-request' }).catch(() => {}), 2000)

  let lastSyncRequest = Date.now()
  node.on('peer:connect', () => {
    const now = Date.now()
    if (now - lastSyncRequest < 10_000) return
    lastSyncRequest = now
    sendSyncRequest()
  })

  return { db, node }
}
