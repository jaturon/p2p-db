// Plain classic script (no <script type="module">) — works when this page
// is served over http(s) AND when opened directly as a file:// page.
// Everything is provided by the global `P2PDB` from ../dist/p2p-db.js.

const $ = id => document.getElementById(id)

function log(msg, type = 'info') {
  const el = document.createElement('div')
  el.className = `log-line ${type}`
  el.textContent = `${new Date().toLocaleTimeString()}  ${msg}`
  $('log').prepend(el)
  while ($('log').children.length > 40) $('log').lastChild.remove()
}

function renderNotes(db) {
  const ul = $('notes')
  ul.innerHTML = ''
  for (const { key, value } of db.all().sort((a, b) => (a.value.ts ?? 0) - (b.value.ts ?? 0))) {
    const li = document.createElement('li')
    const span = document.createElement('span')
    span.textContent = value.body
    const del = document.createElement('button')
    del.textContent = '✕'
    del.onclick = () => db.delete(key)
    li.append(span, del)
    ul.appendChild(li)
  }
}

function renderPeers(node) {
  $('peer-count').textContent = node.peers.length
  $('peers').textContent = node.peers.map(p => p.slice(-6)).join(', ')
}

;(async () => {
  const { db, node } = await P2PDB.connect({
    name: 'p2p-db-standalone',
    // The public relays (202.44.53.65 / 199.241.138.174) only route the
    // 'p2p-db-notes-v1' topic. To use your own topic, run your own relay
    // (see ../relay/) with TOPICS=your-topic,_peer-discovery._p2p._pubsub
    topic: 'p2p-db-notes-v1',
    onSync(op, key) {
      log(`sync ${op} ${key}`, 'remote')
    },
  })

  $('peer-id').textContent = node.peerId
  $('storage-mode').textContent = db._db
    ? 'in-memory SQLite + localStorage (Mode A)'
    : 'localStorage only (wa-sqlite unavailable — normal under file://)'

  renderNotes(db)
  renderPeers(node)

  node.on('peer:connect', id => { log(`peer connect ${id}`, 'peer'); renderPeers(node) })
  node.on('peer:disconnect', id => { log(`peer disconnect ${id}`, 'peer'); renderPeers(node) })

  db.on('change', () => renderNotes(db))

  $('add-btn').onclick = async () => {
    const input = $('note-input')
    const text = input.value.trim()
    if (!text) return
    await db.set('note:' + Date.now(), { body: text, ts: Date.now() })
    input.value = ''
  }
  $('note-input').addEventListener('keydown', e => {
    if (e.key === 'Enter') $('add-btn').click()
  })

  $('relay-connect').onclick = async () => {
    const url = $('relay-url').value.trim()
    if (!url) return
    const ok = await node.dialRelay(url)
    log(ok ? `connected to relay ${url}` : `failed to connect ${url}`, ok ? 'peer' : 'err')
  }
})().catch(err => log(`init error: ${err.message}`, 'err'))
