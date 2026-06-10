// Standalone bundle entry point.
//
// Built by build.js into dist/p2p-db.js as a classic IIFE (global `P2PDB`)
// for plain <script src="p2p-db.js"> usage — no importmap, no module loader.
// Works when served from any static web server, and degrades gracefully
// (memory-only DB, no db.sql()) when opened directly as a file:// page.
export { DBIndex } from './db.js'
export { IDBStorage } from './idb-storage.js'
export { P2PNode, BOOTSTRAP_LIST } from './p2p.js'
export { createP2PDB, createP2PDB as connect } from './index.js'

import { setWasmBaseUrl } from './db.js'

// Both relays are peered with each other (PEER_RELAYS mesh) — connecting to
// either is enough to sync with the whole network.
export const DEFAULT_RELAYS = [
  'http://202.44.53.65:4010',
  'http://199.241.138.174:4010',
]
export const DEFAULT_RELAY = DEFAULT_RELAYS[0]

// document.currentScript.src is only valid synchronously while this classic
// script is executing — capture it now so wa-sqlite.wasm and db-worker.js
// can be located next to dist/p2p-db.js regardless of the page's own URL.
const _scriptUrl = (typeof document !== 'undefined' && document.currentScript?.src) || null

if (_scriptUrl) setWasmBaseUrl(_scriptUrl)

// Mode B (durable IndexedDB storage) worker — opt-in via:
//   createP2PDB({ workerUrl: P2PDB.DB_WORKER_URL })
// Requires http(s) serving (Worker + wasm fetch are blocked under file://).
export const DB_WORKER_URL = _scriptUrl
  ? new URL('./db-worker.js', _scriptUrl).href
  : './db-worker.js'
