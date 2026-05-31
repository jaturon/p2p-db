#!/usr/bin/env node
/**
 * p2p-db relay server
 *
 * Provides circuit relay v2 + GossipSub routing so browser peers
 * can discover and connect to each other without a central server.
 *
 * Two listeners:
 *   WS_PORT  (default 8080) — libp2p WebSocket, proxied as WSS by Fly.io
 *   API_PORT (default 3000) — HTTP /api/info and /healthz
 *
 * Environment variables:
 *   WS_PORT          Internal port for libp2p WebSocket (default 8080)
 *   API_PORT         Internal port for HTTP API        (default 3000)
 *   HOSTNAME         Public hostname for address announcement
 *                    e.g. my-relay.fly.dev
 *   WS_EXTERNAL_PORT External WSS port clients dial    (default 443)
 *   KEY_FILE         Path to persist the Ed25519 private key
 *                    (keeps peer ID stable across restarts)
 *                    Defaults to /data/relay.key when /data exists,
 *                    otherwise ./relay.key
 */

import { createLibp2p }           from 'libp2p'
import { webSockets }             from '@libp2p/websockets'
import { noise }                  from '@chainsafe/libp2p-noise'
import { yamux }                  from '@chainsafe/libp2p-yamux'
import { identify }               from '@libp2p/identify'
import { circuitRelayServer }     from '@libp2p/circuit-relay-v2'
import { gossipsub }              from '@chainsafe/libp2p-gossipsub'
import { pubsubPeerDiscovery }    from '@libp2p/pubsub-peer-discovery'
import { generateKeyPair,
         privateKeyToProtobuf,
         privateKeyFromProtobuf } from '@libp2p/crypto/keys'
import { createServer }           from 'node:http'
import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'node:fs'
import { dirname }                from 'node:path'
import process                    from 'node:process'

// ── Config ────────────────────────────────────────────────────────────────────

const WS_PORT          = parseInt(process.env.WS_PORT          ?? '8080')
const API_PORT         = parseInt(process.env.API_PORT         ?? '3000')
const HOSTNAME         = process.env.HOSTNAME                  ?? ''
const WS_EXTERNAL_PORT = parseInt(process.env.WS_EXTERNAL_PORT ?? '443')

// Comma-separated list of GossipSub topics to subscribe to.
// The relay joins these topics so it can route messages between browsers
// that are only connected to the relay and not directly to each other.
// Must include both the peer-discovery topic and any app topics.
const TOPICS = (process.env.TOPICS ?? 'p2p-db-notes-v1').split(',').map(t => t.trim()).filter(Boolean)

// Key file: prefer /data/ (Fly.io volume mount) if it exists
const KEY_FILE = process.env.KEY_FILE ??
  (existsSync('/data') ? '/data/relay.key' : './relay.key')

// ── Key persistence ───────────────────────────────────────────────────────────
// Stable peer ID means clients can cache the relay multiaddr.

async function loadOrCreateKey (path) {
  try {
    return privateKeyFromProtobuf(readFileSync(path))
  } catch {
    const key = await generateKeyPair('Ed25519')
    try {
      mkdirSync(dirname(path), { recursive: true })
      writeFileSync(path, privateKeyToProtobuf(key))
      console.log(`Generated new relay key → ${path}`)
    } catch (err) {
      console.warn(`Could not persist key to ${path}: ${err.message}`)
    }
    return key
  }
}

// ── Start ─────────────────────────────────────────────────────────────────────

const privateKey = await loadOrCreateKey(KEY_FILE)

// Announce the externally reachable WSS address so browser clients can
// dial us.  Fly.io terminates TLS; internally we speak plain WS.
const announceAddrs = HOSTNAME
  ? [`/dns4/${HOSTNAME}/tcp/${WS_EXTERNAL_PORT}/wss`]
  : []

const node = await createLibp2p({
  privateKey,
  addresses: {
    listen:   [`/ip4/0.0.0.0/tcp/${WS_PORT}/ws`],
    announce: announceAddrs,
  },
  transports: [webSockets()],
  connectionEncrypters: [noise()],
  streamMuxers: [yamux()],
  peerDiscovery: [
    // Subscribe to the peer-discovery GossipSub topic so the relay
    // participates in the mesh and can forward discovery messages between
    // browser peers that are only connected through this relay.
    pubsubPeerDiscovery({ interval: 5_000 }),
  ],

  services: {
    identify: identify(),

    // Circuit relay server — lets browser peers route connections through us
    relay: circuitRelayServer({
      reservations: {
        maxReservations: 256,
        // Reservations last 2 hours by default; increase for stability
        defaultDuration: 2 * 60 * 60 * 1000,
      },
    }),

    // GossipSub — the relay subscribes to both peer-discovery and app topics
    // so it can route messages between browsers that are only connected to
    // the relay and not directly to each other.  App topics are configured
    // via the TOPICS env var (default: p2p-db-notes-v1).
    pubsub: gossipsub({
      allowPublishToZeroTopicPeers: true,
      emitSelf: false,
      scoreThresholds: {
        gossipThreshold:             -Infinity,
        publishThreshold:            -Infinity,
        graylistThreshold:           -Infinity,
        acceptPXThreshold:           0,
        opportunisticGraftThreshold: 1,
      },
    }),
  },
})

await node.start()

// Subscribe to all configured topics so the relay can route messages
// between browsers that only share the relay as a mutual peer.
for (const topic of TOPICS) {
  node.services.pubsub.subscribe(topic)
  console.log('Subscribed to topic:', topic)
}

const peerId = node.peerId.toString()
console.log('\nRelay peer ID:', peerId)
console.log('Listening on:')
for (const ma of node.getMultiaddrs()) console.log(' ', ma.toString())
console.log()

// ── HTTP API ──────────────────────────────────────────────────────────────────
// /api/info  — JSON { peer_id, addrs }; browser fetches this to get the WS addr
// /healthz   — 200 OK for Fly.io health checks

const apiServer = createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*')

  if (req.url === '/api/info') {
    const addrs = node.getMultiaddrs().map(a => a.toString())
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(JSON.stringify({ peer_id: peerId, addrs }))
    return
  }

  if (req.url === '/healthz' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'text/plain' })
    res.end('ok')
    return
  }

  res.writeHead(404)
  res.end()
})

apiServer.listen(API_PORT, '0.0.0.0', () => {
  console.log(`API server on :${API_PORT}`)
})

// ── Logging ───────────────────────────────────────────────────────────────────

node.addEventListener('peer:connect',    e => console.log('[+]', e.detail.toString()))
node.addEventListener('peer:disconnect', e => console.log('[-]', e.detail.toString()))

setInterval(() => {
  const n = node.getPeers().length
  if (n > 0) console.log(`[peers] connected: ${n}`)
}, 60_000)

// ── Graceful shutdown ─────────────────────────────────────────────────────────

async function shutdown () {
  console.log('\nShutting down…')
  await node.stop()
  apiServer.close()
  process.exit(0)
}

process.on('SIGINT',  shutdown)
process.on('SIGTERM', shutdown)
