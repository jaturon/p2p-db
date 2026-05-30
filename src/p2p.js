// P2P node using libp2p v2 with:
//   - WebSockets transport (ws:// for local dev; wss:// for deployed relay)
//   - Circuit relay v2 (relay auto-discovered via /api/info)
//   - GossipSub pub/sub for topic-based messaging
//   - pubsub peer discovery
//
// Relay discovery order:
//   1. ?relay=https://my-relay.fly.dev  (URL query param)
//   2. http://localhost:4010             (local libp2p_test gateway, dev only)
// Deploy relay/server.js to Fly.io (see relay/fly.toml) for public access.

import { createLibp2p } from 'libp2p'
import { webSockets } from '@libp2p/websockets'
import { webRTC } from '@libp2p/webrtc'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { pubsubPeerDiscovery } from '@libp2p/pubsub-peer-discovery'
import { circuitRelayTransport } from '@libp2p/circuit-relay-v2'
import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { identify } from '@libp2p/identify'
import { multiaddr } from '@multiformats/multiaddr'

// Allow all WebSocket addresses (ws:// and wss://) including LAN
const wsAll = addrs => addrs

export const BOOTSTRAP_LIST = [] // kept for API compatibility

// Build the ordered list of relay API endpoints to try.
// ?relay=https://my-relay.fly.dev is checked first so deployed apps work
// without changing any source files — just change the URL.
function buildRelayCandidates() {
  const candidates = []

  if (typeof window !== 'undefined') {
    const relayParam = new URLSearchParams(window.location.search).get('relay')
    if (relayParam) {
      candidates.push(`${relayParam.replace(/\/$/, '')}/api/info`)
    }
  }

  // Local libp2p_test gateway (dev)
  const host  = typeof window !== 'undefined' ? window.location.hostname : 'localhost'
  const proto = typeof window !== 'undefined' ? window.location.protocol : 'http:'
  candidates.push(`${proto}//${host}:4010/api/info`)
  candidates.push('http://localhost:4010/api/info')

  return candidates
}

async function discoverGateway(extraUrls = []) {
  for (const url of [...extraUrls, ...buildRelayCandidates()]) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(3000) })
      if (!res.ok) continue
      const info = await res.json()
      const addrs = info.addrs ?? []
      // Accept any WebSocket address (ws or wss); prefer non-loopback
      const ws = addrs.find(a => a.includes('/ws') && !a.includes('127.0.0.1') && !a.includes('172.'))
               ?? addrs.find(a => a.includes('/ws'))
      if (ws) return { addr: ws, peerId: info.peer_id }
    } catch { /* try next */ }
  }
  return null
}

const enc = new TextEncoder()
const dec = new TextDecoder()

export class P2PNode {
  constructor(node, topic) {
    this._node = node
    this._topic = topic
    this._handlers = { message: [], 'peer:connect': [], 'peer:disconnect': [], 'self:update': [] }
  }

  /**
   * Create and start a P2P node.
   *
   * @param {object} opts
   * @param {string}   [opts.topic='p2p-db']      GossipSub topic for messaging
   * @param {string[]} [opts.bootstrapList]        Override bootstrap multiaddrs
   * @param {function} [opts.onMessage]            Shorthand message handler (from, data)
   */
  static async create(opts = {}) {
    const {
      topic = 'p2p-db',
      onMessage,
    } = opts

    // Auto-discover the local libp2p_test gateway before creating the node
    const gateway = await discoverGateway()

    const node = await createLibp2p({
      transports: [
        webSockets({ filter: wsAll }),
        webRTC(),
        circuitRelayTransport({ discoverRelays: 1 }),
      ],
      connectionEncrypters: [noise()],
      streamMuxers: [yamux()],
      // Allow dialing private/LAN addresses (gateway is on LAN)
      connectionGater: { denyDialMultiaddr: async () => false },
      peerDiscovery: [
        pubsubPeerDiscovery({ interval: 10_000 }),
      ],
      services: {
        identify: identify(),
        pubsub: gossipsub({
          allowPublishToZeroTopicPeers: true,
          emitSelf: false,
          scoreThresholds: {
            gossipThreshold: -Infinity,
            publishThreshold: -Infinity,
            graylistThreshold: -Infinity,
            acceptPXThreshold: 0,
            opportunisticGraftThreshold: 1,
          },
        }),
      },
    })

    await node.start()

    // Dial the gateway directly — mirrors browser-client/main.js
    if (gateway) {
      try {
        await node.dial(multiaddr(gateway.addr))
        console.log('[p2p] connected to gateway:', gateway.addr)
      } catch (err) {
        console.warn('[p2p] gateway dial failed:', err.message)
      }
    } else {
      console.warn('[p2p] no local gateway found at port 4010')
    }

    const p2p = new P2PNode(node, topic)

    node.services.pubsub.subscribe(topic)

    node.services.pubsub.addEventListener('message', evt => {
      if (evt.detail.topic !== topic) return
      let data
      try { data = JSON.parse(dec.decode(evt.detail.data)) } catch { data = dec.decode(evt.detail.data) }
      const from = evt.detail.from.toString()
      p2p._dispatch('message', from, data)
      onMessage?.(from, data)
    })

    node.addEventListener('peer:connect', evt => {
      p2p._dispatch('peer:connect', evt.detail.toString())
    })

    node.addEventListener('peer:disconnect', evt => {
      p2p._dispatch('peer:disconnect', evt.detail.toString())
    })

    // Fires when the node's multiaddrs change — e.g. when a circuit relay
    // reservation is obtained and a /p2p-circuit/ address becomes available.
    for (const evtName of ['self:peer:update', 'peer:update', 'peer:identify:push', 'transport:listening']) {
      node.addEventListener(evtName, () => {
        console.debug('[p2p] event:', evtName, 'multiaddrs:', node.getMultiaddrs().map(a => a.toString()))
        p2p._dispatch('self:update')
      })
    }

    return p2p
  }

  /** Local peer ID (base58btc string) */
  get peerId() {
    return this._node.peerId.toString()
  }

  /** All active multiaddrs this node is reachable at */
  get multiaddrs() {
    return this._node.getMultiaddrs().map(a => a.toString())
  }

  /** Circuit-relay multiaddrs — available after relay reservation is made */
  get relayMultiaddrs() {
    return this._node.getMultiaddrs()
      .map(a => a.toString())
      .filter(a => a.includes('/p2p-circuit/'))
  }

  /** Currently connected peer IDs */
  get peers() {
    return this._node.getPeers().map(p => p.toString())
  }

  /**
   * Dial a relay by its /api/info URL and make a circuit relay reservation.
   * Use this after node creation to connect to a relay entered in the UI.
   * Returns true on success, false if the relay could not be reached.
   *
   * @param {string} relayBaseUrl  e.g. 'https://my-relay.fly.dev'
   */
  async dialRelay(relayBaseUrl) {
    const apiUrl = `${relayBaseUrl.replace(/\/$/, '')}/api/info`
    const gateway = await discoverGateway([apiUrl])
    if (!gateway) return false
    try {
      await this._node.dial(multiaddr(gateway.addr))
      console.log('[p2p] connected to relay:', gateway.addr)
      return true
    } catch (err) {
      console.warn('[p2p] relay dial failed:', err.message)
      return false
    }
  }

  /**
   * Publish a message to the topic. Data is JSON-serialized automatically.
   * Resolves when the message has been sent to at least one peer (or buffered
   * if allowPublishToZeroTopicPeers is set).
   */
  async send(data) {
    await this._node.services.pubsub.publish(this._topic, enc.encode(JSON.stringify(data)))
  }

  /**
   * Register an event handler.
   * @param {'message'|'peer:connect'|'peer:disconnect'} event
   * @param {function} handler
   *   - message:          (fromPeerId: string, data: any) => void
   *   - peer:connect:     (peerId: string) => void
   *   - peer:disconnect:  (peerId: string) => void
   */
  on(event, handler) {
    if (this._handlers[event]) this._handlers[event].push(handler)
    return this
  }

  off(event, handler) {
    if (this._handlers[event]) {
      this._handlers[event] = this._handlers[event].filter(h => h !== handler)
    }
    return this
  }

  async stop() {
    await this._node.stop()
  }

  _dispatch(event, ...args) {
    for (const h of this._handlers[event] ?? []) h(...args)
  }
}
