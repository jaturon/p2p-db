// P2P node using libp2p v2 with:
//   - WebSockets transport
//   - Circuit relay v2 (local libp2p_test gateway as relay server)
//   - GossipSub pub/sub for topic-based messaging
//   - pubsub peer discovery

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
const wsAll = () => true

// Gateway API candidates — mirrors browser-client/main.js auto-discovery logic.
// The libp2p_test Node.js gateway runs on port 4010 (API) / 4012 (WS).
const GATEWAY_API_CANDIDATES = [
  `${typeof window !== 'undefined' ? window.location.protocol : 'http:'}//${typeof window !== 'undefined' ? window.location.hostname : 'localhost'}:4010/api/info`,
  'http://localhost:4010/api/info',
]

export const BOOTSTRAP_LIST = [] // not used when local gateway is available

async function discoverGateway() {
  for (const url of GATEWAY_API_CANDIDATES) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(2000) })
      if (!res.ok) continue
      const info = await res.json()
      const addrs = info.addrs ?? []
      // Prefer LAN WS address so it works from other machines on the same network
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
