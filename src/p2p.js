// P2P node using libp2p v2 with:
//   - WebSockets + WebRTC transports
//   - Circuit relay v2 for NAT traversal (discovers relays from bootstrap list)
//   - GossipSub pub/sub for topic-based messaging
//   - Bootstrap + pubsub peer discovery

import { createLibp2p } from 'libp2p'
import { webSockets } from '@libp2p/websockets'
import { webRTC } from '@libp2p/webrtc'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { bootstrap } from '@libp2p/bootstrap'
import { pubsubPeerDiscovery } from '@libp2p/pubsub-peer-discovery'
import { circuitRelayTransport } from '@libp2p/circuit-relay-v2'
import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { identify } from '@libp2p/identify'

// @libp2p/websockets removed the filters sub-path in v10; inline the equivalent
const wsAll = addrs => addrs.filter(ma => {
  const p = ma.protoNames()
  return p.includes('ws') || p.includes('wss')
})

// Public Protocol Labs bootstrap + relay nodes (support circuit relay v2).
// Using concrete /dns/.../tcp/443/wss/... addresses instead of /dnsaddr/
// because the local DNS resolver filters TXT records for libp2p.io, which
// breaks the dnsaddr multi-step walk the browser would otherwise perform.
export const BOOTSTRAP_LIST = [
  '/dns/sv15.bootstrap.libp2p.io/tcp/443/wss/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN',
  '/dns/ny5.bootstrap.libp2p.io/tcp/443/wss/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa',
  '/dns/sg1.bootstrap.libp2p.io/tcp/443/wss/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt',
  '/dns/am6.bootstrap.libp2p.io/tcp/443/wss/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb',
]

const enc = new TextEncoder()
const dec = new TextDecoder()

export class P2PNode {
  constructor(node, topic) {
    this._node = node
    this._topic = topic
    this._handlers = { message: [], 'peer:connect': [], 'peer:disconnect': [] }
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
      bootstrapList = BOOTSTRAP_LIST,
      onMessage,
    } = opts

    const node = await createLibp2p({
      transports: [
        webSockets({ filter: wsAll }),
        webRTC(),
        // Circuit relay enables connections through relay nodes for peers behind NAT
        circuitRelayTransport({ discoverRelays: 2 }),
      ],
      connectionEncrypters: [noise()],
      streamMuxers: [yamux()],
      peerDiscovery: [
        bootstrap({ list: bootstrapList }),
        // Announces self on _peer-discovery._p2p._pubsub and discovers others
        pubsubPeerDiscovery({ interval: 10_000 }),
      ],
      services: {
        identify: identify(),
        pubsub: gossipsub({ allowPublishToZeroTopicPeers: true, emitSelf: false }),
      },
    })

    await node.start()

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
