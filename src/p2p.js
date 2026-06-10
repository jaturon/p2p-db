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
import { all as wsAll } from '@libp2p/websockets/filters'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
// pubsubPeerDiscovery removed — GossipSub signature validation prevents it
// from routing across different libp2p versions. Browser discovery uses
// db:peer-announce messages over the confirmed-working notes topic instead.
import { circuitRelayTransport, circuitRelayServer } from '@libp2p/circuit-relay-v2'
import { webRTC } from '@libp2p/webrtc'
import { gossipsub } from '@chainsafe/libp2p-gossipsub'
import { identify } from '@libp2p/identify'
import { multiaddr } from '@multiformats/multiaddr'

export const BOOTSTRAP_LIST = [] // kept for API compatibility

// Build the ordered list of relay API endpoints to try.
// ?relay=https://my-relay.fly.dev is checked first so deployed apps work
// without changing any source files — just change the URL.
function buildRelayCandidates() {
  const candidates = []
  let hasCustomRelay = false

  if (typeof window !== 'undefined') {
    const relayParam = new URLSearchParams(window.location.search).get('relay')
    if (relayParam) {
      hasCustomRelay = true
      // Comma-separated list: ?relay=http://r1.fly.dev,http://r2.fly.dev
      for (const u of relayParam.split(',')) {
        const t = u.trim()
        if (t) candidates.push(`${t.replace(/\/$/, '')}/api/info`)
      }
    }
  }

  // When no custom relay is configured, auto-try the local relay ports when
  // the page is served from a "local" origin:
  //   localhost / 127.0.0.1  — classic dev
  //   *.local               — mDNS hostnames (e.g. local-ai-home.local)
  // On a bare LAN IP (192.168.x.x) we skip — those fetches always refuse.
  if (!hasCustomRelay && typeof window !== 'undefined') {
    const host    = window.location.hostname
    const isLocal = host === 'localhost' || host === '127.0.0.1' || host.endsWith('.local')
    if (isLocal) {
      // Use the page hostname so local-ai-home.local:4010 is tried directly
      candidates.push(`http://${host}:4010/api/info`)
      candidates.push(`http://${host}:3000/api/info`)
      // Also try plain localhost in case the relay only binds there
      if (host !== 'localhost') {
        candidates.push('http://localhost:4010/api/info')
        candidates.push('http://localhost:3000/api/info')
      }
    }
  }

  // Default public relays — always tried as a final fallback so any device
  // on any network can sync without manual configuration. Both relays are
  // peered with each other (PEER_RELAYS mesh), so connecting to either is
  // enough, but trying both gives redundancy if one is down.
  candidates.push('http://202.44.53.65:4010/api/info')
  candidates.push('http://199.241.138.174:4010/api/info')

  return candidates
}

// Maximum number of relay slots the browser will maintain simultaneously.
const MAX_RELAYS = 3

// Try a single /api/info URL; throws on any failure.
async function tryRelayUrl(url) {
  const res = await fetch(url, { signal: AbortSignal.timeout(3000) })
  if (!res.ok) throw new Error(`${res.status}`)
  const info = await res.json()
  const addrs = info.addrs ?? []

  // When the browser itself is on localhost/127.0.0.1/*.local, prefer the
  // loopback address so we dial ws://127.0.0.1:port instead of the LAN IP.
  // The LAN IP still works, but loopback is more reliable and avoids
  // spurious "can't establish connection" warnings in Firefox when the relay
  // is briefly unreachable from the external interface.
  const onLocalhost = typeof window !== 'undefined' &&
    (window.location.hostname === 'localhost' ||
     window.location.hostname === '127.0.0.1' ||
     window.location.hostname.endsWith('.local'))

  let ws
  if (onLocalhost) {
    // Prefer loopback for localhost browsers, then any WS
    ws = addrs.find(a => a.includes('/ws') && a.includes('127.0.0.1'))
      ?? addrs.find(a => a.includes('/ws'))
  } else {
    // For remote browsers, prefer non-loopback non-docker-bridge addresses
    ws = addrs.find(a => a.includes('/ws') && !a.includes('127.0.0.1') && !a.includes('172.'))
      ?? addrs.find(a => a.includes('/ws'))
  }

  if (!ws) throw new Error('no ws addr')
  return { addr: ws, peerId: info.peer_id }
}

// Discover ALL reachable relays in parallel.
// Returns an array (possibly empty) deduplicated by peer ID.
async function discoverGateways(extraUrls = []) {
  const candidates = [...new Set([...extraUrls, ...buildRelayCandidates()])]
  const results = await Promise.allSettled(candidates.map(tryRelayUrl))
  const seen = new Set()
  return results
    .filter(r => r.status === 'fulfilled')
    .map(r => r.value)
    .filter(g => { if (seen.has(g.peerId)) return false; seen.add(g.peerId); return true })
}

// Backwards-compat single-result helper used by dialRelay().
async function discoverGateway(extraUrls = []) {
  try { return await Promise.any(extraUrls.map(tryRelayUrl)) } catch { return null }
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

    // Discover all reachable relays before creating the node so we can set
    // discoverRelays to the right count and dial them all after start().
    const gateways = await discoverGateways()

    const node = await createLibp2p({
      addresses: {
        // /p2p-circuit  — circuit relay reservation (server relay gives us a public addr)
        // /webrtc       — WebRTC via circuit relay signaling; once established the
        //                 direct peer connection survives relay death
        listen: ['/p2p-circuit', '/webrtc'],
      },
      transports: [
        // wsAll (from @libp2p/websockets/filters) accepts ws:// + wss:// but NOT
        // /p2p-circuit, so the WebSocket transport won't try to create a browser
        // server listener when /p2p-circuit is in addresses.listen.
        webSockets({ filter: wsAll }),
        // WebRTC: after signaling over a circuit relay, the actual data connection
        // is peer-to-peer and survives relay death.  Browsers that connected via
        // a server relay can upgrade to WebRTC and keep talking after it dies.
        webRTC(),
        // discoverRelays tells libp2p how many relay reservations to maintain.
        // More than 1 means the browser keeps a slot at each relay; if one
        // goes down it stays reachable via the others.
        circuitRelayTransport({ discoverRelays: MAX_RELAYS }),
      ],
      connectionEncrypters: [noise()],
      streamMuxers: [yamux()],
      // Allow dialing private/LAN addresses (gateway is on LAN)
      connectionGater: { denyDialMultiaddr: async () => false },
      services: {
        identify: identify(),

        // Browser-as-relay: every open tab acts as a circuit relay server so
        // peers that connect through a server relay can then use this browser
        // as an additional relay for other browsers.  If the server relay goes
        // down, connected browsers continue routing for each other.
        // maxReservations is kept small — a browser tab is not a server.
        relay: circuitRelayServer({ reservations: { maxReservations: 10 } }),

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

    // Dial every discovered relay. Each successful dial gives the browser a
    // circuit relay reservation; if any relay later goes down the others
    // keep it reachable.
    if (gateways.length === 0) {
      console.log('[p2p] no relay configured — enter a relay URL in the sidebar to enable sync')
    }
    for (const gw of gateways) {
      try {
        await node.dial(multiaddr(gw.addr))
        console.log('[p2p] connected to relay:', gw.addr)
      } catch (err) {
        console.warn('[p2p] relay dial failed:', err.message)
      }
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
      // If a relay disconnects and we now have fewer peers than we started with,
      // re-discover and re-dial all configured relays after a short delay.
      // This makes the browser automatically reconnect to the default relay
      // (202.44.53.65) when the local relay dies.
      setTimeout(async () => {
        if (node.getPeers().length < gateways.length) {
          const fresh = await discoverGateways()
          for (const gw of fresh) {
            if (!node.getPeers().some(p => p.toString() === gw.peerId)) {
              node.dial(multiaddr(gw.addr)).catch(() => {})
            }
          }
        }
      }, 3000)
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
   * Dial one or more relays by their /api/info base URLs.
   * Accepts a comma-separated string or an array.
   * Returns true if at least one relay connected successfully.
   *
   * @param {string|string[]} relayUrls  e.g. 'https://r1.fly.dev,https://r2.fly.dev'
   */
  async dialRelay(relayUrls) {
    const urls = (Array.isArray(relayUrls) ? relayUrls : relayUrls.split(','))
      .map(u => `${u.trim().replace(/\/$/, '')}/api/info`)
      .filter(Boolean)

    const gateways = await discoverGateways(urls)
    if (gateways.length === 0) return false

    let anyOk = false
    for (const gw of gateways) {
      try {
        await this._node.dial(multiaddr(gw.addr))
        console.log('[p2p] connected to relay:', gw.addr)
        anyOk = true
      } catch (err) {
        console.warn('[p2p] relay dial failed:', err.message)
      }
    }
    return anyOk
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
