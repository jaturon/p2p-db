// Minimal static file server for the p2p-db example.
// Serves the example/ directory with the COOP + COEP headers required
// for SharedArrayBuffer (IDBBatchAtomicVFS Worker mode).
//
// Usage:  node server.js [port]
// Default port: 8080

import http           from 'http'
import fs             from 'fs'
import path           from 'path'
import os             from 'os'
import { fileURLToPath } from 'url'

const PORT = Number(process.argv[2]) || 8080
const ROOT = path.dirname(fileURLToPath(import.meta.url))

// Walk up one level so the library src/ is also reachable as ../src/
const SERVE_ROOT = path.resolve(ROOT, '..')

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'text/javascript; charset=utf-8',
  '.mjs':  'text/javascript; charset=utf-8',
  '.css':  'text/css; charset=utf-8',
  '.wasm': 'application/wasm',
  '.json': 'application/json',
  '.ico':  'image/x-icon',
}

const server = http.createServer((req, res) => {
  // Decode and strip query string
  let urlPath = decodeURIComponent(req.url.split('?')[0])
  if (urlPath === '/' || urlPath === '') {
    res.writeHead(302, { Location: '/example/' })
    res.end()
    return
  } else if (urlPath.endsWith('/')) {
    urlPath += 'index.html'   // /example/ → /example/index.html
  }

  const filePath = path.join(SERVE_ROOT, urlPath)

  // Prevent path traversal outside SERVE_ROOT
  if (!filePath.startsWith(SERVE_ROOT + path.sep) && filePath !== SERVE_ROOT) {
    res.writeHead(403)
    res.end('Forbidden')
    return
  }

  fs.readFile(filePath, (err, data) => {
    if (err) {
      const status = err.code === 'ENOENT' ? 404 : 500
      console.error(`${status} ${urlPath} — ${err.message}`)
      res.writeHead(status)
      res.end(err.code === 'ENOENT' ? `Not found: ${urlPath}` : `Server error: ${err.message}`)
      return
    }

    const ext  = path.extname(filePath).toLowerCase()
    const mime = MIME[ext] ?? 'application/octet-stream'

    res.writeHead(200, {
      'Content-Type': mime,
    })
    res.end(data)
  })
})

// Resolve LAN IPs (all non-internal IPv4 addresses)
function lanIPs() {
  return Object.values(os.networkInterfaces())
    .flat()
    .filter(n => n.family === 'IPv4' && !n.internal)
    .map(n => n.address)
}

server.listen(PORT, '0.0.0.0', () => {
  const ips = lanIPs()
  console.log(`p2p-db example server`)
  console.log(`  local   http://localhost:${PORT}/example/`)
  for (const ip of ips) {
    console.log(`  lan     http://${ip}:${PORT}/example/`)
  }
  console.log(``)
  console.log(`  IDB mode (Web Locks): available on all origins`)
  console.log(`  Ctrl-C to stop`)
})
