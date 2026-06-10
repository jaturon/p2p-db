import * as esbuild from 'esbuild'
import { copyFile, mkdir, readFile, writeFile } from 'node:fs/promises'

await mkdir('dist', { recursive: true })

await esbuild.build({
  entryPoints: ['src/standalone.js'],
  bundle: true,
  format: 'iife',
  globalName: 'P2PDB',
  outfile: 'dist/p2p-db.js',
  platform: 'browser',
  target: ['chrome100', 'firefox100', 'safari15'],
  define: { 'process.env.NODE_ENV': '"production"' },
  logLevel: 'info',
})

// wa-sqlite.wasm must sit next to dist/p2p-db.js — standalone.js points
// wa-sqlite's locateFile() at document.currentScript.src's directory.
await copyFile('node_modules/wa-sqlite/dist/wa-sqlite.wasm', 'dist/wa-sqlite.wasm')

// db-worker.js (Mode B / durable IndexedDB) — opt-in, loaded via DB_WORKER_URL.
// It imports its own deps from esm.sh at runtime, so it's copied as-is.
await copyFile('src/db-worker.js', 'dist/db-worker.js')

console.log('Copied dist/wa-sqlite.wasm, dist/db-worker.js')

// ── Single-file standalone HTML ─────────────────────────────────────────────
// Firefox (and others) refuse to load <script src="..."> pointing at
// sibling file:// resources — every file:// document is a "unique origin",
// so cross-file script loading is blocked entirely when double-clicked from
// disk. Inline both the bundle and the demo app into one .html file so it
// can be opened directly with no server and no other files alongside it.
const escapeForInlineScript = (js) => js.replace(/<\/script/gi, '<\\/script')

const bundleJs = await readFile('dist/p2p-db.js', 'utf8')
const appJs    = await readFile('standalone/app.js', 'utf8')
const html     = await readFile('standalone/index.html', 'utf8')

const standaloneHtml = html
  .replace(
    '<script src="../dist/p2p-db.js"></script>',
    `<script>\n${escapeForInlineScript(bundleJs)}\n</script>`
  )
  .replace(
    '<script src="./app.js"></script>',
    `<script>\n${escapeForInlineScript(appJs)}\n</script>`
  )

await writeFile('dist/p2p-db.standalone.html', standaloneHtml)
console.log('Wrote dist/p2p-db.standalone.html (single self-contained file)')
