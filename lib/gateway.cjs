'use strict'
/**
 * The public edge: `https://<your-host>/ipfs/<cid>[/path]`.
 *
 * Two audiences. Other IPFS software (kubo with HTTP retrieval, verified-
 * fetch, other gateways) asks for `?format=raw` / `?format=car` or the
 * matching Accept type and verifies what it gets — the trustless gateway
 * spec. People ask for `/ipfs/<cid>/index.html` in a browser and expect the
 * bytes — the UnixFS path resolver in unixfs.cjs. Both are served from the
 * same block store: a range read out of the pin's CAR on Sia (pins.locate),
 * through the block cache (cache.cjs: RAM for the recent, the box's disk for
 * the popular) so a site's second visitor costs no host bandwidth.
 *
 * TIME TO FIRST BYTE IS THE EXPERIENCE. A fresh pin is warm: its blocks go
 * into the cache while the worker indexes the CAR (pins.indexPin → warm()).
 * A block that has gone cold is read two ways at once — its own 256 KiB
 * (≈0.2 s on a Sia host, measured) answers the request, and one big read of
 * the CAR window around it (≈27 MB/s, against ≈1 MB/s one block at a time)
 * lands the rest in the cache for the blocks that follow, with the next
 * window prefetched as a reader nears the end of this one.
 *
 * METERING IS THE POINT: every byte that leaves is
 * counted against the pin's owner for the UTC day, and the owner's plan has
 * a monthly public allowance. Past it the edge slows (a wait before each
 * response), past 1.5× it answers 429 — and never unpins: the data stays,
 * the free ride ends. One abuser's site gets slow; nobody else notices.
 */
const { CID } = require('./cid.cjs')
const car = require('./car.cjs')
const unixfs = require('./unixfs.cjs')
const pins = require('./pins.cjs')
const sia = require('./sia.cjs')
const { BlockCache } = require('./cache.cjs')

const RAW = 'application/vnd.ipld.raw'
const CAR = 'application/vnd.ipld.car'
const SLOW_MS = 1500
// What a stranger's page gets on the site's own origin: an opaque origin (no
// allow-same-origin), so its scripts can neither reach the signed-in account
// nor read this origin's storage. A name's site is its own origin and runs free.
const SANDBOX = 'sandbox allow-scripts allow-forms allow-popups allow-popups-to-escape-sandbox allow-modals allow-downloads'
const HARD_FACTOR = 1.5
const WINDOW = 16 * 1024 * 1024 // one Sia read fills this much of a cold CAR (fill())
const FILLED_MAX = 4096 // windows remembered as filled, for the read-ahead
const keyOf = (blockCid) => CID.fromKey(blockCid).multihash.toString('hex')

const MIME = {
  html: 'text/html; charset=utf-8',
  htm: 'text/html; charset=utf-8',
  css: 'text/css; charset=utf-8',
  js: 'text/javascript; charset=utf-8',
  mjs: 'text/javascript; charset=utf-8',
  json: 'application/json',
  txt: 'text/plain; charset=utf-8',
  md: 'text/markdown; charset=utf-8',
  png: 'image/png',
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  gif: 'image/gif',
  svg: 'image/svg+xml',
  webp: 'image/webp',
  ico: 'image/x-icon',
  avif: 'image/avif',
  mp4: 'video/mp4',
  webm: 'video/webm',
  mp3: 'audio/mpeg',
  ogg: 'audio/ogg',
  wav: 'audio/wav',
  flac: 'audio/flac',
  woff: 'font/woff',
  woff2: 'font/woff2',
  ttf: 'font/ttf',
  pdf: 'application/pdf',
  wasm: 'application/wasm',
  xml: 'application/xml',
  zip: 'application/zip'
}
const mimeOf = (name) => MIME[(name.split('.').pop() || '').toLowerCase()] || 'application/octet-stream'

class Gateway {
  /**
   * @param {object} o
   * @param {object} o.db
   * @param {() => object|null} o.sdk  the service Sia session (null = not connected)
   * @param {object} o.plans
   * @param {(id: string) => object|null} o.accountOf
   */
  constructor ({ db, sdk, plans, accountOf, cache = new BlockCache(), window = WINDOW, log = () => {} }) {
    this.db = db; this.sdk = sdk; this.plans = plans; this.accountOf = accountOf; this.cache = cache; this.window = window; this.log = log
    this.objects = new Map() // object_id → PinnedObject, reopened once
    this.fills = new Map() // "<object_id>:<window start>" → the read in flight
    this.filled = new Set() // windows that landed, most recent last (read-ahead skips them)
  }

  async object (objectId) {
    let o = this.objects.get(objectId)
    if (!o) { o = await sia.object(this.sdk(), objectId); this.objects.set(objectId, o) }
    return o
  }

  /** A block we already have the bytes of (a pin being indexed): RAM now, disk too — it survives a restart. */
  warm (cid, data) {
    cid.verify(data)
    const key = cid.multihash.toString('hex')
    this.cache.set(key, data)
    this.cache.promote(key, data)
  }

  /** A block by CID from the cache or Sia; null when we do not hold it. */
  async block (cid) {
    const inline = cid.inline()
    if (inline) return { data: inline, loc: null, cached: true }
    const key = cid.multihash.toString('hex') // the bytes are the hash's, whatever the codec
    const hit = await this.cache.get(key)
    if (hit) {
      cid.verify(hit)
      const loc = pins.locate(this.db, cid)
      if (loc) this.ahead(loc)
      return { data: hit, loc, cached: true }
    }
    const loc = pins.locate(this.db, cid)
    if (!loc) return null
    const s = this.sdk()
    if (!s) throw new Error('storage is not connected')
    // Two reads race: the block's own bytes (small, quick) and the window
    // holding it (big, for everything after). Whichever lands first answers.
    const direct = sia.readRange(s, await this.object(loc.object_id), loc.offset, loc.length).then((data) => cid.verify(data))
    const viaWindow = this.fill(loc).then(() => this.cache.peek(key) || direct)
    let data
    try { data = await Promise.any([direct, viaWindow]) } catch (e) { throw e.errors ? e.errors[0] : e }
    direct.catch(() => {}); viaWindow.catch(() => {}) // the loser may still fail; it has been counted
    this.cache.set(key, data)
    return { data, loc, cached: false }
  }

  /**
   * One Sia read for the CAR window a block sits in — WINDOW-aligned, and
   * reaching to the end of the last block that starts inside it — every
   * block of which goes into the cache. Concurrent misses in one window
   * share the read. Resolves when the window is in.
   */
  fill (loc) {
    const start = Math.floor(loc.offset / this.window) * this.window
    const id = `${loc.object_id}:${start}`
    let p = this.fills.get(id)
    if (p) return p
    p = (async () => {
      const rows = pins.blocksBetween(this.db, loc.pin, start, Math.min(start + this.window, loc.size))
      if (!rows.length) return
      const end = Math.max(...rows.map((r) => r.offset + r.length))
      const buf = await sia.readRange(this.sdk(), await this.object(loc.object_id), start, end - start)
      for (const r of rows) {
        const cid = CID.fromKey(r.block_cid)
        const data = Buffer.from(buf.subarray(r.offset - start, r.offset - start + r.length))
        cid.verify(data)
        this.cache.set(keyOf(r.block_cid), data)
      }
      this.filled.add(id)
      if (this.filled.size > FILLED_MAX) this.filled.delete(this.filled.values().next().value)
    })().finally(() => this.fills.delete(id))
    this.fills.set(id, p)
    return p
  }

  /** Wait for in-flight window reads (tests). */
  async idle () {
    while (this.fills.size) await Promise.allSettled([...this.fills.values()])
  }

  /** Reading near the end of a window: have the next one on the way before it is asked for. */
  ahead (loc) {
    const start = Math.floor(loc.offset / this.window) * this.window
    const next = start + this.window
    if (next >= loc.size || loc.offset + loc.length < next - this.window / 4) return
    if (this.filled.has(`${loc.object_id}:${next}`) || this.fills.has(`${loc.object_id}:${next}`)) return
    const [head] = pins.blocksBetween(this.db, loc.pin, next, next + this.window)
    if (!head || this.cache.has(keyOf(head.block_cid))) return // nothing there, or it is warm already
    if (!this.sdk()) return
    this.fill({ ...loc, offset: next }).catch((e) => this.log(`read-ahead ${loc.pin} @${next}: ${e.message}`))
  }

  /** A block's size from the index alone; null when we do not hold it. */
  blockSize (cid) {
    const inline = cid.inline()
    if (inline) return inline.length
    const loc = pins.locate(this.db, cid)
    return loc ? loc.length : null
  }

  /* ------------------------------------------------------- metering */

  monthBytes (accountId) {
    const month = new Date().toISOString().slice(0, 7)
    return Number(this.db.prepare('SELECT COALESCE(SUM(bytes), 0) AS b FROM egress WHERE account_id = ? AND day LIKE ?').get(accountId, `${month}-%`).b)
  }

  /** 'ok' | 'slow' | 'refuse' for this owner right now. */
  standing (accountId) {
    if (!accountId) return 'ok'
    const acct = this.accountOf(accountId)
    const plan = this.plans[acct ? acct.plan : 'free'] || this.plans.free
    const cap = plan.publicBytesPerMonth || 0
    if (!cap) return 'ok'
    const used = this.monthBytes(accountId)
    if (used >= cap * HARD_FACTOR) return 'refuse'
    if (used >= cap) return 'slow'
    return 'ok'
  }

  count (accountId, pinKey, bytes) {
    if (!accountId || !bytes) return
    const day = new Date().toISOString().slice(0, 10)
    this.db.prepare(`INSERT INTO egress (day, account_id, cid, bytes) VALUES (?, ?, ?, ?)
      ON CONFLICT(day, account_id, cid) DO UPDATE SET bytes = bytes + excluded.bytes`).run(day, accountId, pinKey, bytes)
  }

  /* -------------------------------------------------------- request */

  /**
   * Answer a request. `req`: {method, path, query, headers}; returns
   * {status, headers, body: Buffer|AsyncIterable|null}. Pure over the
   * store; the HTTP server just copies it out.
   */
  async handle (req) {
    const m = /^\/ipfs\/([^/?#]+)(\/[^?#]*)?$/.exec(req.path)
    if (!m) return text(404, 'not an /ipfs/ path')
    let root
    try { root = CID.parse(decodeURIComponent(m[1])) } catch { return text(400, 'not a CID') }
    const segments = (m[2] || '').split('/').filter(Boolean).map((s) => decodeURIComponent(s))
    const accept = String(req.headers.accept || '')
    const format = (req.query.format || (accept.includes(RAW) ? 'raw' : accept.includes(CAR) ? 'car' : '')).toLowerCase()
    if (req.method !== 'GET' && req.method !== 'HEAD') return text(405, 'GET or HEAD')

    // Whose bytes are these? The pin holding the root decides; a path under it
    // resolves through the same archive. An inline (identity) CID is nobody's:
    // its bytes are in the request, so it is served unmetered — kubo probes
    // `bafkqaaa` before it will fetch anything from a gateway.
    const rootLoc = root.inline() ? { pin: null, owner: null } : pins.locate(this.db, root)
    if (!rootLoc) return text(404, 'not pinned here')
    const owner = rootLoc.owner
    const standing = this.standing(owner)
    if (standing === 'refuse') return text(429, 'this site is over its monthly allowance', { 'retry-after': '3600' })
    if (standing === 'slow') await new Promise((resolve) => setTimeout(resolve, SLOW_MS))

    // One request asks the cache once per block: resolving a path, describing
    // the node and reading it are three looks at the same block, and only a
    // second request should make a block "popular" (cache.cjs).
    const getBlock = requestBlocks((cid) => this.block(cid))
    const send = (status, headers, body) => {
      // A HEAD moves no bytes, so it costs the owner none.
      const len = req.method === 'HEAD' ? 0 : Buffer.isBuffer(body) ? body.length : Number(headers['content-length'] || 0)
      this.count(owner, rootLoc.pin, len)
      headers['access-control-allow-origin'] = '*' // content-addressed bytes are anyone's to read: a sandboxed page fetching its own files, a verifying client anywhere
      if (req.shown == null) headers['content-security-policy'] = SANDBOX
      // `/ipfs/<cid>` is the same bytes forever, so a year is right. A name's
      // site (`req.shown`) is a MUTABLE url — repointing it, or unpinning to
      // take content down, has to reach people who have already visited, or
      // the site outlives its content after all. The
      // etag is still the CID, so a revalidation is a cheap 304.
      if (req.shown != null && headers['cache-control']) headers['cache-control'] = 'public, no-cache'
      return { status, headers, body: req.method === 'HEAD' ? null : body }
    }

    try {
      if (format === 'raw') {
        const cid = segments.length ? await unixfs.resolvePath(getBlock, root, segments) : root
        // kubo HEADs a block before it GETs it; the index knows the size, so
        // that costs no read from the hosts.
        const size = req.method === 'HEAD' ? this.blockSize(cid) : null
        const b = size === null ? await this.block(cid) : { data: null }
        if (!b) return text(404, 'block not held here')
        return send(200, {
          'content-type': RAW,
          'content-length': String(size === null ? b.data.length : size),
          etag: `"${cid}.raw"`,
          'cache-control': 'public, max-age=29030400, immutable',
          'x-ipfs-path': req.path,
          'content-disposition': `attachment; filename="${cid}.bin"`
        }, b.data)
      }
      if (format === 'car') {
        // dag-scope=all (the default) of the root: the archive verbatim. Any
        // other scope, or a sub-path: block-scope CAR of that one node.
        const scope = (req.query['dag-scope'] || 'all').toLowerCase()
        if (!segments.length && scope === 'all' && rootLoc.pin === root.key()) {
          const s = this.sdk()
          const obj = await this.object(rootLoc.object_id)
          return send(200, {
            'content-type': `${CAR}; version=1`,
            'content-length': String(rootLoc.size),
            etag: `"${root}.car"`,
            'cache-control': 'public, max-age=29030400, immutable',
            'x-ipfs-path': req.path,
            'content-disposition': `attachment; filename="${root}.car"`
          }, sia.streamRange(s, obj, 0, rootLoc.size))
        }
        const cid = segments.length ? await unixfs.resolvePath(getBlock, root, segments) : root
        const b = await this.block(cid)
        if (!b) return text(404, 'block not held here')
        const body = Buffer.concat([car.header([cid]), car.section(cid, b.data)])
        return send(200, {
          'content-type': `${CAR}; version=1`,
          'content-length': String(body.length),
          etag: `"${cid}.car"`,
          'cache-control': 'public, max-age=29030400, immutable',
          'x-ipfs-path': req.path
        }, body)
      }

      // A person, or a plain HTTP client: resolve the path and serve the file.
      let cid = await unixfs.resolvePath(getBlock, root, segments)
      let d = await unixfs.describe(getBlock, cid)
      if (d.kind === 'directory') {
        // A site served under its own hostname shows that path, not /ipfs.
        const shown = req.shown ?? req.path
        if (!shown.endsWith('/') && segments.length) return { status: 301, headers: { location: shown + '/' }, body: null }
        const idx = d.entries.find((e) => e.name === 'index.html')
        if (!idx) return send(200, { 'content-type': 'text/html; charset=utf-8' }, Buffer.from(listing(root, segments, d.entries, req.shown)))
        cid = idx.cid; d = await unixfs.describe(getBlock, cid); segments.push('index.html')
      }
      const name = segments[segments.length - 1] || String(root)
      const size = d.size
      const range = parseRange(req.headers.range, size)
      if (range === 'bad') return text(416, 'range not satisfiable', { 'content-range': `bytes */${size}` })
      const [from, to] = range || [0, size - 1]
      const length = size === 0 ? 0 : to - from + 1
      const headers = {
        'content-type': mimeOf(name),
        'content-length': String(length),
        'accept-ranges': 'bytes',
        etag: `"${cid}"`,
        'cache-control': 'public, max-age=29030400, immutable',
        'x-ipfs-path': req.path,
        'x-ipfs-roots': String(root)
      }
      if (range) headers['content-range'] = `bytes ${from}-${to}/${size}`
      const body = length === 0 ? Buffer.alloc(0) : unixfs.fileRange(getBlock, cid, from, length)
      return send(range ? 206 : 200, headers, body)
    } catch (e) {
      if (e instanceof unixfs.NotFound) return text(404, e.message)
      if (e instanceof unixfs.NotServable) return text(501, e.message)
      throw e
    }
  }
}

function parseRange (header, size) {
  if (!header) return null
  const m = /^bytes=(\d*)-(\d*)$/.exec(String(header).trim())
  if (!m || size === 0) return null
  let from, to
  if (m[1] === '' && m[2] === '') return null
  if (m[1] === '') { const n = Number(m[2]); from = Math.max(0, size - n); to = size - 1 } else { from = Number(m[1]); to = m[2] === '' ? size - 1 : Math.min(Number(m[2]), size - 1) }
  if (from > to || from >= size) return 'bad'
  return [from, to]
}

function text (status, msg, extra = {}) {
  const body = Buffer.from(msg + '\n')
  return { status, headers: { 'content-type': 'text/plain; charset=utf-8', 'content-length': String(body.length), ...extra }, body }
}

const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]))
function listing (root, segments, entries, shown = null) {
  const here = shown ? shown.replace(/\/?$/, '/') : `/ipfs/${root}${segments.length ? '/' + segments.map(encodeURIComponent).join('/') : ''}/`
  const rows = entries.map((e) => `<li><a href="${esc(here + encodeURIComponent(e.name))}">${esc(e.name)}</a> <small>${e.tsize}</small></li>`).join('\n')
  return `<!doctype html><meta charset="utf-8"><title>${esc(here)}</title><style>body{font:14px system-ui;margin:2rem}small{color:#777}</style><h1>${esc(here)}</h1><ul>${rows}</ul>`
}

// A response retains at most 1 MiB of completed blocks, independent of file
// length. Larger blocks are returned without memoization. In-flight reads
// belong to the sequential UnixFS reader, not this cache.
function requestBlocks (read, limit = 1024 * 1024) {
  const recent = new Map()
  let bytes = 0
  return async (cid) => {
    const key = cid.multihash.toString('hex')
    if (recent.has(key)) {
      const data = recent.get(key)
      recent.delete(key); recent.set(key, data)
      return data
    }
    const block = await read(cid)
    if (!block) throw new unixfs.NotFound(`block ${cid} is not held here`)
    const data = block.data
    if (data.length <= limit) {
      while (recent.size && (bytes + data.length > limit || recent.size >= 128)) {
        const first = recent.keys().next().value
        bytes -= recent.get(first).length; recent.delete(first)
      }
      recent.set(key, data); bytes += data.length
    }
    return data
  }
}

module.exports = { Gateway, RAW, CAR, SLOW_MS, HARD_FACTOR, WINDOW, mimeOf, parseRange, requestBlocks }
