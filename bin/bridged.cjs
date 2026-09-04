#!/usr/bin/env node
'use strict'
/**
 * bridged — the daemon. One process holds the whole bridge:
 *
 *   gateway       /ipfs/<cid>[/path]  — the trustless gateway (raw, CAR) for
 *                 other IPFS software, and a UnixFS path resolver for people
 *                 and plain HTTP clients. Both read blocks by range-reading
 *                 the pin's CAR object on Sia, through the block cache.
 *   routing       /routing/v1/providers/<cid> — Delegated Routing V1
 *                 (IPIP-337): "this peer has it, over HTTP".
 *   blocks        /api/blocks — the key list the kubo sidecar in provider/
 *                 reprovides to the DHT. Internal; never expose it.
 *   pinning       /pins, /pins/<id> — the IPFS Pinning Service API, so
 *                 `ipfs pin remote` and Pinata-style clients work unchanged.
 *   share         POST /api/pins {cid, shareUrl} — pin an object a client
 *                 already stored on ITS OWN Sia account. No bytes move.
 *   uploads       POST /api/uploads … — send files that are not on IPFS yet;
 *                 they are hashed into the DAG kubo would build, written as
 *                 a CAR and queued like any other pin request.
 *
 * WHO OWNS A PIN is the one thing this daemon does not decide. `ownerOf(req)`
 * turns a request into an owner id and `policy` says what that owner may do
 * (lib/policy.cjs). Out of the box: one bearer token, one owner, no limits.
 * Replace both to put it behind your own accounts and billing — nothing else
 * in the bridge changes.
 *
 *   bridged connect     one-time: make the service's Sia session
 *   bridged serve       the daemon (default)
 */
const fs = require('fs')
const path = require('path')
const http = require('http')
const crypto = require('crypto')
const { Readable } = require('stream')

const dbm = require('../lib/db.cjs')
const { Indexd } = require('../lib/indexd.cjs')
const sia = require('../lib/sia.cjs')
const pins = require('../lib/pins.cjs')
const uploads = require('../lib/uploads.cjs')
const unixfs = require('../lib/unixfs.cjs')
const routingm = require('../lib/routing.cjs')
const policym = require('../lib/policy.cjs')
const { Provider } = require('../lib/provider.cjs')
const { Gateway } = require('../lib/gateway.cjs')
const { BlockCache } = require('../lib/cache.cjs')

const CAR_TYPE = 'application/vnd.ipld.car'
const JSON_LIMIT = 1024 * 1024
const MiB = 1024 * 1024

class HttpError extends Error { constructor (status, message) { super(message); this.status = status } }
const fail = (status, message) => { throw new HttpError(status, message) }
const json = (status, body) => ({ status, headers: { 'content-type': 'application/json' }, body: Buffer.from(JSON.stringify(body)) })

function positiveInt (v, dflt, name) {
  if (v === undefined || v === '') return dflt
  const n = Number(v)
  if (!Number.isFinite(n) || n <= 0) throw new Error(`${name}: not a positive number: ${v}`)
  return Math.floor(n)
}

/**
 * Configuration is the environment, so the whole thing is one compose file.
 * Only ORIGIN and the indexer are required; everything else has a default
 * that works on one box.
 */
function config (env = process.env) {
  const state = env.BRIDGE_STATE || '/var/lib/bridge'
  if (!env.ORIGIN) throw new Error('ORIGIN is required: the public https:// origin this gateway answers on')
  if (!env.INDEXD_APP_URL) throw new Error('INDEXD_APP_URL is required: the indexer this service is an app on')
  return {
    origin: env.ORIGIN.replace(/\/+$/, ''),
    port: positiveInt(env.PORT, 8080, 'PORT'),
    state,
    dbFile: env.BRIDGE_DB || path.join(state, 'bridge.db'),
    siaFile: env.SIA_SESSION || path.join(state, 'sia.json'),
    // The app's identity on the indexer. The id is 32 bytes made once
    // (`openssl rand -hex 32`) and then never changed.
    app: {
      id: env.APP_ID || '',
      name: env.APP_NAME || 'sia-ipfs-bridge',
      description: env.APP_DESCRIPTION || 'Public pins served to IPFS and the web.',
      serviceUrl: env.ORIGIN.replace(/\/+$/, '')
    },
    indexd: {
      adminUrl: env.INDEXD_ADMIN_URL || 'http://indexd:9980/api',
      adminPassword: env.INDEXD_ADMIN_PASSWORD || '',
      appUrl: env.INDEXD_APP_URL.replace(/\/+$/, '')
    },
    providerUrl: env.PROVIDER_URL || 'http://provider:5001',
    // Where a CID we do not hold is fetched from for a Pinning Service
    // request. Public gateways are a convenience and a dependency: the
    // durable answer is to fetch over bitswap through the sidecar.
    carGateways: (env.CAR_GATEWAYS || 'https://trustless-gateway.link').split(',').map((s) => s.trim()).filter(Boolean),
    cache: {
      memoryBytes: positiveInt(env.CACHE_MEMORY_BYTES, 512 * MiB, 'CACHE_MEMORY_BYTES'),
      diskBytes: positiveInt(env.CACHE_BYTES, 10 * 1024 * MiB, 'CACHE_BYTES'),
      dir: path.join(state, 'cache')
    },
    // The default one-owner authentication. Unset it and every write is
    // refused; replace App#ownerOf to use your own accounts instead.
    token: env.BRIDGE_TOKEN || '',
    owner: env.BRIDGE_OWNER || 'default'
  }
}

class App {
  /**
   * @param {object} o
   * @param {object} o.cfg      config()
   * @param {object} o.db
   * @param {object} o.ix       Indexd
   * @param {object} [o.policy] lib/policy.cjs (default: one owner, no limits)
   * @param {(req) => string|null} [o.ownerOf]  request -> owner id
   */
  constructor ({ cfg, db, ix, policy = null, ownerOf = null, fetch: fetchImpl = globalThis.fetch, now = Date.now, log = () => {} }) {
    this.cfg = cfg; this.db = db; this.ix = ix; this.fetch = fetchImpl; this.now = now; this.log = log
    this.policy = policy || policym.single(cfg.owner)
    this.session = null
    this.sdkSession = null
    this.busy = {}; this.again = {}
    this.cache = new BlockCache({ ...cfg.cache, db, log })
    this.gateway = new Gateway({
      db,
      sdk: () => this.sdkSession,
      plans: this.policy.plans,
      accountOf: (id) => this.policy.accountOf(id),
      cache: this.cache,
      log
    })
    this.routing = new routingm.Routing({ db, cfg: { origin: cfg.origin, state: cfg.state } })
    this.provider = new Provider({ url: cfg.providerUrl, fetch: fetchImpl })
    this.warm = (cid, data) => this.gateway.warm(cid, data)
    this.kick = () => setImmediate(() => this.pinTick())
    this.ownerOf = ownerOf || ((req) => this.bearerOwner(req))
  }

  /** The default door: one constant-time bearer token for one owner. */
  bearerOwner (req) {
    const given = /^Bearer (.+)$/.exec(String(req.headers.authorization || ''))
    if (!this.cfg.token || !given) return null
    const a = Buffer.from(given[1]); const b = Buffer.from(this.cfg.token)
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null
    return this.policy.ownerId || this.cfg.owner
  }

  /** The owner of this request, or 401. */
  owner (req) {
    const id = this.ownerOf(req)
    if (!id) fail(401, 'a bearer token is required')
    return id
  }

  /** The owner's plan, or 402 when they may not pin publicly at all. */
  planOf (ownerId) {
    const acct = this.policy.accountOf(ownerId)
    const plan = this.policy.plans[acct ? acct.plan : 'default']
    if (!plan) fail(402, 'no plan for that owner')
    if (!plan.publicPins) fail(402, `the ${plan.title} plan does not include public pins`)
    return plan
  }

  sdk () { return this.sdkSession }

  /* ------------------------------------------------------------ routes */

  async request (req) {
    try {
      if (req.path.startsWith('/ipfs/')) return await this.gateway.handle(req)
      if (req.path.startsWith('/routing/v1/')) return this.routing.handle(req)
      if (req.path === '/pins' || req.path.startsWith('/pins/')) return await this.pinningApi(req)
      return await this.api(req)
    } catch (e) {
      if (e instanceof HttpError) return json(e.status, { error: e.message })
      this.log(`${req.method} ${req.path}: ${e.stack || e.message}`)
      return json(500, { error: 'something went wrong on our side' })
    }
  }

  async api (req) {
    const route = `${req.method} ${req.path}`
    const b = () => {
      try { return JSON.parse(req.body ? req.body.toString('utf8') : '{}') } catch { return fail(400, 'not JSON') }
    }

    if (route === 'GET /api/health') {
      return json(200, { ok: true, storage: Boolean(this.sdkSession), pins: this.db.prepare('SELECT COUNT(*) AS n FROM pins').get().n })
    }

    if (route === 'GET /api/blocks') {
      // The provider sidecar's key list: one canonical CID per line of every
      // block held. INTERNAL — it arrives over the container network, never
      // through the edge. A reverse proxy stamps x-forwarded-for on
      // everything it proxies, so a request carrying one is from outside and
      // gets the same 404 as any unknown route.
      if (req.headers['x-forwarded-for']) fail(404, 'not found')
      const lines = (function * (db) { for (const cid of pins.allBlocks(db)) yield cid + '\n' })(this.db)
      return { status: 200, headers: { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' }, body: lines }
    }

    if (route === 'GET /api/pins') {
      const owner = this.owner(req)
      return json(200, { pins: pins.forAccount(this.db, owner) })
    }

    if (route === 'POST /api/pins') {
      // Way in #1: an object the client already stored on its own Sia
      // account. We pin the shared object under the service account, so it
      // outlives the client's device and plan, and index it. No bytes move.
      const owner = this.owner(req)
      const plan = this.planOf(owner)
      const s = this.sdk()
      if (!s) fail(503, 'storage is not connected right now')
      const body = b()
      if (!body.cid || !body.shareUrl) fail(400, 'cid and shareUrl are needed')
      if (!String(body.shareUrl).startsWith(this.cfg.indexd.appUrl + '/')) fail(400, 'shareUrl must be a share link from this indexer')
      let row
      try {
        row = await pins.register(this.db, s, {
          accountId: owner,
          plan,
          name: body.name ? String(body.name).slice(0, 200) : null,
          cid: String(body.cid),
          shareUrl: String(body.shareUrl),
          warm: this.warm
        })
      } catch (e) { fail(e.status || 400, e.message) }
      this.kick() // announce it to the DHT
      return json(201, { cid: String(body.cid), size: row.size, url: `${this.cfg.origin}/ipfs/${body.cid}` })
    }

    if (route === 'POST /api/uploads') {
      const owner = this.owner(req)
      this.planOf(owner)
      const body = b()
      const up = uploads.create(this.db, this.cfg.state, { accountId: owner, name: body.name ? String(body.name).slice(0, 200) : null, folder: Boolean(body.folder), at: this.now() })
      return json(201, uploads.view(up))
    }

    const putFile = /^\/api\/uploads\/([0-9a-f-]{36})\/files\/(.+)$/.exec(req.path)
    if (putFile && req.method === 'PUT') {
      const owner = this.owner(req)
      const plan = this.planOf(owner)
      const up = uploads.get(this.db, putFile[1])
      if (!up || up.account_id !== owner) fail(404, 'no such upload')
      if (!req.stream) fail(400, 'a body is required')
      let out
      try {
        out = await uploads.put(this.db, this.cfg.state, up, decodeURIComponent(putFile[2]), req.stream, { room: this.room(owner, plan, up), declared: declaredLength(req) })
      } catch (e) { if (e.status) throw e; fail(400, e.message) }
      return json(200, { path: out.path, bytes: out.bytes, upload: uploads.view(out.upload) })
    }

    const finish = /^\/api\/uploads\/([0-9a-f-]{36})\/finish$/.exec(req.path)
    if (finish && req.method === 'POST') {
      const owner = this.owner(req)
      const plan = this.planOf(owner)
      const up = uploads.get(this.db, finish[1])
      if (!up || up.account_id !== owner) fail(404, 'no such upload')
      return json(202, await this.finishUpload(owner, plan, up))
    }

    // `curl -T file https://host/api/upload?filename=x` — one file, one call.
    if (req.path === '/api/upload' && (req.method === 'PUT' || req.method === 'POST')) {
      const owner = this.owner(req)
      const plan = this.planOf(owner)
      if (!req.stream) fail(400, 'a body is required')
      const filename = String(req.query.filename || 'file').replace(/[/\\]/g, '_')
      const up = uploads.create(this.db, this.cfg.state, { accountId: owner, name: req.query.name ? String(req.query.name).slice(0, 200) : filename, folder: false, at: this.now() })
      try {
        await uploads.put(this.db, this.cfg.state, up, filename, req.stream, { room: this.room(owner, plan, up), declared: declaredLength(req) })
      } catch (e) { if (e.status) throw e; fail(400, e.message) }
      return json(202, await this.finishUpload(owner, plan, uploads.get(this.db, up.id)))
    }

    fail(404, 'not found')
  }

  /** Bytes the plan has left for this upload: storage, minus public pins, minus what the session already holds. */
  room (ownerId, plan, up) {
    return Math.max(0, plan.storageBytes - pins.publicBytes(this.db, ownerId) - (up ? up.bytes : 0))
  }

  /**
   * Hash, write the CAR, queue the request; the worker is nudged. 202 with
   * the CID the caller can already share — the bytes are still going to Sia.
   */
  async finishUpload (owner, plan, up) {
    let made
    try { made = await uploads.finish(this.db, this.cfg.state, up) } catch (e) { if (e.status || e instanceof unixfs.NotServable) throw e; fail(400, e.message) }
    if (pins.publicBytes(this.db, owner) + made.size > plan.storageBytes && !pins.get(this.db, pins.cidKey(made.cid))) {
      uploads.drop(this.db, this.cfg.state, up.id)
      fail(402, 'that would exceed the space on your plan')
    }
    const r = pins.createRequest(this.db, {
      accountId: owner,
      cid: made.cid.toString(),
      name: up.name,
      meta: { source: 'upload', bytes: made.bytes, files: up.files },
      uploadId: up.id
    })
    this.kick()
    return { cid: made.cid.toString(), url: `${this.cfg.origin}/ipfs/${made.cid}/`, bytes: made.bytes, request: pins.pinStatus(r) }
  }

  /** The IPFS Pinning Service API, as specified. */
  async pinningApi (req) {
    const { method, path: p } = req
    const owner = this.owner(req)
    const view = (r) => pins.pinStatus(r, { delegates: [] })

    if (p === '/pins') {
      if (method === 'GET') {
        const q = req.query
        const out = pins.listRequests(this.db, owner, {
          cid: q.cid ? q.cid.split(',').filter(Boolean).slice(0, 10) : [],
          name: q.name || null,
          status: q.status ? q.status.split(',').filter(Boolean) : [],
          limit: Math.min(Math.max(Number(q.limit) || 10, 1), 1000),
          before: q.before || null
        })
        return json(200, { count: out.count, results: out.results.map(view) })
      }
      if (method === 'POST') {
        this.planOf(owner)
        const body = (() => { try { return JSON.parse(req.body ? req.body.toString('utf8') : '{}') } catch { return fail(400, 'not JSON') } })()
        if (!body.cid) fail(400, 'cid is required')
        let r
        try { r = pins.createRequest(this.db, { accountId: owner, cid: String(body.cid), name: body.name, origins: body.origins, meta: body.meta }) } catch (e) { fail(400, e.message) }
        this.kick()
        return json(202, view(r))
      }
      fail(405, 'GET or POST')
    }

    const m = /^\/pins\/([0-9a-f-]{36})$/.exec(p)
    if (!m) fail(404, 'no such pin request')
    const r = pins.getRequest(this.db, m[1])
    if (!r || r.account_id !== owner) fail(404, 'no such pin request')
    if (method === 'GET') return json(200, view(r))
    if (method === 'POST') {
      const body = (() => { try { return JSON.parse(req.body ? req.body.toString('utf8') : '{}') } catch { return fail(400, 'not JSON') } })()
      if (!body.cid) fail(400, 'cid is required')
      const s = this.sdk()
      if (r.status === pins.STATUS.pinned && !s) fail(503, 'storage is not connected')
      let fresh
      try { fresh = await pins.replaceRequest(this.db, s, r.id, { cid: String(body.cid), name: body.name, origins: body.origins, meta: body.meta }) } catch (e) { fail(400, e.message) }
      this.kick()
      return json(202, view(fresh))
    }
    if (method === 'DELETE') {
      const s = this.sdk()
      if (r.status === pins.STATUS.pinned && !s) fail(503, 'storage is not connected')
      await pins.deleteRequest(this.db, s, r.id) // unregisters the pin itself
      return json(202, view(r))
    }
    fail(405, 'GET, POST or DELETE')
  }

  /* ------------------------------------------------------------- ticks */

  /** Fetch a CAR for the pin worker from the first gateway that has it. */
  async fetchCar (cid) {
    let last = null
    for (const g of this.cfg.carGateways) {
      try {
        const res = await this.fetch(`${g.replace(/\/+$/, '')}/ipfs/${cid}?format=car&dag-scope=all`, { headers: { accept: CAR_TYPE }, signal: AbortSignal.timeout(120e3) })
        if (res.ok && res.body && /vnd\.ipld\.car/.test(res.headers.get('content-type') || '')) return res.body
        last = new Error(`${g} → ${res.status}`)
      } catch (e) { last = e }
    }
    throw last || new Error('no CAR gateway configured')
  }

  /** The CAR for a queued request: the file an upload left on disk, or a gateway's. */
  async carFor (r) {
    if (r.upload_id) return uploads.carStream(this.cfg.state, r.upload_id)
    return Readable.fromWeb(await this.fetchCar(r.cid))
  }

  /** One run of `name` at a time. A call that finds it busy is not lost: it runs right after. */
  async once (name, fn) {
    if (this.busy[name]) { this.again[name] = fn; return null }
    this.busy[name] = true
    try { return await fn() } catch (e) { this.log(`${name}: ${e.message}`); return null } finally {
      this.busy[name] = false
      const again = this.again[name]
      if (again) { delete this.again[name]; setImmediate(() => this.once(name, again)) }
    }
  }

  pinTick () {
    return this.once('pins', async () => {
      const s = this.sdk()
      if (!s) return null
      let did = 0
      while (await pins.work(this.db, s, {
        source: (r) => this.carFor(r),
        plans: this.policy.plans,
        accountOf: (id) => this.policy.accountOf(id),
        warm: this.warm,
        at: this.now()
      })) { if (++did >= 20) break }
      if (did) uploads.sweep(this.db, this.cfg.state, this.now())
      await pins.announce(this.db, (cid) => this.provider.provide(cid), { at: this.now(), log: this.log })
      return did
    })
  }

  /** Stay peered with the gateways on the line: the cold first byte lives or dies by it. */
  gatewayTick () {
    return this.once('gateways', async () => {
      const r = await this.provider.keepGatewaysClose({ at: this.now() })
      if (r.added.length || r.forgotten.length) this.log(`gateways: peered ${r.added.map((g) => `${g.id} (${g.agent})`).join(', ') || 'none'}; forgot ${r.forgotten.length}; ${r.peered} peered`)
      return r
    })
  }

  /** Open the Sia session; the daemon runs without one and keeps trying. */
  async connectStorage () {
    const stored = sia.loadSession(this.cfg.siaFile)
    if (!stored) { this.log(`no Sia session at ${this.cfg.siaFile} — run \`bridged connect\``); return null }
    const session = sia.follow(this.cfg.siaFile, stored, this.cfg.indexd.appUrl, this.log)
    this.sdkSession = await sia.open(session, this.cfg.app)
    this.log(`storage connected at ${session.indexer}`)
    return this.sdkSession
  }
}

/* ---------------------------------------------------------------- http */

// Requests whose body is a stream, not a buffer: uploads can be far larger
// than memory.
const STREAMED = (method, url) => (method === 'PUT' || method === 'POST') && /^\/api\/upload/.test(url)

/** Content-Length, when the client sent a usable one. */
function declaredLength (req) {
  const n = Number(req.headers['content-length'])
  return Number.isFinite(n) && n >= 0 ? n : null
}

function readBody (req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = []; let n = 0
    req.on('data', (c) => { n += c.length; if (n > limit) { reject(new HttpError(413, 'too big')); req.destroy() } else chunks.push(c) })
    req.on('end', () => resolve(Buffer.concat(chunks)))
    req.on('error', reject)
  })
}

function serve (app, port, log) {
  const server = http.createServer(async (req, res) => {
    let out
    try {
      const u = new URL(req.url, 'http://x')
      const streamed = STREAMED(req.method, req.url)
      const body = streamed ? null : await readBody(req, JSON_LIMIT)
      out = await app.request({
        method: req.method,
        url: req.url,
        path: u.pathname,
        query: Object.fromEntries(u.searchParams),
        headers: req.headers,
        body,
        stream: streamed ? req : null
      })
    } catch (e) {
      out = e instanceof HttpError ? json(e.status, { error: e.message }) : json(500, { error: 'something went wrong on our side' })
    }
    res.writeHead(out.status, { 'x-content-type-options': 'nosniff', ...out.headers })
    if (req.method === 'HEAD' || out.body == null) {
      if (out.body && typeof out.body.destroy === 'function') out.body.destroy()
      res.end(); return
    }
    if (Buffer.isBuffer(out.body) || typeof out.body === 'string') { res.end(out.body); return }
    const stream = out.body instanceof Readable ? out.body : Readable.from(out.body)
    stream.on('error', (e) => { log(`stream ${req.url}: ${e.message}`); res.destroy() })
    stream.pipe(res)
  })
  server.listen(port, '0.0.0.0', () => log(`bridged listening on :${port}`))
  return server
}

/* ---------------------------------------------------------------- main */

async function main (argv) {
  const cfg = config()
  const log = (line) => console.log(`${new Date().toISOString()} ${line}`)
  fs.mkdirSync(cfg.state, { recursive: true, mode: 0o700 })
  const db = dbm.open(cfg.dbFile)
  const ix = new Indexd(cfg.indexd)
  const cmd = argv[2] || 'serve'

  if (cmd === 'connect') {
    const made = await sia.bootstrap(ix, { indexer: cfg.indexd.appUrl, file: cfg.siaFile, app: cfg.app })
    log(`Sia session written to ${cfg.siaFile} (app key ${Buffer.from(made.publicKey).toString('hex').slice(0, 16)}…)`)
    return
  }
  if (cmd !== 'serve') { console.error('usage: bridged [serve|connect]'); process.exit(2) }

  // Constructing the App makes the peer key and the sidecar's kubo identity
  // in the state directory if they are not there yet.
  const app = new App({ cfg, db, ix, log })
  log(`peer ${app.routing.peer.id}`)
  await app.connectStorage().catch((e) => log(`storage: ${e.message} — retrying in the background`))
  if (!app.sdkSession) {
    const retry = setInterval(() => { app.connectStorage().then((s) => { if (s) clearInterval(retry) }).catch(() => {}) }, 60e3)
    retry.unref()
  }
  serve(app, cfg.port, log)
  setInterval(() => app.pinTick(), 60e3).unref()
  setInterval(() => app.gatewayTick(), 5 * 60e3).unref()
  app.pinTick(); app.gatewayTick()
}

if (require.main === module) {
  main(process.argv).catch((e) => { console.error(e.stack || e.message); process.exit(1) })
}

module.exports = { App, config, serve, HttpError, json }
