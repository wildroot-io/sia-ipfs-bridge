'use strict'
/**
 * The bridge, offline. The indexer and the provider sidecar are fakes behind
 * one `fetch`, and the Sia session is an in-memory object store that counts
 * every range read — the number the cache exists to keep down.
 *
 * The CAR fixtures are REAL: kubo (`ipfs --offline add`) builds a small site
 * in a temporary repo, so the gateway is tested against exactly the bytes the
 * network would produce. Without `ipfs` on PATH those tests skip; they are
 * never faked, because "we agree with ourselves about what a DAG is" would
 * prove nothing.
 */
const fs = require('fs')
const os = require('os')
const path = require('path')
const crypto = require('crypto')
const { execFileSync } = require('child_process')
const { Readable } = require('stream')

const dbm = require('../lib/db.cjs')
const { Indexd } = require('../lib/indexd.cjs')
const policym = require('../lib/policy.cjs')
const { App } = require('../bin/bridged.cjs')

const ORIGIN = 'https://bridge.test'
const ADMIN = 'http://indexd:9980/api'
const APP = 'https://indexer.bridge.test'
const PROVIDER = 'http://provider:5001'
const TOKEN = 'test-token'
const OWNER = 'owner-1'
const SCRATCH = process.env.SCRATCH || fs.mkdtempSync(path.join(os.tmpdir(), 'bridge-test-'))

const MiB = 1024 * 1024

/** Two plans, so the metering and quota paths have something to enforce. */
const PLANS = {
  publish: { title: 'Publish', publicPins: true, storageBytes: 100 * MiB, publicBytesPerMonth: 10 * MiB },
  keep: { title: 'Keep', publicPins: false, storageBytes: 10 * MiB, publicBytesPerMonth: 0 }
}

/** The service's Sia session: an object store with share links. */
class FakeSia {
  constructor () { this.objects = new Map(); this.pinned = new Set(); this.chunk = 1000 }
  put (data) { const oid = crypto.createHash('sha256').update(data).digest('hex').slice(0, 32); this.objects.set(oid, Buffer.from(data)); return oid }
  share (oid) { return `${APP}/share/${oid}#key` }
  obj (oid) { const d = this.objects.get(oid); return { id: () => oid, size: () => BigInt(d.length) } }
  async sharedObject (url) { const oid = url.split('/').pop().split('#')[0]; if (!this.objects.has(oid)) throw new Error('shared object not found'); return this.obj(oid) }
  async object (oid) { if (!this.objects.has(oid)) throw new Error('object not found'); return this.obj(oid) }
  async pinObject (o) { this.pinned.add(o.id()) }
  async deleteObject (oid) { if (!this.objects.has(oid)) throw new Error('not found'); this.objects.delete(oid); this.pinned.delete(oid) }

  download (o, { offset, length }) {
    this.reads = (this.reads || 0) + 1 // every range read is host bandwidth the cache should spare
    const d = this.objects.get(o.id()).subarray(Number(offset), Number(offset) + Number(length))
    const chunks = []; for (let i = 0; i < d.length; i += this.chunk) chunks.push(d.subarray(i, i + this.chunk))
    return Readable.toWeb(Readable.from(chunks))
  }

  async upload (_obj, webStream) {
    const parts = []; for await (const c of Readable.fromWeb(webStream)) parts.push(Buffer.from(c))
    return this.obj(this.put(Buffer.concat(parts)))
  }
}

/**
 * The provider sidecar's kubo RPC: `routing provide` remembers what was
 * announced; `swarm peers`/`peering` are the line and the config.
 */
class FakeProvider {
  constructor () { this.provided = []; this.down = false; this.peers = []; this.peering = [] }
  handle (method, url) {
    if (this.down) return reply(502, 'provider is down')
    const u = new URL(url)
    if (method !== 'POST') return reply(405, 'POST')
    const arg = u.searchParams.get('arg')
    switch (u.pathname) {
      case '/api/v0/routing/provide': this.provided.push(arg); return reply(200, JSON.stringify({ ID: arg, Type: 4 }) + '\n', 'application/json')
      case '/api/v0/swarm/peers': return { Peers: this.peers.map((p) => ({ Addr: '/ip4/10.0.0.1/tcp/4001', Peer: p.id, Identify: u.searchParams.get('identify') === 'true' ? { ID: p.id, AgentVersion: p.agent, Addresses: [] } : undefined })) }
      case '/api/v0/swarm/peering/ls': return { Peers: this.peering.map((id) => ({ ID: id, Addrs: [] })) }
      case '/api/v0/swarm/peering/add': { const id = arg.replace(/^\/p2p\//, ''); if (!this.peering.includes(id)) this.peering.push(id); return { ID: id, Status: 'success' } }
      case '/api/v0/swarm/peering/rm': this.peering = this.peering.filter((id) => id !== arg); return { ID: arg, Status: 'success' }
      default: return reply(404, 'no such RPC')
    }
  }
}

function reply (status, text, type = 'text/plain') { return { __status: status, __text: text, __type: type } }

/** One fetch for all fakes; anything else is a test bug, not a network call. */
function router (fakes) {
  return async (url, init = {}) => {
    const method = (init.method || 'GET').toUpperCase()
    const u = new URL(url)
    let out
    if (u.origin === PROVIDER) out = fakes.provider.handle(method, url, init)
    else if (fakes.gateways && fakes.gateways[u.origin]) out = fakes.gateways[u.origin](method, url, init)
    else throw new Error(`unexpected fetch ${method} ${url}`)
    if (out && out.__status) return new Response(out.__text, { status: out.__status, headers: { 'content-type': out.__type } })
    if (out instanceof Response) return out
    return new Response(out === null || out === undefined ? '' : JSON.stringify(out), { status: 200, headers: { 'content-type': 'application/json' } })
  }
}

/** A bridge with everything faked but the CARs. */
function fresh ({ now, plan = 'publish' } = {}) {
  const fakes = { sia: new FakeSia(), provider: new FakeProvider(), gateways: {}, log: [] }
  const fetch = router(fakes)
  const dir = fs.mkdtempSync(path.join(SCRATCH, 'state-'))
  const cfg = {
    origin: ORIGIN,
    port: 0,
    state: dir,
    dbFile: path.join(dir, 'bridge.db'),
    siaFile: path.join(dir, 'sia.json'),
    app: { id: 'a'.repeat(64), name: 'test', description: 'test', serviceUrl: ORIGIN },
    indexd: { adminUrl: ADMIN, adminPassword: 'pw', appUrl: APP },
    providerUrl: PROVIDER,
    carGateways: ['https://car.test'],
    cache: { dir: path.join(dir, 'cache'), memoryBytes: 16 * MiB, diskBytes: 16 * MiB },
    token: TOKEN,
    owner: OWNER
  }
  const db = dbm.open(cfg.dbFile)
  const ix = new Indexd({ ...cfg.indexd, fetch })
  const clock = { t: now || Date.parse('2026-09-02T12:00:00Z') }
  const policy = { ...policym.of(PLANS, (id) => (id === OWNER ? { id, plan } : null)), ownerId: OWNER }
  const app = new App({ cfg, db, ix, policy, fetch, log: (l) => fakes.log.push(l), now: () => clock.t })
  app.sdkSession = fakes.sia
  app.gateway.window = MiB // a small window, so a fixture can span several
  app.kick = () => {} // tests drive pinTick() themselves
  return { app, db, ix, fakes, cfg, clock }
}

/** Empty both cache tiers: what a pin looks like once it has gone cold. */
async function chill (cache) {
  await cache.idle()
  cache.map.clear(); cache.size = 0
  for (const r of cache.db.prepare('SELECT key, length FROM cache_blocks').all()) await cache.evict(r.key, r.length)
}

/** Drive the app like the socket layer would. */
async function call (app, method, url, { body = null, headers = {}, auth = false, stream = null } = {}) {
  const u = new URL(url, 'http://x')
  const raw = body === null ? null : Buffer.isBuffer(body) ? body : Buffer.from(typeof body === 'string' ? body : JSON.stringify(body))
  const out = await app.request({
    method,
    url,
    path: u.pathname,
    query: Object.fromEntries(u.searchParams),
    headers: { ...(auth ? { authorization: `Bearer ${TOKEN}` } : {}), ...headers },
    body: raw,
    stream
  })
  const chunks = []
  if (out.body) { if (Buffer.isBuffer(out.body)) chunks.push(out.body); else for await (const c of out.body) chunks.push(Buffer.from(c)) }
  const buf = Buffer.concat(chunks)
  let json = null
  if (/json/.test(out.headers['content-type'] || '')) { try { json = JSON.parse(buf.toString()) } catch {} }
  return { status: out.status, headers: out.headers, buf, text: buf.toString('utf8'), json }
}

/* --------------------------------------------------------- fixtures */

let site = null
/** A real site, hashed by a real kubo. null when there is no `ipfs` binary. */
function ipfsSite () {
  if (site) return site
  let bin = null
  for (const c of [process.env.IPFS_BIN, path.join(os.homedir(), '.local/bin/ipfs'), 'ipfs']) {
    try { if (c) { execFileSync(c, ['version'], { stdio: 'ignore' }); bin = c; break } } catch {}
  }
  if (!bin) return null
  const dir = fs.mkdtempSync(path.join(SCRATCH, 'site-'))
  const repo = path.join(dir, 'repo'); const src = path.join(dir, 'src')
  fs.mkdirSync(path.join(src, 'sub'), { recursive: true })
  fs.writeFileSync(path.join(src, 'index.html'), '<!doctype html><h1>hello from sia</h1>')
  fs.writeFileSync(path.join(src, 'style.css'), 'h1 { color: rebeccapurple }')
  fs.writeFileSync(path.join(src, 'sub', 'page.html'), '<p>a sub page</p>')
  const big = Buffer.alloc(700 * 1024); for (let i = 0; i < big.length; i++) big[i] = (i * 7 + (i >> 10)) & 0xff
  fs.writeFileSync(path.join(src, 'big.bin'), big)
  const env = { ...process.env, IPFS_PATH: repo }
  const run = (args) => execFileSync(bin, args, { env, stdio: ['ignore', 'pipe', 'ignore'] }).toString().trim()
  run(['init', '--profile=test'])
  const rootV1 = run(['--offline', 'add', '-r', '-Q', '--cid-version=1', src])
  const rootV0 = run(['--offline', 'add', '-r', '-Q', '--cid-version=0', src])
  const carV1 = execFileSync(bin, ['--offline', 'dag', 'export', rootV1], { env, maxBuffer: 64 * MiB })
  const carV0 = execFileSync(bin, ['--offline', 'dag', 'export', rootV0], { env, maxBuffer: 64 * MiB })
  site = {
    bin,
    src,
    add: (args) => run(['--offline', 'add', '-Q', ...args]),
    rootV1,
    rootV0,
    carV1,
    carV0,
    big,
    files: {
      'index.html': fs.readFileSync(path.join(src, 'index.html')),
      'style.css': fs.readFileSync(path.join(src, 'style.css')),
      'sub/page.html': fs.readFileSync(path.join(src, 'sub/page.html'))
    }
  }
  return site
}

/** Register a CAR the client "already stored on Sia" as a public pin. */
async function pinCar (app, fakes, archive, root, { auth = true } = {}) {
  const oid = fakes.sia.put(archive)
  const res = await call(app, 'POST', '/api/pins', { auth, body: { cid: root, shareUrl: fakes.sia.share(oid) } })
  return { oid, res }
}

module.exports = { ORIGIN, APP, PROVIDER, TOKEN, OWNER, PLANS, SCRATCH, MiB, FakeSia, FakeProvider, fresh, chill, call, ipfsSite, pinCar }
