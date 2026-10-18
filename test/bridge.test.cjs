'use strict'
/**
 * The bridge, end to end against real CARs.
 *
 *   node --test test/
 *
 * Tests that need a real `ipfs` binary skip without one; the two that drive a
 * whole kubo (HTTP retrieval, and the provider sidecar over bitswap) are the
 * only proof that the thing is universal rather than merely self-consistent.
 */
const test = require('node:test')
const assert = require('node:assert/strict')
const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const { Readable } = require('stream')

const car = require('../lib/car.cjs')
const { CID } = require('../lib/cid.cjs')
const unixfs = require('../lib/unixfs.cjs')
const pins = require('../lib/pins.cjs')
const routing = require('../lib/routing.cjs')
const { RAW, CAR, HARD_FACTOR, parseRange } = require('../lib/gateway.cjs')
const { BlockCache } = require('../lib/cache.cjs')
const { fresh, chill, call, ipfsSite, pinCar, OWNER, PLANS } = require('./harness.cjs')

/* ------------------------------------------------------- the formats */

test('CAR: header, index offsets and a bounded reader', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  for (const [root, archive] of [[s.rootV1, s.carV1], [s.rootV0, s.carV0]]) {
    const items = []
    const chunks = []; for (let i = 0; i < archive.length; i += 333) chunks.push(archive.subarray(i, i + 333))
    for await (const it of car.index(Readable.from(chunks))) items.push(it)
    assert.equal(items[0].roots.length, 1)
    assert.equal(items[0].roots[0].equals(CID.parse(root)), true)
    const blocks = items.slice(1)
    assert.ok(blocks.length >= 6, `${blocks.length} blocks`)
    // Every offset points at bytes whose hash is the CID's multihash. This is
    // the whole trick: the index is only as good as these offsets.
    for (const b of blocks) {
      const bytes = archive.subarray(b.offset, b.offset + b.length)
      assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'), b.cid.multihash.subarray(2).toString('hex'))
    }
    assert.ok(blocks.some((b) => b.cid.equals(CID.parse(root))))
  }
  // Round trip through the writer.
  const cid = CID.parse(s.rootV1)
  const hdr = car.header([cid])
  const items = []
  for await (const it of car.index(Readable.from([hdr, car.section(cid, Buffer.from('abc'))]))) items.push(it)
  assert.equal(items[1].length, 3)
  await assert.rejects(async () => { for await (const it of car.index(Readable.from([hdr.subarray(0, 5)]))) assert.ok(it) }, /truncated/)
})

test('CID: v0/v1 parse, one key for both, and text forms', () => {
  const s = ipfsSite()
  const v0 = CID.parse('QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG')
  assert.equal(v0.version, 0)
  assert.equal(v0.toV0String(), 'QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG')
  assert.equal(CID.parse(v0.toString()).key(), v0.key())
  assert.equal(v0.toString(), 'bafybeie5nqv6kd3qnfjupgvz34woh3oksc3iau6abmyajn7qvtf6d2ho34')
  assert.equal(CID.fromKey(v0.key()).toString(), v0.toString(), 'a pin key goes back to the CID')
  assert.throws(() => CID.fromKey('bafy'), /pin key/)
  if (s) {
    assert.equal(CID.parse(s.rootV1).version, 1)
    assert.equal(CID.parse(s.rootV0).toString().startsWith('bafy'), true)
  }
  assert.throws(() => CID.parse('zzz'), /CID|base/)
})

test('unixfs encoder: the DAG kubo builds, block for block', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  const file = (p, o) => unixfs.addFile(() => fs.createReadStream(p), o)
  const dir = async (p) => {
    const entries = []
    for (const n of fs.readdirSync(p)) { const f = path.join(p, n); entries.push({ name: n, entry: fs.statSync(f).isDirectory() ? await dir(f) : await file(f) }) }
    return unixfs.addDirectory(entries)
  }
  // Same root as `ipfs add -r --cid-version=1`, and every block in kubo's CAR.
  const root = await dir(s.src)
  assert.equal(root.cid.toString(), s.rootV1)
  const ours = new Map()
  for await (const b of unixfs.blocks(root)) ours.set(b.cid.key(), b.bytes)
  const theirs = new Map()
  for await (const it of car.index(Readable.from([s.carV1]), { blocks: true })) {
    if (!it.roots) theirs.set(it.cid.key(), it.data)
  }
  assert.equal(ours.size, theirs.size, 'the same number of blocks')
  for (const [k, bytes] of theirs) assert.equal(ours.get(k).equals(bytes), true, `block ${CID.fromKey(k)}`)
})

/* ---------------------------------------------------------- the pins */

test('pin: a share URL becomes a public pin without re-uploading a byte', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  const { app, db, fakes } = fresh()
  const before = fakes.sia.objects.size
  const { oid, res } = await pinCar(app, fakes, s.carV1, s.rootV1)
  assert.equal(res.status, 201, res.text)
  assert.equal(fakes.sia.objects.size, before + 1, 'the CAR the client already had; nothing re-uploaded')
  assert.equal(fakes.sia.pinned.has(oid), true, 'pinned under the SERVICE account, so it outlives the client')

  const row = db.prepare('SELECT * FROM pins').get()
  assert.equal(row.object_id, oid)
  assert.equal(row.indexed, 1)
  assert.ok(db.prepare('SELECT COUNT(*) AS n FROM blocks WHERE cid = ?').get(row.cid).n >= 6, 'every block located inside the archive')
  assert.equal(db.prepare('SELECT account_id FROM pinners').get().account_id, OWNER)

  // The claimed root has to BE a root of the archive; a share URL for the
  // wrong file is refused HERE, while indexing, and the object released.
  const objects = fakes.sia.objects.size
  const wrong = await call(app, 'POST', '/api/pins', { auth: true, body: { cid: s.rootV0, shareUrl: fakes.sia.share(oid) } })
  assert.equal(wrong.status, 400, wrong.text)
  assert.equal(fakes.sia.objects.size, objects, 'and nothing was left behind on Sia')
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM pins').get().n, 1)

  // Unauthenticated writes never land.
  assert.equal((await call(app, 'POST', '/api/pins', { body: { cid: s.rootV1, shareUrl: fakes.sia.share(oid) } })).status, 401)
})

test('pin: a plan without public pins is refused, and the space ceiling holds', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  const keep = fresh({ plan: 'keep' })
  const refused = await pinCar(keep.app, keep.fakes, s.carV1, s.rootV1)
  assert.equal(refused.res.status, 402, refused.res.text)
  assert.equal(keep.db.prepare('SELECT COUNT(*) AS n FROM pins').get().n, 0)
})

test('pin worker: a queued CID is fetched, uploaded, indexed — and cut off at the room left BEFORE the bytes are bought', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  const { app, db, fakes } = fresh()
  fakes.gateways['https://car.test'] = () => new Response(s.carV1, { status: 200, headers: { 'content-type': 'application/vnd.ipld.car; version=1' } })

  const queued = await call(app, 'POST', '/pins', { auth: true, body: { cid: s.rootV1, name: 'site' } })
  assert.equal(queued.status, 202, queued.text)
  assert.equal(queued.json.status, 'queued')
  assert.equal(await app.pinTick(), 1)
  const listed = await call(app, 'GET', '/pins', { auth: true })
  assert.equal(listed.json.results[0].status, 'pinned')
  assert.equal(db.prepare('SELECT indexed FROM pins').get().indexed, 1)

  // The gateway serves it, which is the only proof the upload and the index agree.
  const got = await call(app, 'GET', `/ipfs/${s.rootV1}/index.html`)
  assert.equal(got.status, 200)
  assert.equal(got.text, s.files['index.html'].toString())

  // A second, much larger CAR against a nearly full plan: the stream is cut
  // at the room left, so we never pay a host for bytes we will refuse.
  const big = Buffer.concat([s.carV1, crypto.randomBytes(PLANS.publish.storageBytes)])
  fakes.gateways['https://car.test'] = () => new Response(big, { status: 200, headers: { 'content-type': 'application/vnd.ipld.car; version=1' } })
  const other = await call(app, 'POST', '/pins', { auth: true, body: { cid: s.rootV0 } })
  assert.equal(other.status, 202)
  await app.pinTick()
  const after = pins.getRequest(db, other.json.requestid)
  assert.equal(after.status, 'failed', 'over quota is final, not retried')
  assert.match(after.info, /exceed the space/)
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM pins').get().n, 1, 'and nothing was left half-pinned')
})

test('unpin: the last pinner out deletes the object; storage goes first', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  const { app, db, fakes } = fresh()
  const { oid } = await pinCar(app, fakes, s.carV1, s.rootV1)
  const req = await call(app, 'POST', '/pins', { auth: true, body: { cid: s.rootV1 } })
  await app.pinTick()

  // A storage refusal leaves the pin whole and retryable.
  const real = fakes.sia.deleteObject.bind(fakes.sia)
  fakes.sia.deleteObject = async () => { throw new Error('hosts unreachable') }
  await assert.rejects(() => call(app, 'DELETE', `/pins/${req.json.requestid}`, { auth: true }).then((r) => { if (r.status >= 500) throw new Error(r.text) }))
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM pins').get().n, 1, 'still pinned')
  assert.equal(fakes.sia.pinned.has(oid), true)

  fakes.sia.deleteObject = real
  const gone = await call(app, 'DELETE', `/pins/${req.json.requestid}`, { auth: true })
  assert.equal(gone.status, 202, gone.text)
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM pins').get().n, 0)
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM blocks').get().n, 0)
  assert.equal(fakes.sia.objects.has(oid), false, 'the Sia object went with it — no rent on data nobody pins')
  assert.equal((await call(app, 'GET', `/ipfs/${s.rootV1}/`)).status, 404)
})

test('Pinning Service API: the spec shapes, and a replace stops billing the old CID', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  const { app, db, fakes } = fresh()
  fakes.gateways['https://car.test'] = () => new Response(s.carV1, { status: 200, headers: { 'content-type': 'application/vnd.ipld.car; version=1' } })

  assert.equal((await call(app, 'GET', '/pins')).status, 401, 'a bearer token is the door')
  const made = await call(app, 'POST', '/pins', { auth: true, body: { cid: s.rootV1, name: 'a', meta: { k: 'v' } } })
  assert.equal(made.status, 202)
  assert.match(made.json.requestid, /^[0-9a-f-]{36}$/)
  assert.equal(made.json.pin.cid, s.rootV1)
  assert.deepEqual(made.json.pin.meta, { k: 'v' })
  assert.deepEqual(made.json.delegates, [])
  await app.pinTick()

  const one = await call(app, 'GET', `/pins/${made.json.requestid}`, { auth: true })
  assert.equal(one.json.status, 'pinned')
  const filtered = await call(app, 'GET', `/pins?status=pinned&cid=${s.rootV1}`, { auth: true })
  assert.equal(filtered.json.count, 1)
  assert.equal((await call(app, 'GET', '/pins?status=failed', { auth: true })).json.count, 0)
  assert.equal((await call(app, 'GET', '/pins/00000000-0000-0000-0000-000000000000', { auth: true })).status, 404)

  // Replace: the old CID is unpinned in the same breath, not left on the bill.
  fakes.gateways['https://car.test'] = () => new Response(s.carV0, { status: 200, headers: { 'content-type': 'application/vnd.ipld.car; version=1' } })
  const replaced = await call(app, 'POST', `/pins/${made.json.requestid}`, { auth: true, body: { cid: s.rootV0 } })
  assert.equal(replaced.status, 202, replaced.text)
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM pinners WHERE cid = ?').get(pins.cidKey(s.rootV1)).n, 0, 'the old CID is no longer this owner\'s')
  assert.equal(pins.publicUsage(db, OWNER).count, 0, 'and no longer on their usage')
})

/* ------------------------------------------------------- the gateway */

test('gateway: files, paths, index.html, Range, raw and CAR formats, metering', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  const { app, db, fakes } = fresh()
  const { oid } = await pinCar(app, fakes, s.carV1, s.rootV1)
  const root = s.rootV1

  const index = await call(app, 'GET', `/ipfs/${root}/`)
  assert.equal(index.status, 200)
  assert.equal(index.text, s.files['index.html'].toString(), 'a directory serves its index.html')
  assert.equal(index.headers['content-type'], 'text/html; charset=utf-8')
  assert.match(index.headers['cache-control'], /immutable/)
  assert.equal(index.headers['x-ipfs-roots'], root)
  assert.equal((await call(app, 'GET', `/ipfs/${root}`)).status, 200)
  const css = await call(app, 'GET', `/ipfs/${root}/style.css`)
  assert.equal(css.headers['content-type'], 'text/css; charset=utf-8')
  assert.equal(css.text, s.files['style.css'].toString())
  assert.equal((await call(app, 'GET', `/ipfs/${root}/sub/page.html`)).text, s.files['sub/page.html'].toString())
  assert.equal((await call(app, 'GET', `/ipfs/${root}/missing.html`)).status, 404)
  assert.equal((await call(app, 'GET', `/ipfs/${root}/style.css/x`)).status, 404)
  assert.equal((await call(app, 'GET', '/ipfs/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi/')).status, 404)
  assert.equal((await call(app, 'GET', '/ipfs/garbage/')).status, 400)

  const head = await call(app, 'HEAD', `/ipfs/${root}/big.bin`)
  assert.equal(head.status, 200)
  assert.equal(head.headers['content-length'], String(s.big.length))

  // A multi-block file, whole and in ranges.
  const big = await call(app, 'GET', `/ipfs/${root}/big.bin`)
  assert.equal(big.buf.equals(s.big), true)
  const r1 = await call(app, 'GET', `/ipfs/${root}/big.bin`, { headers: { range: 'bytes=262000-262999' } })
  assert.equal(r1.status, 206)
  assert.equal(r1.headers['content-range'], `bytes 262000-262999/${s.big.length}`)
  assert.equal(r1.buf.equals(s.big.subarray(262000, 263000)), true)
  const r2 = await call(app, 'GET', `/ipfs/${root}/big.bin`, { headers: { range: 'bytes=-10' } })
  assert.equal(r2.buf.equals(s.big.subarray(-10)), true)
  const r3 = await call(app, 'GET', `/ipfs/${root}/big.bin`, { headers: { range: 'bytes=700000-' } })
  assert.equal(r3.buf.equals(s.big.subarray(700000)), true)
  assert.equal((await call(app, 'GET', `/ipfs/${root}/big.bin`, { headers: { range: 'bytes=9999999-' } })).status, 416)
  assert.deepEqual(parseRange('bytes=0-1', 10), [0, 1])
  assert.equal(parseRange('bytes=5-1', 10), 'bad')

  // Trustless: a raw block, hash-checkable, and the CAR.
  const raw = await call(app, 'GET', `/ipfs/${root}?format=raw`)
  assert.equal(raw.headers['content-type'], RAW)
  assert.equal(crypto.createHash('sha256').update(raw.buf).digest('hex'), CID.parse(root).multihash.subarray(2).toString('hex'))
  assert.equal((await call(app, 'GET', `/ipfs/${root}`, { headers: { accept: RAW } })).buf.equals(raw.buf), true)
  const whole = await call(app, 'GET', `/ipfs/${root}?format=car&dag-scope=all`)
  assert.equal(whole.headers['content-type'], `${CAR}; version=1`)
  assert.equal(whole.buf.equals(s.carV1), true, 'the archive is served verbatim')
  const one = await call(app, 'GET', `/ipfs/${root}?format=car&dag-scope=block`)
  const items = []; for await (const it of car.index(Readable.from([one.buf]))) items.push(it)
  assert.equal(items.length, 2)
  assert.equal(items[1].cid.equals(CID.parse(root)), true)
  // A block deep in the DAG, by its own CID.
  const cssCid = await unixfs.resolvePath(async (c) => (await app.gateway.block(c)).data, CID.parse(root), ['style.css'])
  assert.equal((await call(app, 'GET', `/ipfs/${cssCid}?format=raw`)).status, 200)

  // Metering: bytes served count against the owner; over the cap → slow, then 429.
  const served = app.gateway.monthBytes(OWNER)
  assert.ok(served >= s.big.length + s.carV1.length, `served ${served}`)
  const cap = PLANS.publish.publicBytesPerMonth
  // The gateway meters by the real calendar month, so date the rows from it.
  const first = new Date(); first.setUTCDate(1)
  const thisMonth = first.toISOString().slice(0, 10)
  first.setUTCMonth(first.getUTCMonth() - 1)
  const lastMonth = first.toISOString().slice(0, 10)
  db.prepare('INSERT INTO egress (day, account_id, cid, bytes) VALUES (?, ?, ?, ?)').run(thisMonth, OWNER, 'x', cap)
  assert.equal(app.gateway.standing(OWNER), 'slow')
  db.prepare('UPDATE egress SET bytes = ? WHERE cid = ?').run(cap * HARD_FACTOR, 'x')
  assert.equal(app.gateway.standing(OWNER), 'refuse')
  const refused = await call(app, 'GET', `/ipfs/${root}/`)
  assert.equal(refused.status, 429)
  assert.equal(refused.headers['retry-after'], '3600')
  assert.equal(fakes.sia.pinned.has(oid), true, 'over the allowance is slowed and refused, NEVER unpinned')
  // Last month's bytes do not count.
  db.prepare("UPDATE egress SET day = ? WHERE cid = 'x'").run(lastMonth)
  assert.equal(app.gateway.standing(OWNER), 'ok')

  // A second read of the same block does not touch Sia.
  const before = fakes.sia.reads || 0
  await call(app, 'GET', `/ipfs/${root}/style.css`)
  assert.equal((await app.gateway.block(cssCid)).cached, true)
  assert.equal(fakes.sia.reads || 0, before, 'served from the cache')
})

test('cold read: one small read answers while a 16 MiB window fills the cache behind it', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  const { app, db, fakes } = fresh()
  await pinCar(app, fakes, s.carV1, s.rootV1)
  const cache = app.gateway.cache
  await cache.idle()

  // A fresh pin is warm: the index pass cached every block on the way past.
  const r0 = fakes.sia.reads || 0
  const got = await call(app, 'GET', `/ipfs/${s.rootV1}/big.bin`)
  assert.equal(got.buf.equals(s.big), true)
  assert.equal(fakes.sia.reads || 0, r0, 'a fresh pin is served from the box, not from a host')
  assert.ok(db.prepare('SELECT COUNT(*) AS n FROM cache_blocks').get().n >= 2, 'and it is on disk, so a restart keeps it')

  // Cold: a leaf answers from its own read; the window read around it lands
  // its neighbours in the cache for everything that follows.
  await chill(cache)
  const leaves = db.prepare('SELECT block_cid, offset, length FROM blocks WHERE cid = ? ORDER BY offset').all(pins.cidKey(s.rootV1))
    .map((b) => ({ ...b, cid: CID.fromKey(b.block_cid) }))
  assert.ok(leaves.length >= 6)
  const mid = leaves[Math.floor(leaves.length / 2)]
  const before = fakes.sia.reads || 0
  const first = await app.gateway.block(mid.cid)
  assert.equal(first.cached, false)
  assert.equal(first.data.length, mid.length)
  await app.gateway.idle()
  assert.ok((fakes.sia.reads || 0) - before <= 3, 'one direct read + one window (+ a read-ahead)')
  const win = Math.floor(mid.offset / app.gateway.window) * app.gateway.window
  for (const l of leaves.filter((l) => l.offset >= win && l.offset < win + app.gateway.window)) {
    assert.equal((await app.gateway.block(l.cid)).cached, true, `neighbour @${l.offset} came with the window`)
  }
})

test('cache: RAM for the recent, disk for the popular, LRU under a budget', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  const { app, db, fakes, cfg } = fresh()
  await pinCar(app, fakes, s.carV1, s.rootV1)
  const cache = app.gateway.cache
  const rows = () => db.prepare('SELECT key, length FROM cache_blocks ORDER BY key').all()
  await cache.idle()
  assert.equal(cache.dir, cfg.cache.dir)
  assert.ok(rows().length >= 2, 'a fresh pin is on disk before anyone asks for it')

  await chill(cache)
  assert.equal(rows().length, 0)
  const leaf = CID.fromKey(db.prepare('SELECT block_cid FROM blocks WHERE cid = ? ORDER BY offset LIMIT 1').get(pins.cidKey(s.rootV1)).block_cid)
  const key = leaf.multihash.toString('hex')

  // Once is a crawler; twice is an audience. Only the second ask earns disk.
  cache.map.clear(); cache.size = 0
  const one = await app.gateway.block(leaf)
  assert.equal(one.cached, false)
  await cache.idle()
  cache.map.clear(); cache.size = 0 // out of RAM, still cold on disk
  assert.equal(await cache.get(key) === null, rows().length === 0)

  // Metering does not care which tier answered.
  const before = app.gateway.monthBytes(OWNER)
  await call(app, 'GET', `/ipfs/${s.rootV1}/style.css`)
  await call(app, 'GET', `/ipfs/${s.rootV1}/style.css`)
  assert.ok(app.gateway.monthBytes(OWNER) > before, 'a cache hit is still the owner\'s bytes')

  // A block far over the RAM budget is never held in RAM.
  const small = new BlockCache({ memoryBytes: 1024 })
  small.set('k', Buffer.alloc(4096))
  assert.equal(small.peek('k'), null)
})

/* ------------------------------------------------- finding the bytes */

test('routing: a pinned CID has one provider — this gateway over HTTP; the probe CID is served', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  const { app, fakes, cfg } = fresh()
  await pinCar(app, fakes, s.carV1, s.rootV1)

  const found = await call(app, 'GET', `/routing/v1/providers/${s.rootV1}`)
  assert.equal(found.status, 200)
  assert.equal(found.json.Providers.length, 1)
  const rec = found.json.Providers[0]
  assert.equal(rec.Schema, 'peer')
  assert.match(rec.ID, /^12D3KooW/)
  assert.deepEqual(rec.Protocols, [routing.PROTOCOL])
  assert.deepEqual(rec.Addrs, ['/dns/bridge.test/tcp/443/https'])
  assert.equal(found.headers['access-control-allow-origin'], '*')

  const missing = await call(app, 'GET', '/routing/v1/providers/bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi')
  assert.equal(missing.status, 404)
  assert.deepEqual(missing.json.Providers, [])
  assert.equal((await call(app, 'GET', '/routing/v1/peers/12D3KooWx')).status, 404)

  // The identity written for the sidecar names the SAME peer, or the DHT
  // would point at a node nobody runs.
  const identity = JSON.parse(fs.readFileSync(path.join(cfg.state, routing.KUBO_IDENTITY_FILE), 'utf8'))
  assert.equal(identity.PeerID, rec.ID)
  assert.equal((fs.statSync(path.join(cfg.state, routing.KUBO_IDENTITY_FILE)).mode & 0o777), 0o600)

  // kubo probes a gateway with the empty inline CID before it will fetch.
  const probe = await call(app, 'GET', '/ipfs/bafkqaaa?format=raw')
  assert.equal(probe.status, 200)
  assert.equal(probe.buf.length, 0)
})

test('provider: the key list the sidecar reprovides, and it never leaves the box', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  const { app, db, fakes } = fresh()
  await pinCar(app, fakes, s.carV1, s.rootV1)

  const list = await call(app, 'GET', '/api/blocks')
  assert.equal(list.status, 200)
  const lines = list.text.trim().split('\n')
  assert.equal(lines.length, db.prepare('SELECT COUNT(DISTINCT block_cid) AS n FROM blocks').get().n)
  for (const l of lines) assert.doesNotThrow(() => CID.parse(l))
  assert.ok(lines.includes(s.rootV1), 'the root is in it')

  // Through the edge it does not exist: a proxy stamps x-forwarded-for on
  // everything it forwards, so anything carrying one came from outside.
  assert.equal((await call(app, 'GET', '/api/blocks', { headers: { 'x-forwarded-for': '203.0.113.7' } })).status, 404)
})

test('announce: a fresh pin reaches the DHT at pin time, and a provider that was down loses nothing', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  const { app, db, fakes } = fresh()
  fakes.gateways['https://car.test'] = () => new Response(s.carV1, { status: 200, headers: { 'content-type': 'application/vnd.ipld.car; version=1' } })
  await call(app, 'POST', '/pins', { auth: true, body: { cid: s.rootV1 } })

  fakes.provider.down = true
  await app.pinTick()
  assert.equal(db.prepare('SELECT status FROM pin_requests').get().status, 'pinned')
  assert.deepEqual(fakes.provider.provided, [])
  assert.equal(db.prepare('SELECT announced_at FROM pins').get().announced_at, null)

  fakes.provider.down = false
  await app.pinTick()
  assert.deepEqual(fakes.provider.provided, [s.rootV1], 'the root, once')
  assert.ok(db.prepare('SELECT announced_at FROM pins').get().announced_at)
  await app.pinTick()
  assert.equal(fakes.provider.provided.length, 1, 'not announced again')
})

test('gateways: the gateway nodes on the line are peered so the connection manager keeps them', async () => {
  const { app, fakes, clock } = fresh({ now: Date.parse('2026-09-03T12:00:00Z') })
  const rainbow = (n) => ({ id: `12D3KooWRainbow${n}`, agent: 'rainbow/v1.24.0 2026-07-29-7b2d936' })
  fakes.provider.peers = [rainbow(1), rainbow(2), { id: '12D3KooWCF1', agent: 'kubo/0.23.0/cloudflare' }, { id: '12D3KooWKubo1', agent: 'kubo/0.43.0' }, { id: 'QmSomeguy', agent: 'someguy/v0.16.0' }]
  fakes.provider.peering = ['12D3KooWOld'] // peered by an earlier process; not on the line
  let r = await app.gatewayTick()
  assert.deepEqual(r.added.map((g) => g.id), ['12D3KooWRainbow1', '12D3KooWRainbow2', '12D3KooWCF1'], 'gateway software only')
  assert.deepEqual(fakes.provider.peering, ['12D3KooWOld', '12D3KooWRainbow1', '12D3KooWRainbow2', '12D3KooWCF1'])

  // Nothing changes: no RPC writes, no log line.
  fakes.log.length = 0
  r = await app.gatewayTick()
  assert.deepEqual([r.added, r.forgotten, r.peered], [[], [], 4])
  assert.equal(fakes.log.length, 0)

  // A day later the one never seen is forgotten; one that dropped off an hour ago is not.
  clock.t += 23 * 3600e3
  await app.gatewayTick()
  clock.t += 3600e3 + 1
  fakes.provider.peers = [rainbow(1), { id: '12D3KooWCF1', agent: 'kubo/0.23.0/cloudflare' }]
  r = await app.gatewayTick()
  assert.deepEqual(r.forgotten, ['12D3KooWOld'])

  // The sidecar being down is a logged miss, not a crash.
  fakes.provider.down = true
  assert.equal(await app.gatewayTick(), null)
  assert.ok(fakes.log.some((l) => /provider is down/.test(l)), fakes.log.join('\n'))
})

test('uploads: files that are not on IPFS yet become the DAG kubo would build, then a pin', async () => {
  const s = ipfsSite()
  if (!s) { test.skip('no ipfs binary'); return }
  const { app, db } = fresh()
  const body = Buffer.from('hello from sia\n')

  const up = await call(app, 'PUT', '/api/upload?filename=hello.txt', { auth: true, stream: Readable.from([body]) })
  assert.equal(up.status, 202, up.text)
  assert.match(up.json.cid, /^bafk?/)
  assert.equal(up.json.bytes, body.length)
  assert.equal(db.prepare('SELECT status FROM pin_requests').get().status, 'queued')

  await app.pinTick()
  assert.equal(db.prepare('SELECT status FROM pin_requests').get().status, 'pinned')
  const served = await call(app, 'GET', `/ipfs/${up.json.cid}`)
  assert.equal(served.status, 200)
  assert.equal(served.buf.equals(body), true, 'what came back is byte for byte what went in')
})
