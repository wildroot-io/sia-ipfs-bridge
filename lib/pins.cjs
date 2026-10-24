'use strict'
/**
 * The pin table: which public CIDs we hold, where their CAR lives on Sia,
 * who asked for them, and where each block sits inside the archive.
 *
 * Three ways in, one table:
 *
 *   Share URL  A client that already backed the CAR up to its OWN Sia
 *              account shares the object and registers {cid, shareUrl}. We
 *              pin the shared object under the SERVICE account — it now
 *              outlives the client's device and plan — and index it. No
 *              bytes are re-uploaded and nothing is paid for twice.
 *
 *   Pinning    The IPFS Pinning Service API. A request is queued; the
 *   Service    worker fetches the CAR from a trustless gateway (or over
 *   API        bitswap), uploads it under the service account, indexes it.
 *
 *   Uploads    Files sent to the service: hashed into the DAG kubo would
 *              build (lib/unixfs.cjs), written as a CAR on disk, then queued
 *              exactly like a Pinning Service request — the worker reads the
 *              CAR from disk instead of from a gateway.
 *
 * A pin is alive while any pinner remains; the last unpin deletes the
 * object. Ownership for metering is the OLDEST pinner (the one who caused the
 * bytes to exist); a later pinner shares the pin but not the bill.
 */
const crypto = require('crypto')
const { Transform } = require('stream')
const { now, tx } = require('./db.cjs')
const { CID, CODEC } = require('./cid.cjs')
const car = require('./car.cjs')
const sia = require('./sia.cjs')

const STATUS = { queued: 'queued', pinning: 'pinning', pinned: 'pinned', failed: 'failed' }
const MAX_ATTEMPTS = 5
const RETRY_MS = 60e3 // after a failed attempt, wait this × attempts before the next

const cidKey = (c) => (c instanceof CID ? c : CID.parse(c)).key()
const get = (db, key) => db.prepare('SELECT * FROM pins WHERE cid = ?').get(key) || null

/**
 * Bytes and CID count this account is responsible for across its public
 * pins — the OLDEST pinner rule (file header): a CID counts against
 * whichever account pinned it first, never a later sharer. This is the ONE
 * place that rule is expressed in SQL; quota enforcement (`room`/`mustPin`
 * above, `register`/`work` below) and usage reporting (`accounts.usage()`,
 * usage reporting both call this, so they cannot compute two different
 * numbers for the same account.
 */
function publicUsage (db, accountId) {
  const row = db.prepare(`SELECT COUNT(*) AS n, COALESCE(SUM(p.size), 0) AS b FROM pins p
    WHERE (SELECT account_id FROM pinners WHERE cid = p.cid ORDER BY created_at LIMIT 1) = ?`).get(accountId)
  return { bytes: Number(row.b), count: row.n }
}

/** Bytes this account is responsible for across its public pins. */
function publicBytes (db, accountId) { return publicUsage(db, accountId).bytes }

/**
 * Register a share URL as a public pin for `accountId`. Returns the row.
 * `plan` decides whether public pins are allowed and how much space is left.
 */
async function register (db, s, { accountId, plan, name = null, cid, shareUrl, warm = null }) {
  if (!plan.publicPins) { const e = new Error(`the ${plan.title} plan does not include public pins`); e.status = 402; throw e }
  const key = cidKey(cid)
  let row = get(db, key)
  if (!row) {
    const pinned = await sia.pinShared(s, shareUrl)
    if (publicBytes(db, accountId) + pinned.size > plan.storageBytes) {
      await sia.unpin(s, pinned.id).catch(() => {})
      const e = new Error('that would exceed the space on your plan'); e.status = 402; throw e
    }
    tx(db, () => {
      db.prepare('INSERT INTO pins (cid, object_id, size, car_size, indexed, created_at) VALUES (?, ?, ?, ?, 0, ?)')
        .run(key, pinned.id, pinned.size, pinned.size, now())
      db.prepare('INSERT OR IGNORE INTO pinners (cid, account_id, name, created_at) VALUES (?, ?, ?, ?)').run(key, accountId, name, now())
    })
    row = get(db, key)
    await indexPin(db, s, row, CID.parse(cid), { warm })
  } else {
    db.prepare('INSERT OR IGNORE INTO pinners (cid, account_id, name, created_at) VALUES (?, ?, ?, ?)').run(key, accountId, name, now())
  }
  return get(db, key)
}

/**
 * Walk the CAR once and record every block. The root must be among the
 * archive's roots — a share URL for the wrong file is refused here and the
 * object released. The bytes are streaming past anyway, so `warm(cid, data)`
 * (the gateway's cache) gets every block: a fresh pin's first visitor finds
 * it on the box, not on a Sia host.
 */
async function indexPin (db, s, row, rootCid, { warm = null } = {}) {
  const obj = await sia.object(s, row.object_id)
  const stream = sia.streamRange(s, obj, 0, row.size)
  const insert = db.prepare('INSERT OR REPLACE INTO blocks (block_cid, cid, offset, length) VALUES (?, ?, ?, ?)')
  let roots = null; let count = 0
  try {
    db.exec('BEGIN')
    for await (const item of car.index(stream, { blocks: true })) {
      if (item.roots) {
        roots = item.roots
        if (!roots.some((r) => r.equals(rootCid))) throw new Error('the archive does not have that CID as a root')
        continue
      }
      item.cid.verify(item.data)
      insert.run(item.cid.key(), row.cid, item.offset, item.length)
      if (warm) warm(item.cid, item.data)
      count++
    }
    db.prepare('UPDATE pins SET indexed = 1 WHERE cid = ?').run(row.cid)
    db.exec('COMMIT')
  } catch (e) {
    try { db.exec('ROLLBACK') } catch {}
    await remove(db, s, row.cid).catch(() => {})
    throw e
  }
  return { blocks: count, roots }
}

/**
 * Drop one pinner; the last one out deletes the object. Storage goes first,
 * so a refusal there leaves the pinner (and the account's view) exactly as it
 * was. The account's settled requests for the CID go with it: `ipfs pin
 * remote ls` must not keep saying pinned.
 */
async function unregister (db, s, { accountId, cid }) {
  const key = cidKey(cid)
  const others = db.prepare('SELECT COUNT(*) AS n FROM pinners WHERE cid = ? AND account_id != ?').get(key, accountId).n
  if (others === 0) await remove(db, s, key)
  else db.prepare('DELETE FROM pinners WHERE cid = ? AND account_id = ?').run(key, accountId)
  for (const r of db.prepare('SELECT id, cid FROM pin_requests WHERE account_id = ? AND status = ?').all(accountId, STATUS.pinned)) {
    if (cidKey(r.cid) === key) db.prepare('DELETE FROM pin_requests WHERE id = ?').run(r.id)
  }
  return { removed: others === 0 }
}

async function remove (db, s, key) {
  const row = get(db, key)
  if (!row) return
  // Two CIDs can name the same Sia object — the same archive registered
  // under a second root, or a failed index releasing what it just pinned —
  // and deleting the object then takes the OTHER pin's bytes with it. Only
  // the last row pointing at an object may delete it.
  const shared = db.prepare('SELECT COUNT(*) AS n FROM pins WHERE object_id = ? AND cid != ?').get(row.object_id, key).n
  if (!shared) await sia.unpin(s, row.object_id).catch((e) => { if (!/not found|404/i.test(e.message)) throw e })
  tx(db, () => {
    db.prepare('DELETE FROM blocks WHERE cid = ?').run(key)
    db.prepare('DELETE FROM pinners WHERE cid = ?').run(key)
    db.prepare('DELETE FROM pins WHERE cid = ?').run(key)
  })
}

/** Where a block lives: {pin, offset, length, owner} | null. */
/**
 * Where a block's bytes are: the pin's archive, offset and length, and the
 * owner they are metered to. A block is its bytes, not its codec: `bafk…`
 * (raw) and `bafy…` (dag-pb) of the same multihash are the same block, and a
 * bitswap peer asks by multihash alone — so every codec spelling matches.
 */
function locate (db, blockCid) {
  const c = blockCid instanceof CID ? blockCid : CID.parse(blockCid)
  const keys = [c.key(), ...Object.values(CODEC).filter((code) => code !== c.codec).map((code) => new CID(1, code, c.multihash).key())]
  const b = db.prepare(`SELECT b.cid AS pin, b.offset, b.length, p.object_id, p.size
    FROM blocks b JOIN pins p ON p.cid = b.cid WHERE b.block_cid IN (${keys.map(() => '?').join(', ')}) AND p.indexed = 1 LIMIT 1`).get(...keys)
  if (!b) return null
  const owner = db.prepare('SELECT account_id FROM pinners WHERE cid = ? ORDER BY created_at LIMIT 1').get(b.pin)
  return { ...b, owner: owner ? owner.account_id : null }
}

/** The blocks of one pin whose bytes start in [from, to) of its CAR, in archive order. */
function blocksBetween (db, pinCid, from, to) {
  return db.prepare('SELECT block_cid, offset, length FROM blocks WHERE cid = ? AND offset >= ? AND offset < ? ORDER BY offset').all(pinCid, from, to)
}

/** Every block held here, as canonical CID strings — what the provider announces. */
function * allBlocks (db) {
  for (const row of db.prepare('SELECT DISTINCT b.block_cid FROM blocks b JOIN pins p ON p.cid = b.cid WHERE p.indexed = 1').iterate()) {
    yield CID.fromKey(row.block_cid).toString()
  }
}

const forAccount = (db, accountId) => db.prepare(`SELECT p.cid, p.size, p.indexed, r.name, r.created_at FROM pinners r JOIN pins p ON p.cid = r.cid
  WHERE r.account_id = ? ORDER BY r.created_at DESC`).all(accountId)

/* ------------------------------------------- Pinning Service API queue */

function createRequest (db, { accountId, cid, name = null, origins = [], meta = {}, uploadId = null }) {
  // Trim, then refuse garbage before it is queued. Trimmed, not
  // canonicalised: CID.parse accepts a padded string, and storing it padded
  // made one CID look like several requests — but the
  // SPELLING is the caller's (a v0 `Qm…` comes back as they sent it).
  cid = String(cid).trim()
  CID.parse(cid)
  const id = crypto.randomUUID()
  db.prepare(`INSERT INTO pin_requests (id, account_id, cid, name, origins, meta, status, upload_id, created_at, updated_at)
              VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?)`)
    .run(id, accountId, cid, name, JSON.stringify(origins || []), JSON.stringify(meta || {}), uploadId, now(), now())
  return getRequest(db, id)
}

const getRequest = (db, id) => db.prepare('SELECT * FROM pin_requests WHERE id = ?').get(id) || null

function listRequests (db, accountId, { cid = [], name = null, status = [], limit = 10, before = null } = {}) {
  const where = ['account_id = ?']; const args = [accountId]
  if (cid.length) { where.push(`cid IN (${cid.map(() => '?').join(',')})`); args.push(...cid) }
  if (name) { where.push('name = ?'); args.push(name) }
  if (status.length) { where.push(`status IN (${status.map(() => '?').join(',')})`); args.push(...status) }
  if (before) { where.push('created_at < ?'); args.push(before) }
  const rows = db.prepare(`SELECT * FROM pin_requests WHERE ${where.join(' AND ')} ORDER BY created_at DESC LIMIT ?`).all(...args, limit)
  const count = db.prepare(`SELECT COUNT(*) AS n FROM pin_requests WHERE ${where.join(' AND ')}`).get(...args).n
  return { count, results: rows }
}

/**
 * The spec's replace: one request becomes another. The OLD CID stops being
 * this account's here — without that a replace left the previous object
 * pinned, stored and billed to them forever, which is a hole that only shows
 * up on the invoice. Unregistering first also takes the old request row with
 * it, so the new one is created afterwards and the delete below is a no-op
 * in that case.
 */
async function replaceRequest (db, s, id, { cid, name, origins, meta }) {
  const old = getRequest(db, id)
  if (!old) return null
  if (old.status === STATUS.pinned) await unregister(db, s, { accountId: old.account_id, cid: old.cid })
  const fresh = createRequest(db, { accountId: old.account_id, cid, name: name ?? old.name, origins, meta })
  db.prepare('DELETE FROM pin_requests WHERE id = ?').run(id)
  return fresh
}

async function deleteRequest (db, s, id) {
  const r = getRequest(db, id)
  if (!r) return false
  if (r.status === STATUS.pinned) await unregister(db, s, { accountId: r.account_id, cid: r.cid }) // takes the row with it
  db.prepare('DELETE FROM pin_requests WHERE id = ?').run(id)
  return true
}

/** The spec's PinStatus object. */
function pinStatus (r, { delegates = [] } = {}) {
  return {
    requestid: r.id,
    status: r.status,
    created: r.created_at,
    pin: { cid: r.cid, name: r.name || undefined, origins: JSON.parse(r.origins || '[]'), meta: JSON.parse(r.meta || '{}') },
    delegates,
    info: r.info ? { reason: r.info } : {}
  }
}

/**
 * One queued request → its CAR uploaded and indexed. `source(r)` returns a
 * Node Readable of the CAR: a trustless gateway for a CID someone asked us
 * to pin, the file uploads.cjs wrote for one they sent us. A request that
 * fails goes back to the queue and is not picked up again until its backoff
 * has passed (`at` is the clock); after MAX_ATTEMPTS it is failed for good.
 * Returns the request's new status, or null when nothing was due.
 */
/** A readable that fails once it has passed `limit` bytes (uploads.cjs does the same for the upload path). */
function capped (readable, limit) {
  let n = 0
  return readable.pipe(new Transform({
    transform (chunk, _enc, cb) {
      n += chunk.length
      if (n > limit) cb(Object.assign(new Error('that would exceed the space on your plan'), { overQuota: true }))
      else cb(null, chunk)
    }
  }))
}

async function work (db, s, { source, plans, accountOf, warm = null, at = Date.now() }) {
  const due = (q) => q.attempts === 0 || Date.parse(q.updated_at) + RETRY_MS * q.attempts <= at
  const r = db.prepare("SELECT * FROM pin_requests WHERE status = 'queued' ORDER BY created_at").all().find(due)
  if (!r) return null
  const stamp = new Date(at).toISOString()
  const mark = (status, info = null) => db.prepare('UPDATE pin_requests SET status = ?, info = ?, updated_at = ? WHERE id = ?').run(status, info, stamp, r.id)
  db.prepare('UPDATE pin_requests SET attempts = attempts + 1 WHERE id = ?').run(r.id)
  mark(STATUS.pinning)
  try {
    const acct = accountOf(r.account_id)
    const plan = plans[acct ? acct.plan : 'free'] || plans.free
    if (!plan.publicPins) throw Object.assign(new Error(`the ${plan.title} plan does not include public pins`), { final: true })
    const key = cidKey(r.cid)
    if (!get(db, key)) {
      // Cut the source off at the room left on the plan BEFORE the bytes are
      // bought. It used to upload the whole CAR to Sia and check afterwards,
      // so a 5 GB account could make us pull and pay for tens of GB per
      // request, five times over. The stream error is
      // final: over quota is not a transport hiccup to retry.
      const room = Math.max(0, plan.storageBytes - publicBytes(db, r.account_id))
      const up = await sia.upload(s, capped(await source(r), room))
      if (publicBytes(db, r.account_id) + up.size > plan.storageBytes) {
        await sia.unpin(s, up.id).catch(() => {})
        throw Object.assign(new Error('that would exceed the space on your plan'), { final: true })
      }
      db.prepare('INSERT INTO pins (cid, object_id, size, car_size, indexed, created_at) VALUES (?, ?, ?, ?, 0, ?)').run(key, up.id, up.size, up.size, now())
      db.prepare('INSERT OR IGNORE INTO pinners (cid, account_id, name, created_at) VALUES (?, ?, ?, ?)').run(key, r.account_id, r.name, now())
      await indexPin(db, s, get(db, key), CID.parse(r.cid), { warm })
    } else {
      db.prepare('INSERT OR IGNORE INTO pinners (cid, account_id, name, created_at) VALUES (?, ?, ?, ?)').run(key, r.account_id, r.name, now())
    }
    mark(STATUS.pinned)
  } catch (e) {
    const giveUp = e.final || e.overQuota || r.attempts + 1 >= MAX_ATTEMPTS
    mark(giveUp ? STATUS.failed : STATUS.queued, e.message)
  }
  const after = getRequest(db, r.id)
  return after ? after.status : null
}

/**
 * Tell the DHT about every indexed pin it has not heard of (provider.cjs):
 * right after a pin lands, and again each tick until the sidecar answers —
 * a provider restart loses nothing. Returns how many were announced; stops
 * at the first failure (the sidecar is down for all of them alike).
 */
async function announce (db, provide, { at = Date.now(), log = () => {} } = {}) {
  let n = 0
  for (const row of db.prepare('SELECT cid FROM pins WHERE indexed = 1 AND announced_at IS NULL ORDER BY created_at').all()) {
    try { await provide(CID.fromKey(row.cid)) } catch (e) { log(`announce ${CID.fromKey(row.cid)}: ${e.message}`); break }
    db.prepare('UPDATE pins SET announced_at = ? WHERE cid = ?').run(new Date(at).toISOString(), row.cid)
    n++
  }
  return n
}

module.exports = { STATUS, MAX_ATTEMPTS, RETRY_MS, cidKey, get, publicBytes, publicUsage, register, indexPin, unregister, remove, locate, blocksBetween, allBlocks, forAccount, createRequest, getRequest, listRequests, replaceRequest, deleteRequest, pinStatus, work, announce }
