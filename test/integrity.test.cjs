'use strict'
const test = require('node:test')
const assert = require('node:assert/strict')
const { createHash } = require('node:crypto')
const { CID } = require('../lib/cid.cjs')
const { Gateway, requestBlocks } = require('../lib/gateway.cjs')
const car = require('../lib/car.cjs')
const pins = require('../lib/pins.cjs')
const { fresh, chill, OWNER, PLANS } = require('./harness.cjs')

const named = (data) => new CID(1, 0x55, Buffer.concat([Buffer.from([0x12, 32]), createHash('sha256').update(data).digest()]))

test('block verification rejects corruption, truncation and unsupported hashes', () => {
  const data = Buffer.from('verified block')
  const cid = named(data)
  assert.equal(cid.verify(data), data)
  assert.throws(() => cid.verify(Buffer.from('corrupt block')), /does not match/)
  assert.throws(() => cid.verify(data.subarray(1)), /does not match/)
  assert.throws(() => new CID(1, 0x55, Buffer.from([0x99, 1, 1, 0])).verify(data), /unsupported/)
  assert.throws(() => new CID(1, 0x55, Buffer.from([0x12, 32, 0])).verify(data), /length/)
  const inline = new CID(1, 0x55, Buffer.concat([Buffer.from([0, data.length]), data]))
  assert.equal(inline.verify(data), data)
  assert.throws(() => inline.verify(Buffer.from('wrong')), /does not match/)
})

test('gateway never promotes corrupt warm blocks or serves corrupt cached bytes', async () => {
  const data = Buffer.from('verified block')
  const cid = named(data)
  let writes = 0
  const gateway = new Gateway({
    cache: {
      set: () => { writes++ },
      promote: () => { writes++ },
      get: async () => Buffer.from('corrupt block')
    }
  })
  assert.throws(() => gateway.warm(cid, Buffer.from('corrupt block')), /does not match/)
  assert.equal(writes, 0)
  await assert.rejects(gateway.block(cid), /does not match/)
  gateway.warm(cid, data)
  assert.equal(writes, 2)
})

test('request memoization evicts old blocks and never retains oversized blocks', async () => {
  let reads = 0
  const get = requestBlocks(async (cid) => { reads++; return { data: cid.data } }, 8)
  const block = (text) => { const data = Buffer.from(text); return { multihash: named(data).multihash, data } }
  const first = block('aaaa'); const second = block('bbbb'); const third = block('cccc')
  await get(first); await get(second); await get(first)
  assert.equal(reads, 2)
  await get(third); await get(second)
  assert.equal(reads, 4, 'least recently used block was evicted')
  const big = block('larger than budget')
  await get(big); await get(big)
  assert.equal(reads, 6)
  for (let i = 0; i < 1000; i++) await get(block(String(i).padStart(4, '0')))
  await get(first)
  assert.equal(reads, 1007, 'first block did not survive the streamed file')
})

test('pin indexing rejects corrupt blocks without leaving a pin or index', async () => {
  const { app, db, fakes } = fresh()
  const cid = named(Buffer.from('valid'))
  const oid = fakes.sia.put(Buffer.concat([car.header([cid]), car.section(cid, Buffer.from('wrong'))]))
  await assert.rejects(pins.register(db, fakes.sia, {
    accountId: OWNER, plan: PLANS.publish, cid: cid.toString(), shareUrl: fakes.sia.share(oid)
  }), /does not match/)
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM pins').get().n, 0)
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM blocks').get().n, 0)
  assert.equal(fakes.sia.objects.has(oid), false)
  await app.gateway.idle()
})

test('cold retrieval rejects storage corruption in both direct and read-ahead paths', async () => {
  const { app, db, fakes } = fresh()
  const data = Buffer.from('valid')
  const cid = named(data)
  const archive = Buffer.concat([car.header([cid]), car.section(cid, data)])
  const oid = fakes.sia.put(archive)
  await pins.register(db, fakes.sia, {
    accountId: OWNER, plan: PLANS.publish, cid: cid.toString(), shareUrl: fakes.sia.share(oid)
  })
  await chill(app.gateway.cache)
  fakes.sia.objects.get(oid)[archive.length - 1] ^= 1
  await assert.rejects(app.gateway.block(cid), /does not match/)
  await app.gateway.idle()
  assert.equal(app.gateway.cache.peek(cid.multihash.toString('hex')), null)
})
