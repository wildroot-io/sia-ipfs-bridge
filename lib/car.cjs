'use strict'
/**
 * CARv1 (Content Addressable aRchive), read and written. A public pin IS a
 * CAR: a client exports the DAG as one file and it is stored on Sia as a
 * single object. This module indexes that archive once — every block's CID,
 * offset and length inside the object — so the gateway can range-read one
 * block out of Sia instead of pulling the whole archive. That index is what
 * makes one Sia object behave like a block store.
 *
 *   CAR  = varint(len) header   { version: 1, roots: [CID…] }   (DAG-CBOR)
 *          then blocks:  varint(len) CID block-bytes
 *
 * The reader is incremental over an async iterable of chunks, so a
 * multi-gigabyte archive is indexed with a bounded buffer. The DAG-CBOR
 * decoder below covers exactly the header's shape (ints, bytes, text,
 * arrays, maps, tag 42) and refuses anything else.
 */
const { CID, readVarint, encodeVarint } = require('./cid.cjs')

/* --------------------------------------------------------- DAG-CBOR */

function cborDecode (buf) {
  let pos = 0
  const readLen = (info) => {
    if (info < 24) return info
    if (info === 24) return buf[pos++]
    if (info === 25) { const v = buf.readUInt16BE(pos); pos += 2; return v }
    if (info === 26) { const v = buf.readUInt32BE(pos); pos += 4; return v }
    if (info === 27) { const v = Number(buf.readBigUInt64BE(pos)); pos += 8; return v }
    throw new Error('cbor: indefinite lengths are not DAG-CBOR')
  }
  const item = () => {
    if (pos >= buf.length) throw new Error('cbor: truncated')
    const b = buf[pos++]; const major = b >> 5; const info = b & 31
    switch (major) {
      case 0: return readLen(info)
      case 1: return -1 - readLen(info)
      case 2: { const n = readLen(info); const v = buf.subarray(pos, pos + n); pos += n; return Buffer.from(v) }
      case 3: { const n = readLen(info); const v = buf.toString('utf8', pos, pos + n); pos += n; return v }
      case 4: { const n = readLen(info); const out = []; for (let i = 0; i < n; i++) out.push(item()); return out }
      case 5: { const n = readLen(info); const out = {}; for (let i = 0; i < n; i++) { const k = item(); out[k] = item() } return out }
      case 6: {
        const tag = readLen(info)
        const inner = item()
        if (tag !== 42 || !Buffer.isBuffer(inner) || inner[0] !== 0) throw new Error(`cbor: unsupported tag ${tag}`)
        return CID.fromBytes(inner.subarray(1))
      }
      case 7: {
        if (info === 20) return false; if (info === 21) return true; if (info === 22) return null
        throw new Error('cbor: unsupported simple value')
      }
      default: throw new Error('cbor: unsupported major type')
    }
  }
  const out = item()
  if (pos !== buf.length) throw new Error('cbor: trailing bytes')
  return out
}

function cborHead (major, n) {
  if (n < 24) return Buffer.from([(major << 5) | n])
  if (n < 0x100) return Buffer.from([(major << 5) | 24, n])
  if (n < 0x10000) { const b = Buffer.alloc(3); b[0] = (major << 5) | 25; b.writeUInt16BE(n, 1); return b }
  if (n < 0x100000000) { const b = Buffer.alloc(5); b[0] = (major << 5) | 26; b.writeUInt32BE(n, 1); return b }
  const b = Buffer.alloc(9); b[0] = (major << 5) | 27; b.writeBigUInt64BE(BigInt(n), 1); return b
}

/** Encode the CAR header for `roots` (DAG-CBOR: keys sorted by length then bytes). */
function cborHeader (roots) {
  const rootsEnc = Buffer.concat([cborHead(4, roots.length), ...roots.map((c) => {
    const bytes = Buffer.concat([Buffer.from([0]), c.bytes()])
    return Buffer.concat([cborHead(6, 42), cborHead(2, bytes.length), bytes])
  })])
  const text = (s) => Buffer.concat([cborHead(3, Buffer.byteLength(s)), Buffer.from(s)])
  // "roots" (5) sorts before "version" (7).
  return Buffer.concat([cborHead(5, 2), text('roots'), rootsEnc, text('version'), cborHead(0, 1)])
}

/* ------------------------------------------------------------ reader */

/**
 * Index a CAR from an async iterable of Buffers. Yields
 * `{cid, offset, length}` per block — `offset` is where the BLOCK BYTES
 * start in the archive (past the varint and CID), `length` is their size —
 * after first yielding `{roots}`. With `blocks: true` each entry carries the
 * block's `data` as well.
 */
async function * index (chunks, { blocks = false } = {}) {
  let buf = Buffer.alloc(0)
  let base = 0 // archive offset of buf[0]
  let done = false
  const it = chunks[Symbol.asyncIterator]()
  const fill = async (need) => {
    while (buf.length < need && !done) {
      const { value, ready } = await it.next().then((r) => ({ value: r.value, ready: !r.done }))
      if (!ready) { done = true; break }
      buf = buf.length ? Buffer.concat([buf, Buffer.from(value)]) : Buffer.from(value)
    }
    return buf.length >= need
  }
  const consume = (n) => { buf = buf.subarray(n); base += n }
  const varint = async () => {
    for (;;) {
      try { return readVarint(buf, 0) } catch (e) {
        if (!/truncated/.test(e.message) || !(await fill(buf.length + 1))) throw e
      }
    }
  }

  if (!(await fill(1))) throw new Error('empty CAR')
  const h = await varint()
  if (!(await fill(h.next + Number(h.value)))) throw new Error('truncated CAR header')
  const header = cborDecode(buf.subarray(h.next, h.next + Number(h.value)))
  if (header.version !== 1) throw new Error(`CAR version ${header.version}; only v1 is read`)
  if (!Array.isArray(header.roots)) throw new Error('CAR header has no roots')
  consume(h.next + Number(h.value))
  yield { roots: header.roots }

  for (;;) {
    if (!(await fill(1))) return
    const l = await varint()
    const sectionLen = Number(l.value)
    if (sectionLen === 0) return // a zero-length section is padding / end
    if (!(await fill(l.next + Math.min(sectionLen, 256)))) throw new Error('truncated CAR section')
    const { cid, next } = CID.readFrom(buf, l.next)
    const cidLen = next - l.next
    const length = sectionLen - cidLen
    if (length < 0) throw new Error('CAR section shorter than its CID')
    const skip = l.next + sectionLen
    if (blocks) {
      // The caller wants the bytes too (warming the cache while indexing):
      // hold this one section, copied out of the rolling buffer.
      if (!(await fill(skip))) throw new Error('truncated CAR block')
      yield { cid, offset: base + next, length, data: Buffer.from(buf.subarray(next, skip)) }
      consume(skip); continue
    }
    yield { cid, offset: base + next, length }
    // Skip the block bytes without holding them, but keep whatever of the
    // next section already arrived.
    if (buf.length >= skip) { consume(skip); continue }
    let remaining = skip - buf.length
    base += buf.length; buf = Buffer.alloc(0)
    while (remaining > 0) {
      const r = await it.next()
      if (r.done) throw new Error('truncated CAR block')
      const c = Buffer.from(r.value)
      if (c.length <= remaining) { remaining -= c.length; base += c.length } else { buf = c.subarray(remaining); base += remaining; remaining = 0 }
    }
  }
}

/* ------------------------------------------------------------ writer */

/** A CAR header block for `roots`. */
function header (roots) {
  const body = cborHeader(roots)
  return Buffer.concat([encodeVarint(body.length), body])
}

/** One CAR section for a block. */
function section (cid, data) {
  const cb = cid.bytes()
  return Buffer.concat([encodeVarint(cb.length + data.length), cb, data])
}

module.exports = { index, header, section, cborDecode, cborHeader }
