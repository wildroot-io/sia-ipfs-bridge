'use strict'
/**
 * CIDs and the two encodings we meet: CIDv0 (`Qm…`, base58btc, always
 * dag-pb + sha2-256) and CIDv1 (`b…`, base32 lower, any codec). Enough to
 * parse what a gateway request or a CAR block carries, compare them, and
 * print them back. A CIDv0 and its CIDv1 form name the same block, so the
 * database key (`key()`) is `<codec>:<multihash hex>` for both.
 *
 * Plain functions over Buffers; no multiformats dependency, because the
 * subset is small and the failure mode of a mismatch is a silent 404.
 */

const CODEC = { raw: 0x55, dagPb: 0x70, dagCbor: 0x71, dagJson: 0x0129 }
const SHA2_256 = 0x12
const { createHash } = require('node:crypto')

/* ------------------------------------------------------------ varints */

function readVarint (buf, pos) {
  let out = 0n; let shift = 0n; let i = pos
  for (;;) {
    if (i >= buf.length) throw new Error('truncated varint')
    const b = buf[i++]
    out |= BigInt(b & 0x7f) << shift
    if ((b & 0x80) === 0) break
    shift += 7n
    if (shift > 63n) throw new Error('varint too long')
  }
  return { value: out, next: i }
}

function encodeVarint (n) {
  let v = BigInt(n); const out = []
  for (;;) {
    const b = Number(v & 0x7fn); v >>= 7n
    if (v === 0n) { out.push(b); break }
    out.push(b | 0x80)
  }
  return Buffer.from(out)
}

/* ------------------------------------------------------------- bases */

const B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
function base58Encode (buf) {
  let n = BigInt('0x' + (buf.length ? buf.toString('hex') : '0'))
  let out = ''
  while (n > 0n) { out = B58[Number(n % 58n)] + out; n /= 58n }
  for (const b of buf) { if (b !== 0) break; out = '1' + out }
  return out
}
function base58Decode (str) {
  let n = 0n
  for (const c of str) {
    const d = B58.indexOf(c)
    if (d < 0) throw new Error('bad base58')
    n = n * 58n + BigInt(d)
  }
  let hex = n.toString(16); if (hex.length % 2) hex = '0' + hex
  let bytes = Buffer.from(n === 0n ? '' : hex, 'hex')
  let zeros = 0; for (const c of str) { if (c !== '1') break; zeros++ }
  if (zeros) bytes = Buffer.concat([Buffer.alloc(zeros), bytes])
  return bytes
}

const B32 = 'abcdefghijklmnopqrstuvwxyz234567'
function base32Encode (buf) {
  let bits = 0; let value = 0; let out = ''
  for (const b of buf) {
    value = (value << 8) | b; bits += 8
    while (bits >= 5) { out += B32[(value >>> (bits - 5)) & 31]; bits -= 5 }
  }
  if (bits > 0) out += B32[(value << (5 - bits)) & 31]
  return out
}
function base32Decode (str) {
  let bits = 0; let value = 0; const out = []
  for (const c of str.toLowerCase()) {
    const d = B32.indexOf(c)
    if (d < 0) throw new Error('bad base32')
    value = (value << 5) | d; bits += 5
    if (bits >= 8) { out.push((value >>> (bits - 8)) & 255); bits -= 8 }
  }
  return Buffer.from(out)
}

/* --------------------------------------------------------------- CID */

class CID {
  constructor (version, codec, multihash) {
    this.version = version; this.codec = codec; this.multihash = multihash
  }

  /** From raw CID bytes (as found in a CAR or a dag-pb link). */
  static fromBytes (buf) {
    if (buf.length === 34 && buf[0] === SHA2_256 && buf[1] === 32) return new CID(0, CODEC.dagPb, Buffer.from(buf))
    const v = readVarint(buf, 0)
    if (v.value !== 1n) throw new Error(`unsupported CID version ${v.value}`)
    const c = readVarint(buf, v.next)
    const mh = buf.subarray(c.next)
    const code = readVarint(mh, 0); const size = readVarint(mh, code.next)
    if (mh.length !== size.next + Number(size.value)) throw new Error('bad multihash length')
    return new CID(1, Number(c.value), Buffer.from(mh))
  }

  /** Bytes with a leading length, as a CAR block or a CBOR tag-42 wants. Returns {cid, next}. */
  static readFrom (buf, pos) {
    if (buf[pos] === SHA2_256 && buf[pos + 1] === 32) return { cid: CID.fromBytes(buf.subarray(pos, pos + 34)), next: pos + 34 }
    const v = readVarint(buf, pos)
    if (v.value !== 1n) throw new Error(`unsupported CID version ${v.value}`)
    const c = readVarint(buf, v.next)
    const code = readVarint(buf, c.next); const size = readVarint(buf, code.next)
    const end = size.next + Number(size.value)
    return { cid: new CID(1, Number(c.value), Buffer.from(buf.subarray(c.next, end))), next: end }
  }

  static parse (str) {
    const s = String(str || '').trim()
    if (/^Qm[1-9A-HJ-NP-Za-km-z]{44}$/.test(s)) return CID.fromBytes(base58Decode(s))
    if (/^b[a-z2-7]+$/.test(s)) return CID.fromBytes(base32Decode(s.slice(1)))
    if (/^B[A-Z2-7]+$/.test(s)) return CID.fromBytes(base32Decode(s.slice(1)))
    throw new Error('not a CID')
  }

  bytes () {
    if (this.version === 0) return this.multihash
    return Buffer.concat([encodeVarint(1), encodeVarint(this.codec), this.multihash])
  }

  /** The database key: the same for a CIDv0 and its CIDv1 form. */
  key () { return `${this.codec.toString(16)}:${this.multihash.toString('hex')}` }

  static fromKey (key) {
    const m = /^([0-9a-f]+):([0-9a-f]+)$/.exec(String(key || ''))
    if (!m) throw new Error('not a pin key')
    return new CID(1, parseInt(m[1], 16), Buffer.from(m[2], 'hex'))
  }

  /** Canonical string: base32 CIDv1 for everything (a v0 prints as v1). */
  toString () {
    return 'b' + base32Encode(Buffer.concat([encodeVarint(1), encodeVarint(this.codec), this.multihash]))
  }

  /** The v0 form when there is one (dag-pb + sha2-256), else toString(). */
  toV0String () {
    return this.codec === CODEC.dagPb && this.multihash[0] === SHA2_256 && this.multihash[1] === 32 ? base58Encode(this.multihash) : this.toString()
  }

  equals (other) { return this.codec === other.codec && this.multihash.equals(other.multihash) }

  /** Fail closed before accepting block bytes from storage or a cache. */
  verify (data) {
    const code = readVarint(this.multihash, 0)
    const size = readVarint(this.multihash, code.next)
    const expected = this.multihash.subarray(size.next)
    if (BigInt(expected.length) !== size.value) throw new Error('bad multihash length')
    let actual
    if (code.value === 0n) actual = data
    else if (code.value === 0x12n) actual = createHash('sha256').update(data).digest()
    else if (code.value === 0x13n) actual = createHash('sha512').update(data).digest()
    else throw new Error(`unsupported block hash ${code.value}`)
    if (!expected.equals(actual)) throw new Error('block does not match its CID')
    return data
  }

  /**
   * The block an identity-hashed CID carries inline (multihash code 0x00),
   * or null. `bafkqaaa`, the empty one, is what kubo's HTTP retrieval probes
   * a gateway with before trusting it with real requests.
   */
  inline () {
    if (this.multihash[0] !== 0) return null
    const size = readVarint(this.multihash, 1)
    return this.multihash.subarray(size.next)
  }
}

/** Is `str` a CID at all? Never throws. */
function isCid (str) { try { CID.parse(str); return true } catch { return false } }

module.exports = { CID, CODEC, SHA2_256, readVarint, encodeVarint, base58Encode, base58Decode, base32Encode, base32Decode, isCid }
