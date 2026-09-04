'use strict'
/**
 * Just enough dag-pb + UnixFS to serve a site and to make one: resolve
 * `/a/b/c.html` through directories, stream a file's bytes out of its leaf
 * blocks, and turn uploaded files into the DAG `ipfs add -r --cid-version=1`
 * would build — same chunks, same tree, same bytes, so the CID a person gets
 * from us is the CID they would get from kubo. What is supported is what
 * `ipfs add -r` of a site produces — dag-pb directories and files, raw
 * leaves, balanced or trickle DAGs. HAMT-sharded directories (thousands of
 * entries) and symlinks are refused with a clear error rather than served
 * wrong; the trustless block/CAR endpoints still carry them.
 *
 *   dag-pb  PBNode { repeated PBLink Links = 2; optional bytes Data = 1 }
 *           PBLink { bytes Hash = 1; string Name = 2; uint64 Tsize = 3 }
 *   unixfs  Data   { DataType Type = 1; bytes Data = 2; uint64 filesize = 3;
 *                    repeated uint64 blocksizes = 4 }
 */
const crypto = require('crypto')
const { CID, CODEC, SHA2_256, readVarint, encodeVarint } = require('./cid.cjs')

const TYPE = { raw: 0, directory: 1, file: 2, metadata: 3, symlink: 4, hamt: 5 }

// kubo's `ipfs add` defaults: size-262144 chunker, balanced layout with 174
// links per node, raw leaves (implied by --cid-version=1), and a directory
// is HAMT-sharded once its block would pass 256 KiB.
const CHUNK_SIZE = 256 * 1024
const MAX_LINKS = 174
const SHARD_THRESHOLD = 256 * 1024

/* ----------------------------------------------------------- protobuf */

function * fields (buf) {
  let pos = 0
  while (pos < buf.length) {
    const tag = readVarint(buf, pos); pos = tag.next
    const field = Number(tag.value >> 3n); const wire = Number(tag.value & 7n)
    if (wire === 0) { const v = readVarint(buf, pos); pos = v.next; yield { field, value: v.value } } else if (wire === 2) {
      const l = readVarint(buf, pos); pos = l.next
      const n = Number(l.value)
      yield { field, value: buf.subarray(pos, pos + n) }; pos += n
    } else if (wire === 1) { pos += 8 } else if (wire === 5) { pos += 4 } else throw new Error(`protobuf wire type ${wire}`)
  }
}

function decodePb (block) {
  const node = { links: [], data: null }
  for (const f of fields(block)) {
    if (f.field === 2) {
      const link = { cid: null, name: '', tsize: 0 }
      for (const g of fields(f.value)) {
        if (g.field === 1) link.cid = CID.fromBytes(g.value)
        else if (g.field === 2) link.name = g.value.toString('utf8')
        else if (g.field === 3) link.tsize = Number(g.value)
      }
      node.links.push(link)
    } else if (f.field === 1) node.data = Buffer.from(f.value)
  }
  return node
}

function decodeUnixfs (data) {
  const out = { type: TYPE.file, data: null, filesize: null, blocksizes: [] }
  if (!data) return out
  for (const f of fields(data)) {
    if (f.field === 1) out.type = Number(f.value)
    else if (f.field === 2) out.data = Buffer.from(f.value)
    else if (f.field === 3) out.filesize = Number(f.value)
    else if (f.field === 4) out.blocksizes.push(Number(f.value))
  }
  return out
}

/* ---------------------------------------------------------- resolution */

/**
 * What is at `cid`: {kind: 'file', size} | {kind: 'directory', entries:[{name,cid,tsize}]} | {kind: 'raw', size}.
 * `getBlock(cid) → Buffer` is the only I/O.
 */
async function describe (getBlock, cid) {
  if (cid.codec === CODEC.raw) { const b = await getBlock(cid); return { kind: 'file', size: b.length, raw: true } }
  if (cid.codec !== CODEC.dagPb) throw new NotServable(`codec 0x${cid.codec.toString(16)} is not UnixFS`)
  const node = decodePb(await getBlock(cid))
  const u = decodeUnixfs(node.data)
  if (u.type === TYPE.directory) return { kind: 'directory', entries: node.links.map((l) => ({ name: l.name, cid: l.cid, tsize: l.tsize })) }
  if (u.type === TYPE.hamt) throw new NotServable('sharded directories are not served yet')
  if (u.type === TYPE.symlink) throw new NotServable('symlinks are not served')
  if (u.type === TYPE.file || u.type === TYPE.raw) {
    const size = u.filesize != null ? u.filesize : (u.data ? u.data.length : 0) + u.blocksizes.reduce((a, b) => a + b, 0)
    return { kind: 'file', size, node, unixfs: u }
  }
  throw new NotServable(`UnixFS type ${u.type} is not served`)
}

/** Walk `segments` from `root`; returns the CID at the end or throws NotFound. */
async function resolvePath (getBlock, root, segments) {
  let cid = root
  for (const seg of segments) {
    const d = await describe(getBlock, cid)
    if (d.kind !== 'directory') throw new NotFound(`${seg}: not a directory`)
    const hit = d.entries.find((e) => e.name === seg)
    if (!hit) throw new NotFound(`${seg}: no such entry`)
    cid = hit.cid
  }
  return cid
}

/**
 * Stream a file's bytes, [offset, offset+length) of it, from its DAG. A
 * balanced DAG is walked depth-first; nodes whose whole range lies outside
 * the window are skipped by Tsize/blocksizes, never fetched.
 */
async function * fileRange (getBlock, cid, offset = 0, length = Infinity) {
  let end = length === Infinity ? Infinity : offset + length
  async function * walk (c, start) {
    // `start` is the absolute file offset this node's content begins at.
    if (c.codec === CODEC.raw) {
      const b = await getBlock(c)
      yield * slice(b, start)
      return
    }
    const node = decodePb(await getBlock(c))
    const u = decodeUnixfs(node.data)
    let at = start
    if (u.data && u.data.length) { yield * slice(u.data, at); at += u.data.length }
    for (let i = 0; i < node.links.length; i++) {
      const size = u.blocksizes[i] != null ? u.blocksizes[i] : node.links[i].tsize
      if (at >= end) return
      if (at + size > offset) yield * walk(node.links[i].cid, at)
      at += size
    }
  }
  function * slice (b, start) {
    const from = Math.max(0, offset - start)
    const to = Math.min(b.length, end - start)
    if (to > from) yield b.subarray(from, to)
  }
  if (end === Infinity) end = Number.MAX_SAFE_INTEGER
  yield * walk(cid, 0)
}

class NotFound extends Error {}
class NotServable extends Error {}

/* ------------------------------------------------------------ encoder */

const pbBytes = (field, bytes) => Buffer.concat([encodeVarint((field << 3) | 2), encodeVarint(bytes.length), bytes])
const pbUint = (field, n) => Buffer.concat([encodeVarint(field << 3), encodeVarint(n)])
const sha256 = (b) => Buffer.concat([Buffer.from([SHA2_256, 32]), crypto.createHash('sha256').update(b).digest()])
const rawCid = (bytes) => new CID(1, CODEC.raw, sha256(bytes))
const pbCid = (bytes) => new CID(1, CODEC.dagPb, sha256(bytes))

/** UnixFS Data, fields in number order; `data` is omitted when absent, as kubo does. */
function encodeUnixfs ({ type, data = null, filesize = null, blocksizes = [] }) {
  const parts = [pbUint(1, type)]
  if (data) parts.push(pbBytes(2, data))
  if (filesize != null) parts.push(pbUint(3, filesize))
  for (const b of blocksizes) parts.push(pbUint(4, b))
  return Buffer.concat(parts)
}

/**
 * A dag-pb node: Links (Hash, Name — written even when empty, as kubo does —
 * Tsize) before Data, exactly the bytes the go encoder writes, or the CID
 * would differ.
 */
function encodePb ({ links = [], data = null }) {
  const parts = links.map((l) => pbBytes(2, Buffer.concat([pbBytes(1, l.cid.bytes()), pbBytes(2, Buffer.from(l.name, 'utf8')), pbUint(3, l.tsize)])))
  if (data) parts.push(pbBytes(1, data))
  return Buffer.concat(parts)
}

/** Re-slice an async iterable of Buffers into pieces of exactly `size` (the last one shorter). */
async function * rechunk (iterable, size) {
  let held = []; let n = 0
  for await (const piece of iterable) {
    let b = Buffer.from(piece)
    while (n + b.length >= size) {
      const take = size - n
      held.push(b.subarray(0, take)); b = b.subarray(take)
      yield held.length === 1 ? held[0] : Buffer.concat(held)
      held = []; n = 0
    }
    if (b.length) { held.push(b); n += b.length }
  }
  if (n) yield held.length === 1 ? held[0] : Buffer.concat(held)
}

/**
 * Hash a file the way kubo's balanced builder does. `source()` returns an
 * async iterable of the file's bytes and is read once here and once more when
 * the blocks are written out, so only the CIDs are held in memory (a 5 GB
 * file is ~20k of them). Returns a file entry:
 *   {kind: 'file', cid, size, tsize, leaves: [{cid, length}], nodes: [{cid, bytes}], source}
 * A one-chunk file IS its raw leaf; bigger files get dag-pb nodes, root last.
 */
async function addFile (source, { chunkSize = CHUNK_SIZE, maxLinks = MAX_LINKS } = {}) {
  const leaves = []
  for await (const c of rechunk(source(), chunkSize)) leaves.push({ cid: rawCid(c), length: c.length })
  if (!leaves.length) leaves.push({ cid: rawCid(Buffer.alloc(0)), length: 0 })
  const nodes = []
  let level = leaves.map((l) => ({ cid: l.cid, tsize: l.length, filesize: l.length }))
  while (level.length > 1) {
    const next = []
    for (let i = 0; i < level.length; i += maxLinks) {
      const group = level.slice(i, i + maxLinks)
      const filesize = group.reduce((a, g) => a + g.filesize, 0)
      const bytes = encodePb({
        links: group.map((g) => ({ cid: g.cid, name: '', tsize: g.tsize })),
        data: encodeUnixfs({ type: TYPE.file, filesize, blocksizes: group.map((g) => g.filesize) })
      })
      const node = { cid: pbCid(bytes), bytes, tsize: bytes.length + group.reduce((a, g) => a + g.tsize, 0), filesize }
      nodes.push(node); next.push(node)
    }
    level = next
  }
  const root = level[0]
  return { kind: 'file', cid: root.cid, size: root.filesize, tsize: root.tsize, leaves, nodes, source, chunkSize }
}

/**
 * A directory over `entries` ({name, entry}); links sorted bytewise by name
 * like Go does. Refuses what kubo would shard into a HAMT, because the
 * gateway does not serve those (describe() above) — the caller tells the
 * person to split the folder.
 */
function addDirectory (entries) {
  const sorted = [...entries].sort((a, b) => Buffer.compare(Buffer.from(a.name, 'utf8'), Buffer.from(b.name, 'utf8')))
  for (let i = 1; i < sorted.length; i++) if (sorted[i].name === sorted[i - 1].name) throw new Error(`two entries named "${sorted[i].name}"`)
  const bytes = encodePb({ links: sorted.map((e) => ({ cid: e.entry.cid, name: e.name, tsize: e.entry.tsize })), data: encodeUnixfs({ type: TYPE.directory }) })
  if (bytes.length > SHARD_THRESHOLD) throw new NotServable(`a folder with ${sorted.length} entries is too big to be served as one directory; split it up`)
  return { kind: 'directory', cid: pbCid(bytes), bytes, tsize: bytes.length + sorted.reduce((a, e) => a + e.entry.tsize, 0), size: sorted.reduce((a, e) => a + e.entry.size, 0), entries: sorted }
}

/**
 * Every block under `entry`, root first, each CID once — what goes into the
 * CAR. File leaves are re-read from their source; the i-th chunk is the
 * i-th leaf, so nothing is re-hashed.
 */
async function * blocks (entry, seen = new Set()) {
  const once = (cid) => { const k = cid.key(); if (seen.has(k)) return false; seen.add(k); return true }
  if (entry.kind === 'directory') {
    if (once(entry.cid)) yield { cid: entry.cid, bytes: entry.bytes }
    for (const e of entry.entries) yield * blocks(e.entry, seen)
    return
  }
  for (let i = entry.nodes.length - 1; i >= 0; i--) if (once(entry.nodes[i].cid)) yield { cid: entry.nodes[i].cid, bytes: entry.nodes[i].bytes }
  if (entry.leaves.every((l) => seen.has(l.cid.key()))) return
  if (entry.size === 0) { if (once(entry.leaves[0].cid)) yield { cid: entry.leaves[0].cid, bytes: Buffer.alloc(0) }; return }
  let i = 0
  for await (const c of rechunk(entry.source(), entry.chunkSize)) {
    const leaf = entry.leaves[i++]
    if (!leaf || leaf.length !== c.length) throw new Error('the file changed while it was being added')
    if (once(leaf.cid)) yield { cid: leaf.cid, bytes: c }
  }
  if (i !== entry.leaves.length) throw new Error('the file changed while it was being added')
}

module.exports = { TYPE, CHUNK_SIZE, MAX_LINKS, decodePb, decodeUnixfs, describe, resolvePath, fileRange, NotFound, NotServable, encodeUnixfs, encodePb, rechunk, addFile, addDirectory, blocks }
