'use strict'
/**
 * Uploads: the way to pin something that is not on IPFS yet. A caller sends
 * a file or a folder (a browser drop, or `curl -T`); we hash it into the DAG
 * kubo would build (unixfs.cjs), write that DAG as a CAR, and queue a pin
 * request that
 * the ordinary worker uploads to Sia and indexes — the same path a CID
 * fetched from a public gateway takes, so there is one way to become a pin.
 *
 *   POST /api/uploads                  → a session {id}; `folder` says whether
 *                                        the root is a directory of everything
 *                                        sent or the single file sent
 *   PUT  /api/uploads/<id>/files/<path> raw bytes, streamed to disk
 *   POST /api/uploads/<id>/finish      → hash, CAR, queue → 202 {cid, url, request}
 *
 * Files live under <state>/uploads/<id>/files until finish turns them into
 * <state>/uploads/<id>/dag.car; the CAR lives until the request is settled,
 * an open session until it expires. sweep() reclaims both. Bytes are counted
 * against the plan while they arrive (a session is refused past the room the
 * plan has left) and once more, definitively, at finish.
 */
const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const { pipeline } = require('stream/promises')
const { Transform } = require('stream')
const { now } = require('./db.cjs')
const unixfs = require('./unixfs.cjs')
const car = require('./car.cjs')

const TTL_MS = 6 * 3600e3 // an open session lives this long
const MAX_FILES = 10000
const MAX_OPEN = 8 // open sessions per account

const dirOf = (root, id) => path.join(root, 'uploads', id)
const filesDir = (root, id) => path.join(dirOf(root, id), 'files')
const carFile = (root, id) => path.join(dirOf(root, id), 'dag.car')

const get = (db, id) => db.prepare('SELECT * FROM uploads WHERE id = ?').get(id) || null

function create (db, root, { accountId, name = null, folder = false, at = Date.now() }) {
  const open = db.prepare('SELECT COUNT(*) AS n FROM uploads WHERE account_id = ? AND finished_at IS NULL').get(accountId).n
  if (open >= MAX_OPEN) { const e = new Error(`${MAX_OPEN} uploads are already open on this account; finish or drop one first`); e.status = 429; throw e }
  const id = crypto.randomUUID()
  db.prepare('INSERT INTO uploads (id, account_id, name, folder, bytes, files, created_at, expires_at) VALUES (?, ?, ?, ?, 0, 0, ?, ?)')
    .run(id, accountId, name, folder ? 1 : 0, new Date(at).toISOString(), new Date(at + TTL_MS).toISOString())
  fs.mkdirSync(filesDir(root, id), { recursive: true })
  return get(db, id)
}

/**
 * `a/b/c.txt` → the same, or throws: no absolute paths, no `.`/`..`, no
 * empty or NUL segments, nothing a filesystem or a UnixFS link would choke
 * on. Segments are kept as sent (bytewise names are what the DAG sorts by).
 */
function safePath (p) {
  const s = String(p || '').replace(/\\/g, '/')
  const segs = s.split('/').filter((x) => x !== '')
  if (!segs.length) throw new Error('a file needs a name')
  if (s.length > 4096) throw new Error('that path is too long')
  for (const seg of segs) {
    if (seg === '.' || seg === '..' || seg.includes('\0')) throw new Error(`"${seg}" is not a valid file name`)
    if (Buffer.byteLength(seg) > 255) throw new Error(`"${seg.slice(0, 40)}…" is too long for a file name`)
  }
  return segs.join('/')
}

/**
 * Store one file. `readable` is the bytes; `room` is how many more bytes the
 * plan can take. The write goes to a temporary name and is renamed into
 * place, so a broken connection leaves nothing behind.
 */
async function put (db, root, up, relPath, readable, { room, declared = null }) {
  if (up.finished_at) { const e = new Error('that upload is already finished'); e.status = 409; throw e }
  if (up.files >= MAX_FILES) { const e = new Error(`an upload holds at most ${MAX_FILES} files`); e.status = 413; throw e }
  if (declared != null && declared > room) { const e = new Error('that would exceed the space on your plan'); e.status = 402; throw e }
  const rel = safePath(relPath)
  const file = path.join(filesDir(root, up.id), ...rel.split('/'))
  fs.mkdirSync(path.dirname(file), { recursive: true })
  let was = -1
  try { was = fs.statSync(file).size } catch {}
  const tmp = `${file}.part-${crypto.randomBytes(4).toString('hex')}`
  let n = 0
  const count = new Transform({
    transform (chunk, _enc, cb) {
      n += chunk.length
      if (n > room) { const e = new Error('that would exceed the space on your plan'); e.status = 402; cb(e) } else cb(null, chunk)
    }
  })
  try {
    await pipeline(readable, count, fs.createWriteStream(tmp))
    fs.renameSync(tmp, file)
  } catch (e) {
    try { fs.unlinkSync(tmp) } catch {}
    throw e
  }
  db.prepare('UPDATE uploads SET bytes = bytes - ? + ?, files = files + ? WHERE id = ?').run(was < 0 ? 0 : was, n, was < 0 ? 1 : 0, up.id)
  return { path: rel, bytes: n, upload: get(db, up.id) }
}

/** The tree on disk, hashed. Directory listings are read in name order; the DAG sorts them itself. */
async function hashTree (dir, opts) {
  const entries = []
  for (const name of fs.readdirSync(dir)) {
    const f = path.join(dir, name)
    const st = fs.lstatSync(f)
    if (st.isDirectory()) entries.push({ name, entry: await hashTree(f, opts) })
    else if (st.isFile()) entries.push({ name, entry: await unixfs.addFile(() => fs.createReadStream(f), opts) })
  }
  return unixfs.addDirectory(entries)
}

/**
 * Turn the session's files into a CAR on disk and return {cid, size}. The
 * source files go as soon as the CAR exists. A single-file session's root is
 * the file itself; a folder session's root is the directory of everything
 * sent, however it was nested.
 */
async function finish (db, root, up, { chunkSize } = {}) {
  if (up.finished_at) { const e = new Error('that upload is already finished'); e.status = 409; throw e }
  if (!up.files) { const e = new Error('nothing was uploaded'); e.status = 400; throw e }
  const opts = chunkSize ? { chunkSize } : {}
  const tree = await hashTree(filesDir(root, up.id), opts)
  let entry = tree
  if (!up.folder) {
    if (tree.entries.length !== 1 || tree.entries[0].entry.kind !== 'file') { const e = new Error('a single-file upload got more than one file; make it a folder upload'); e.status = 400; throw e }
    entry = tree.entries[0].entry
  }
  const out = carFile(root, up.id)
  await pipeline(carChunks(entry), fs.createWriteStream(out))
  fs.rmSync(filesDir(root, up.id), { recursive: true, force: true })
  const size = fs.statSync(out).size
  db.prepare('UPDATE uploads SET cid = ?, car_size = ?, finished_at = ? WHERE id = ?').run(entry.cid.toString(), size, now(), up.id)
  return { cid: entry.cid, size, bytes: entry.size }
}

async function * carChunks (entry) {
  yield car.header([entry.cid])
  for await (const b of unixfs.blocks(entry)) yield car.section(b.cid, b.bytes)
}

/** The finished CAR as a Readable, for the pin worker. */
function carStream (root, id) {
  const f = carFile(root, id)
  if (!fs.existsSync(f)) throw Object.assign(new Error('the uploaded files are gone; upload them again'), { final: true })
  return fs.createReadStream(f)
}

function drop (db, root, id) {
  db.prepare('DELETE FROM uploads WHERE id = ?').run(id)
  fs.rmSync(dirOf(root, id), { recursive: true, force: true })
}

/**
 * Reclaim disk: open sessions past their expiry, and finished ones whose pin
 * request has settled (or never existed). Returns the ids dropped.
 */
function sweep (db, root, at = Date.now()) {
  const rows = db.prepare(`SELECT u.id, u.finished_at, u.expires_at,
      (SELECT status FROM pin_requests r WHERE r.upload_id = u.id ORDER BY r.created_at DESC LIMIT 1) AS status
    FROM uploads u`).all()
  const gone = []
  for (const r of rows) {
    const stale = r.finished_at ? (r.status !== 'queued' && r.status !== 'pinning') : r.expires_at < new Date(at).toISOString()
    if (stale) { drop(db, root, r.id); gone.push(r.id) }
  }
  return gone
}

const view = (u) => ({ id: u.id, name: u.name, folder: Boolean(u.folder), bytes: u.bytes, files: u.files, cid: u.cid, expires: u.expires_at, finished: Boolean(u.finished_at) })

module.exports = { TTL_MS, MAX_FILES, MAX_OPEN, get, create, safePath, put, finish, carStream, drop, sweep, view, dirOf }
