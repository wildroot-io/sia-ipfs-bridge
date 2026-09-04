'use strict'
/**
 * One SQLite file holds everything the bridge knows: the pin table, the
 * block index, the Pinning Service API queue, the disk cache's bookkeeping
 * and per-owner egress.
 *
 * `node:sqlite` (Node >= 22.13, so there is no native dependency to build)
 * in WAL mode; every write is a statement, so a crash mid-request leaves a
 * consistent file.
 *
 * The indexer holds the other half — quotas, connect keys, funding — and is
 * the authority for it. What lives here is what the indexer cannot know: who
 * owns which public pin, and where each block sits inside its archive.
 *
 * Owners are opaque strings. The bridge never defines what an owner IS: an
 * account id, a public key, a tenant, or the single constant owner the
 * default policy hands out (lib/policy.cjs). Whatever your service calls a
 * customer goes in this column.
 */
const { DatabaseSync } = require('node:sqlite')

const SCHEMA = [
  // A public object: CID -> the Sia object pinned under the service account
  // (`sdk.object(object_id)` reopens it). Lives while any pinner remains.
  `CREATE TABLE IF NOT EXISTS pins (
     cid TEXT PRIMARY KEY,
     object_id TEXT NOT NULL,
     size INTEGER NOT NULL,
     car_size INTEGER NOT NULL,
     indexed INTEGER NOT NULL DEFAULT 0,
     announced_at TEXT,
     created_at TEXT NOT NULL
   )`,
  // Who asked for this CID. A pin is alive while any pinner remains; the
  // OLDEST pinner is the one it is metered to.
  `CREATE TABLE IF NOT EXISTS pinners (
     cid TEXT NOT NULL,
     account_id TEXT NOT NULL,
     name TEXT,
     created_at TEXT NOT NULL,
     PRIMARY KEY (cid, account_id)
   )`,
  'CREATE INDEX IF NOT EXISTS pinners_account ON pinners(account_id)',
  // CARv2-style index: which block sits where inside the CAR object. This is
  // the table that turns a Sia object into a block store.
  `CREATE TABLE IF NOT EXISTS blocks (
     block_cid TEXT NOT NULL,
     cid TEXT NOT NULL,
     offset INTEGER NOT NULL,
     length INTEGER NOT NULL,
     PRIMARY KEY (block_cid, cid)
   )`,
  // IPFS Pinning Service API requests. One row per requestid; status follows
  // the spec's queued -> pinning -> pinned | failed.
  `CREATE TABLE IF NOT EXISTS pin_requests (
     id TEXT PRIMARY KEY,
     account_id TEXT NOT NULL,
     cid TEXT NOT NULL,
     name TEXT,
     origins TEXT,
     meta TEXT,
     status TEXT NOT NULL,
     info TEXT,
     attempts INTEGER NOT NULL DEFAULT 0,
     upload_id TEXT,
     created_at TEXT NOT NULL,
     updated_at TEXT NOT NULL
   )`,
  'CREATE INDEX IF NOT EXISTS pin_requests_account ON pin_requests(account_id, created_at)',
  'CREATE INDEX IF NOT EXISTS pin_requests_status ON pin_requests(status)',
  // Files sent to the service to be pinned (lib/uploads.cjs): an open
  // session collects them on disk; finishing it hashes them into a CAR and
  // queues a pin request carrying the session id (pin_requests.upload_id).
  `CREATE TABLE IF NOT EXISTS uploads (
     id TEXT PRIMARY KEY,
     account_id TEXT NOT NULL,
     name TEXT,
     folder INTEGER NOT NULL DEFAULT 0,
     bytes INTEGER NOT NULL DEFAULT 0,
     files INTEGER NOT NULL DEFAULT 0,
     cid TEXT,
     car_size INTEGER,
     created_at TEXT NOT NULL,
     expires_at TEXT NOT NULL,
     finished_at TEXT
   )`,
  // Bytes served from public pins, per owner per UTC day, and per CID.
  `CREATE TABLE IF NOT EXISTS egress (
     day TEXT NOT NULL,
     account_id TEXT NOT NULL,
     cid TEXT NOT NULL,
     bytes INTEGER NOT NULL,
     PRIMARY KEY (day, account_id, cid)
   )`,
  // The disk tier of the block cache (lib/cache.cjs): one row per file.
  `CREATE TABLE IF NOT EXISTS cache_blocks (
     key TEXT PRIMARY KEY,
     length INTEGER NOT NULL,
     last_hit INTEGER NOT NULL
   )`,
  'CREATE INDEX IF NOT EXISTS cache_blocks_hit ON cache_blocks(last_hit)'
]

function open (file) {
  const db = new DatabaseSync(file)
  db.exec('PRAGMA journal_mode = WAL')
  db.exec('PRAGMA foreign_keys = ON')
  db.exec('PRAGMA busy_timeout = 5000')
  for (const s of SCHEMA) db.exec(s)
  return db
}

const now = () => new Date().toISOString()

/** Run `fn` inside one transaction; rethrows after rollback. */
function tx (db, fn) {
  db.exec('BEGIN IMMEDIATE')
  try {
    const out = fn()
    db.exec('COMMIT')
    return out
  } catch (e) {
    try { db.exec('ROLLBACK') } catch {}
    throw e
  }
}

module.exports = { open, now, tx, SCHEMA }
