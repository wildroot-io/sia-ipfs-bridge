'use strict'
/**
 * The block cache: what the gateway serves without reading a Sia host.
 *
 * Two tiers. RAM holds the recent blocks — an LRU under a byte budget. Disk
 * holds the popular ones: a block is written to disk the second time it is
 * wanted (once is a crawler walking a cold site, twice is an audience), and
 * evicted least-recently-used under its own budget, so a hot site stays on
 * the box across restarts and one scan of a large cold site cannot flush it.
 *
 * Neither tier is a store. Every block here is a copy of a pinned CAR on Sia
 * and the gateway serves a pin's blocks only while the pin exists (it checks
 * the root first). And the cache never changes what is metered: bytes that
 * leave are counted whether they came from RAM, disk or a host — the cache
 * protects the service's Sia float, not the owner's allowance.
 *
 * Disk state is the `cache_blocks` table (key, length, last hit) beside the
 * files under <dir>/<aa>/<key>; a file the table does not know is swept at
 * start, a row whose file is gone is forgotten on read.
 */
const fs = require('fs')
const path = require('path')

const MiB = 1024 * 1024
const TOUCH_MS = 60 * 1000 // a disk hit refreshes last_hit at most this often

class BlockCache {
  /**
   * @param {object} [o]
   * @param {number} [o.memoryBytes]  RAM budget (default 64 MiB)
   * @param {string} [o.dir]          disk tier directory; none → RAM only
   * @param {number} [o.diskBytes]    disk budget (default 10 GiB)
   * @param {object} [o.db]           the service database (required with dir)
   * @param {(line: string) => void} [o.log]
   */
  constructor ({ memoryBytes = 64 * MiB, dir = null, diskBytes = 10 * 1024 * MiB, db = null, log = () => {} } = {}) {
    this.max = memoryBytes; this.size = 0; this.map = new Map()
    this.dir = dir; this.diskMax = diskBytes; this.db = db; this.log = log
    this.diskSize = 0
    this.writes = new Set() // keys being written right now
    if (dir) {
      if (!db) throw new Error('a disk cache needs the database')
      fs.mkdirSync(dir, { recursive: true })
      this.diskSize = Number(db.prepare('SELECT COALESCE(SUM(length), 0) AS b FROM cache_blocks').get().b)
      this.sweep()
    }
  }

  /* --------------------------------------------------------------- RAM */

  /** The block from RAM, refreshed; null when absent. */
  peek (key) {
    const v = this.map.get(key)
    if (v) { this.map.delete(key); this.map.set(key, v) }
    return v || null
  }

  /** Keep a block in RAM (evicting the least recent past the budget). */
  set (key, data) {
    if (data.length > this.max / 4) return
    if (this.map.has(key)) this.size -= this.map.get(key).length
    this.map.set(key, data); this.size += data.length
    while (this.size > this.max) { const [k, v] = this.map.entries().next().value; this.map.delete(k); this.size -= v.length }
  }

  /* -------------------------------------------------------------- disk */

  file (key) { return path.join(this.dir, key.slice(0, 2), key) }

  /** RAM, then disk; a RAM hit is the second ask, which earns the block its place on disk. */
  async get (key) {
    const hot = this.peek(key)
    if (hot) { this.promote(key, hot); return hot }
    if (!this.dir) return null
    const row = this.db.prepare('SELECT length, last_hit FROM cache_blocks WHERE key = ?').get(key)
    if (!row) return null
    let data
    try { data = await fs.promises.readFile(this.file(key)) } catch (e) {
      if (e.code !== 'ENOENT') throw e
      this.forget(key, row.length)
      return null
    }
    if (data.length !== row.length) { await this.evict(key, row.length); return null }
    if (Date.now() - row.last_hit > TOUCH_MS) this.db.prepare('UPDATE cache_blocks SET last_hit = ? WHERE key = ?').run(Date.now(), key)
    this.set(key, data)
    return data
  }

  /** Whether a block is in either tier, without touching it. */
  has (key) {
    if (this.map.has(key)) return true
    return Boolean(this.dir && this.db.prepare('SELECT 1 FROM cache_blocks WHERE key = ?').get(key))
  }

  /** Write a block to disk, once, in the background; the budget is kept after. */
  promote (key, data) {
    if (!this.dir || this.writes.has(key) || data.length > this.diskMax / 4) return
    if (this.db.prepare('SELECT 1 FROM cache_blocks WHERE key = ?').get(key)) return
    this.writes.add(key)
    const file = this.file(key)
    const tmp = `${file}.${process.pid}.tmp`
    fs.promises.mkdir(path.dirname(file), { recursive: true })
      .then(() => fs.promises.writeFile(tmp, data))
      .then(() => fs.promises.rename(tmp, file))
      .then(() => {
        this.db.prepare('INSERT OR REPLACE INTO cache_blocks (key, length, last_hit) VALUES (?, ?, ?)').run(key, data.length, Date.now())
        this.diskSize += data.length
        return this.trim()
      })
      .catch((e) => { this.log(`cache: ${key}: ${e.message}`); fs.promises.unlink(tmp).catch(() => {}) })
      .finally(() => this.writes.delete(key))
  }

  /** Evict least-recently-hit blocks until the disk tier is under budget. */
  async trim () {
    while (this.diskSize > this.diskMax) {
      const victim = this.db.prepare('SELECT key, length FROM cache_blocks ORDER BY last_hit ASC LIMIT 1').get()
      if (!victim) { this.diskSize = 0; return }
      await this.evict(victim.key, victim.length)
    }
  }

  async evict (key, length) {
    this.forget(key, length)
    await fs.promises.unlink(this.file(key)).catch(() => {})
  }

  forget (key, length) {
    if (this.db.prepare('DELETE FROM cache_blocks WHERE key = ?').run(key).changes) this.diskSize -= length
  }

  /** Files the table does not know (a write that died between rename and insert) are removed. */
  sweep () {
    const known = new Set(this.db.prepare('SELECT key FROM cache_blocks').all().map((r) => r.key))
    let swept = 0
    for (const shard of fs.readdirSync(this.dir)) {
      const dir = path.join(this.dir, shard)
      let names
      try { names = fs.readdirSync(dir) } catch { continue }
      for (const name of names) {
        if (known.has(name)) continue
        try { fs.unlinkSync(path.join(dir, name)); swept++ } catch {}
      }
    }
    if (swept) this.log(`cache: swept ${swept} stray files`)
  }

  /** Wait for in-flight disk writes (tests). */
  async idle () {
    while (this.writes.size) await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

module.exports = { BlockCache }
