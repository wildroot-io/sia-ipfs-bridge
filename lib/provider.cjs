'use strict'
/**
 * The provider sidecar (provider/, a stock kubo with our datastore) over its
 * RPC API on the container network. Two jobs:
 *
 * - tell the DHT a CID is here, now — kubo's own sweep walks /api/blocks
 *   only every reprovide interval (22 h), which is a day a fresh pin would be
 *   unfindable by anyone who is not already our peer. The root alone is
 *   announced: a fetcher that has the root's provider asks that same peer
 *   for the children over bitswap.
 *
 * - keep the public gateways connected. The number that decides the product
 *   is a cold gateway's time to first byte, and bitswap sets it: a
 *   gateway asks the peers it is already connected to first and only after
 *   its provider-search delay (1 s in boxo) goes to the DHT (~0.5 s more,
 *   then a dial). Connected, the first byte is one round trip; not, it is
 *   the 1.2–1.6 s measured 2026-09-03. The gateways dial us (ipfs.io's
 *   rainbow nodes crawl every DHT server; they advertise no address of their
 *   own), so the one thing in our hands is not to drop them: kubo's
 *   connection manager trims the oldest connections first — theirs — unless
 *   they are peered. `Peering.Peers` is the documented way for a content
 *   provider to stay close to gateways; we fill it from who is actually on
 *   the line, because the list is not published. `swarm peering add` is
 *   runtime state in kubo 0.43 (not written to the config), so a restarted
 *   provider starts bare and the next tick, within 5 min, peers again.
 */
const TIMEOUT_MS = 30e3

/** Gateway software, by libp2p agent string. rainbow: ipfs.io/dweb.link and every other Shipyard-style gateway; kubo/…/cloudflare: cloudflare-ipfs.com. */
const GATEWAY_AGENT = /^rainbow\/|\/cloudflare\b/
/** A peered gateway that has not been on the line this long is forgotten (redeployed under a new identity, most likely). */
const FORGET_AFTER_MS = 24 * 3600e3

class Provider {
  /**
   * @param {object} o
   * @param {string} o.url   kubo's RPC origin, e.g. http://provider:5001
   * @param {(url, init) => Promise<Response>} [o.fetch]
   */
  constructor ({ url, fetch: fetchImpl = globalThis.fetch }) {
    this.url = String(url).replace(/\/+$/, ''); this.fetch = fetchImpl
    /** @type {Map<string, number>} peered gateway → when it was last seen connected */
    this.seen = new Map()
  }

  async rpc (path, params = {}) {
    const q = new URLSearchParams(params).toString()
    const res = await this.fetch(`${this.url}/api/v0/${path}${q ? `?${q}` : ''}`, { method: 'POST', signal: AbortSignal.timeout(TIMEOUT_MS) })
    if (!res.ok) throw new Error(`provider: ${path}${q ? ` ${q}` : ''}: ${res.status} ${(await res.text()).slice(0, 200)}`)
    return res.text()
  }

  /** Announce one CID (not its children) to the DHT. Throws when the sidecar is unreachable. */
  async provide (cid) {
    await this.rpc('routing/provide', { arg: String(cid), recursive: 'false' }) // a stream of routing events; nothing in it changes what we do
  }

  /** The connected peers running gateway software: [{ id, agent }]. */
  async gateways () {
    const { Peers = [] } = JSON.parse(await this.rpc('swarm/peers', { identify: 'true' }))
    const out = new Map()
    for (const p of Peers) {
      const agent = (p.Identify && p.Identify.AgentVersion) || ''
      if (GATEWAY_AGENT.test(agent)) out.set(p.Peer, agent)
    }
    return [...out].map(([id, agent]) => ({ id, agent }))
  }

  /** The peers kubo keeps connected (the peering subsystem), by id. */
  async peered () {
    const { Peers } = JSON.parse(await this.rpc('swarm/peering/ls'))
    return (Peers || []).map((p) => p.ID)
  }

  /**
   * Peer with every connected gateway that is not yet peered, and forget the
   * peered ones that have been gone a day. Returns what changed.
   * @param {{ at?: number }} [o]
   */
  async keepGatewaysClose ({ at = Date.now() } = {}) {
    const [gateways, peered] = await Promise.all([this.gateways(), this.peered()])
    const kept = new Set(peered)
    for (const id of peered) if (!this.seen.has(id)) this.seen.set(id, at) // peered before this process: gone from now, not from forever
    const added = []
    for (const g of gateways) {
      this.seen.set(g.id, at)
      if (kept.has(g.id)) continue
      await this.rpc('swarm/peering/add', { arg: `/p2p/${g.id}` })
      added.push(g)
    }
    const forgotten = []
    for (const id of peered) {
      if (at - this.seen.get(id) < FORGET_AFTER_MS) continue
      await this.rpc('swarm/peering/rm', { arg: id })
      this.seen.delete(id); forgotten.push(id)
    }
    return { added, forgotten, peered: kept.size + added.length - forgotten.length }
  }
}

module.exports = { Provider, GATEWAY_AGENT }
