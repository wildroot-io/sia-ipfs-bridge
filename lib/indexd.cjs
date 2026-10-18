'use strict'
/**
 * A client for indexd, the Sia Storage indexer: the admin API (keep it on
 * loopback or a private network — it is basic auth with the admin password)
 * for quotas, connect keys, usage and the wallet; and the application API
 * for the one thing done there on a user's behalf — approving a pending app
 * connection with their connect key.
 *
 * Every call is a plain fetch with the error text in the exception, because
 * indexd's errors are the useful part ("key in use", "quota not found").
 * Nothing here retries: the callers are ticks that run again in a minute.
 *
 * `capabilities()` reports whether the indexer answers `GET /stats/funding`
 * (per-connect-key funding attribution). Upstream indexd does not, and has no
 * per-key funding cap either: funding is the only lever an indexer holds over
 * a client that talks straight to hosts. See "Metering and quotas" in
 * docs/ARCHITECTURE.md.
 */

/** Every call out to the indexer is bounded. */
const TIMEOUT_MS = 30e3

class Indexd {
  constructor ({ adminUrl, adminPassword, appUrl, fetch: fetchImpl = globalThis.fetch }) {
    if (!adminUrl || !appUrl) throw new Error('indexd needs adminUrl and appUrl')
    this.adminUrl = adminUrl.replace(/\/+$/, '')
    this.appUrl = appUrl.replace(/\/+$/, '')
    this.adminPassword = adminPassword || ''
    this.fetch = fetchImpl
  }

  async admin (method, pathname, body) {
    const res = await this.fetch(this.adminUrl + pathname, {
      method,
      headers: {
        authorization: 'Basic ' + Buffer.from(':' + this.adminPassword).toString('base64'),
        ...(body !== undefined ? { 'content-type': 'application/json' } : {})
      },
      body: body !== undefined ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(TIMEOUT_MS)
    })
    const text = await res.text()
    if (!res.ok) throw new Error(`indexd ${method} ${pathname} → ${res.status} ${text.trim().slice(0, 300)}`)
    return text ? JSON.parse(text) : null
  }

  /* -------------------------------------------------------------- quotas */

  quota (key) { return this.admin('GET', `/quotas/${encodeURIComponent(key)}`) }
  quotas () { return this.admin('GET', '/quotas') }
  putQuota (key, q) { return this.admin('PUT', `/quotas/${encodeURIComponent(key)}`, q) }

  /* ----------------------------------------------------------- contracts */

  contractSettings () { return this.admin('GET', '/settings/contracts') }
  /** Contract maintenance on. Off in a fresh indexd, and nothing forms until it is. */
  async enableContracts () {
    const cur = await this.contractSettings()
    if (cur.enabled) return cur
    const next = { ...cur, enabled: true }
    await this.admin('PUT', '/settings/contracts', next)
    return next
  }

  /* -------------------------------------------------------- connect keys */

  connectKey (key) { return this.admin('GET', `/apps/connect/keys/${encodeURIComponent(key)}`) }
  /** Every connect key; indexd pages at 500 at most. */
  async connectKeys () {
    const all = []
    for (let offset = 0; ; offset += 500) {
      const page = await this.admin('GET', `/apps/connect/keys?limit=500&offset=${offset}`)
      all.push(...page)
      if (page.length < 500) return all
    }
  }

  /** Empty `key` lets the server generate one; the response carries it. */
  addConnectKey ({ description, quota }) { return this.admin('POST', '/apps/connect/keys', { key: '', description, quota }) }
  /** Update. `fundingPeriodStart` (ISO) rolls the period over — the cap resets. */
  putConnectKey ({ key, description, quota, fundingPeriodStart }) {
    return this.admin('PUT', '/apps/connect/keys', {
      key, description, quota, ...(fundingPeriodStart ? { fundingPeriodStart } : {})
    })
  }

  deleteConnectKey (key) { return this.admin('DELETE', `/apps/connect/keys/${encodeURIComponent(key)}`) }
  accounts (connectKey) { return this.admin('GET', `/accounts?connectkey=${encodeURIComponent(connectKey)}`) }
  deleteAccount (accountKey) { return this.admin('DELETE', `/account/${encodeURIComponent(accountKey)}`) }

  /* --------------------------------------------------------------- usage */

  /** [{connectKey, amount, events, capped}] — attributed host funding. */
  fundingStats (since, until) {
    const q = new URLSearchParams()
    if (since) q.set('since', since)
    if (until) q.set('until', until)
    const s = q.toString()
    return this.admin('GET', '/stats/funding' + (s ? `?${s}` : ''))
  }

  wallet () { return this.admin('GET', '/wallet') }
  state () { return this.admin('GET', '/state') }
  contracts () { return this.admin('GET', '/contracts') }
  contractsStats () { return this.admin('GET', '/stats/contracts') }
  exchangeRate (currency = 'usd') { return this.admin('GET', `/explorer/exchange-rate/siacoin/${currency}`) }

  /**
   * Does the indexer answer `/stats/funding`? Upstream does not; a 404 is
   * reported as `funding: false`. An unreachable indexer throws instead —
   * that is a different problem with a different fix.
   */
  async capabilities () {
    const state = await this.admin('GET', '/state')
    let funding = true
    try { await this.admin('GET', '/stats/funding') } catch (e) { if (!/→ 404/.test(e.message)) throw e; funding = false }
    return { version: state.version, synced: Boolean(state.synced), funding }
  }

  /* --------------------------------------------- app API: approve for user */

  /**
   * Approve a pending app connection on the user's behalf. This is exactly
   * what the indexer's own approval page does after the person types their
   * connect key; here the key never leaves the server and the person never
   * sees a page.
   */
  async approveConnection (requestId, connectKey) {
    if (!/^[0-9a-f]{32}$/.test(requestId)) throw new Error('bad connection request id')
    const res = await this.fetch(`${this.appUrl}/auth/connect/${requestId}`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        authorization: 'Basic ' + Buffer.from(':' + connectKey).toString('base64')
      },
      body: JSON.stringify({ approve: true }),
      signal: AbortSignal.timeout(TIMEOUT_MS)
    })
    const text = await res.text()
    if (!res.ok) throw new Error(`approve ${requestId} → ${res.status} ${text.trim().slice(0, 200)}`)
    return true
  }
}

/** The request id out of a ResponseURL the SDK handed the app. */
function requestIdOf (url) {
  const m = /\/auth\/connect\/([0-9a-f]{32})(?:[/?#]|$)/.exec(String(url || ''))
  return m ? m[1] : null
}

module.exports = { Indexd, requestIdOf }
