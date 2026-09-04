'use strict'
/**
 * Delegated routing: how the rest of IPFS finds what is pinned here.
 *
 * A CID lives on Sia, behind this gateway. On its own that is invisible to
 * the network: a node asked for the CID has nobody to fetch it from. This is
 * the first of the two answers to that (the other is the kubo sidecar in
 * provider/, which speaks bitswap on the DHT).
 *
 * The Delegated Routing V1 HTTP API (IPIP-337): `GET
 * /routing/v1/providers/<cid>` says "this peer has it, over HTTP", and a
 * node with HTTP retrieval (kubo 0.35+, verified-fetch, Helia) then reads
 * the blocks from `/ipfs/<cid>?format=raw` on the gateway, hash-checking
 * each one. Point a client's `Routing.DelegatedRouters` here and a CID
 * pinned only on your Sia account resolves in it. Every block served this
 * way goes through the gateway's metering like any other byte.
 *
 * Provider records name a peer. The peer is this service: an Ed25519 key
 * made once and kept in the state directory, its peer ID in every answer.
 * Nothing speaks libp2p with it today — HTTP retrieval never dials the ID,
 * it dials the address — but the record must carry a real one, and a
 * bitswap sidecar tomorrow can be the same peer.
 */
const crypto = require('crypto')
const fs = require('fs')
const path = require('path')

const { CID, base58Encode } = require('./cid.cjs')
const pins = require('./pins.cjs')

/** What we speak: the trustless HTTP gateway, nothing else. */
const PROTOCOL = 'transport-ipfs-gateway-http'
const KEY_FILE = 'peer-key.json'
/**
 * The same key as kubo's `Identity` config section, for the provider sidecar
 * (provider/): a kubo whose peer ID is this one, so the record above and the
 * DHT records it announces name the same peer. PrivKey is libp2p's protobuf
 * (type ed25519, the 64-byte private||public seed) in base64.
 */
const KUBO_IDENTITY_FILE = 'kubo-identity.json'

/**
 * The peer ID of an Ed25519 public key: the identity multihash of the
 * libp2p PublicKey protobuf (type 1, 32 bytes), base58btc — `12D3KooW…`.
 */
function peerIdOf (publicKey) {
  const proto = Buffer.concat([Buffer.from([0x08, 0x01, 0x12, 0x20]), publicKey])
  return base58Encode(Buffer.concat([Buffer.from([0x00, proto.length]), proto]))
}

/** The service's peer key: read from the state directory, made on first use. */
function peerKey (stateDir) {
  const file = path.join(stateDir, KEY_FILE)
  let saved = null
  try { saved = JSON.parse(fs.readFileSync(file, 'utf8')) } catch {}
  let pub, priv
  if (saved && saved.type === 'ed25519' && /^[0-9a-f]{64}$/.test(saved.publicKey) && /^[0-9a-f]{64}$/.test(saved.privateKey)) {
    pub = Buffer.from(saved.publicKey, 'hex'); priv = Buffer.from(saved.privateKey, 'hex')
  } else {
    const pair = crypto.generateKeyPairSync('ed25519')
    // Raw 32-byte keys: SPKI DER is a fixed 12-byte prefix + key, PKCS8 a 16-byte one.
    pub = pair.publicKey.export({ type: 'spki', format: 'der' }).subarray(-32)
    priv = pair.privateKey.export({ type: 'pkcs8', format: 'der' }).subarray(-32)
    fs.mkdirSync(stateDir, { recursive: true })
    fs.writeFileSync(file, JSON.stringify({ type: 'ed25519', publicKey: pub.toString('hex'), privateKey: priv.toString('hex') }), { mode: 0o600 })
  }
  const id = peerIdOf(pub)
  const identity = { PeerID: id, PrivKey: Buffer.concat([Buffer.from([0x08, 0x01, 0x12, 0x40]), priv, pub]).toString('base64') }
  const kuboFile = path.join(stateDir, KUBO_IDENTITY_FILE)
  let have = null
  try { have = fs.readFileSync(kuboFile, 'utf8') } catch {}
  if (have !== JSON.stringify(identity)) fs.writeFileSync(kuboFile, JSON.stringify(identity), { mode: 0o600 })
  return { publicKey: pub, id }
}

/** The gateway's origin as the multiaddr a provider record carries. */
function addrOf (origin) {
  const u = new URL(origin)
  const https = u.protocol === 'https:'
  const port = u.port || (https ? 443 : 80)
  const host = /^\d+\.\d+\.\d+\.\d+$/.test(u.hostname) ? `/ip4/${u.hostname}` : `/dns/${u.hostname}`
  return `${host}/tcp/${port}/${https ? 'https' : 'http'}`
}

class Routing {
  /**
   * @param {object} o
   * @param {object} o.db
   * @param {{origin: string, state: string}} o.cfg  where the gateway answers
   *   (e.g. https://example.com) and the state directory (holds the peer key)
   */
  constructor ({ db, cfg }) {
    this.db = db
    this.cfg = cfg
    this.peer = peerKey(cfg.state)
  }

  /** The one provider record we ever give. */
  record () {
    return { Schema: 'peer', ID: this.peer.id, Addrs: [addrOf(this.cfg.origin)], Protocols: [PROTOCOL] }
  }

  /**
   * Answer `/routing/v1/*`. Only providers are ours to answer; peers and
   * IPNS are 404 (the browser registers the providers path alone).
   * @param {{method, path, headers}} req
   */
  handle (req) {
    const m = /^\/routing\/v1\/providers\/([^/?#]+)$/.exec(req.path)
    if (!m) return reply(404, { Providers: [] })
    if (req.method !== 'GET' && req.method !== 'HEAD') return reply(405, { Providers: [] })
    let cid
    try { cid = CID.parse(decodeURIComponent(m[1])) } catch { return reply(400, { Providers: [] }) }
    const held = pins.locate(this.db, cid)
    const body = { Providers: held ? [this.record()] : [] }
    return reply(held ? 200 : 404, body, req.method === 'HEAD')
  }
}

function reply (status, body, headOnly = false) {
  const buf = Buffer.from(JSON.stringify(body))
  return {
    status,
    headers: {
      'content-type': 'application/json',
      'content-length': String(buf.length),
      // A lookup answer is small and changes only when the pin does.
      'cache-control': status === 200 ? 'public, max-age=300' : 'public, max-age=30',
      'access-control-allow-origin': '*'
    },
    body: headOnly ? null : buf
  }
}

module.exports = { Routing, PROTOCOL, KUBO_IDENTITY_FILE, peerIdOf, peerKey, addrOf }
