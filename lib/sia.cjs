'use strict'
/**
 * The bridge's own Sia session: the service is itself an app on the indexer,
 * with one account that holds every PUBLIC pin, so an object outlives the
 * device that uploaded it and stays readable by the gateway.
 *
 * Nothing private should ever come through this account. A client-sealed
 * backup is the client's own app key and this session cannot read a byte of
 * it; everything here is public content, so the object's key material may
 * safely be stored beside the pin.
 *
 * Bootstrap (`bridged connect`, once per box) is the ordinary SDK connect
 * flow, approved by the service itself with a connect key on its own quota;
 * the resulting app key + entropy are written to the session file (0600).
 * The recovery phrase is derived from the entropy for `register()` and
 * dropped; it is never logged.
 *
 * The SDK's `exports` map picks the wasm or the native build by condition
 * and the difference does not surface until the first upload — so require
 * the package plainly and let the `node` condition give you the N-API build.
 */
const fs = require('fs')
const path = require('path')
const crypto = require('crypto')
const { Readable } = require('stream')
const { requestIdOf } = require('./indexd.cjs')

const SERVICE_QUOTA = 'service'
const SERVICE_KEY_DESC = 'bridge:service'
const GB = 1024 ** 3

/**
 * The quota the service account runs on. `fundTargetBytes` is what the
 * contract manager keeps pre-funded on every host for each active account —
 * the indexer requires it, and it scales the float by hosts x accounts, so
 * keep it modest. `fundingCap` (a hastings digit string, or null for
 * unlimited) is the per-period ceiling: the only lever an indexer has over a
 * client that talks straight to hosts.
 */
const SERVICE_QUOTA_SPEC = {
  description: 'sia-ipfs-bridge service account',
  maxPinnedData: 4 * 1024 ** 5,
  totalUses: 4,
  fundTargetBytes: GB,
  fundingCap: null
}

let sdkModule = null
function sdk () {
  // The package's `node` export condition is the N-API build (index.node.cjs).
  if (!sdkModule) sdkModule = require('@siafoundation/sia-storage')
  return sdkModule
}

/**
 * How this service names itself to the indexer. `id` MUST be 32 bytes,
 * generated once (`openssl rand -hex 32`) and then never changed — the app
 * key derives from it, so a new id is a new account with none of your pins.
 * It is not a secret; the indexer shows it at approval.
 */
function appMeta (app) {
  if (!app || !/^[0-9a-f]{64}$/.test(String(app.id || ''))) {
    throw new Error('the app needs a 32-byte hex id (openssl rand -hex 32), generated once and kept')
  }
  return {
    id: Buffer.from(app.id, 'hex'),
    name: app.name,
    description: app.description || 'Public pins served to IPFS and the web.',
    serviceUrl: app.serviceUrl
  }
}

/** The session file → {indexer, appKeyHex, entropyHex} or null. */
function loadSession (file) {
  if (!fs.existsSync(file)) return null
  const j = JSON.parse(fs.readFileSync(file, 'utf8'))
  if (!j.appKeyHex || !j.indexer) throw new Error(`${file}: needs indexer and appKeyHex`)
  return j
}

function saveSession (file, session) {
  fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 })
  fs.writeFileSync(file, JSON.stringify(session, null, 2) + '\n', { mode: 0o600 })
}

/**
 * The stored session follows the advertised indexer URL: the app key is
 * the same after staging → live, only the host signatures bind to changed.
 * Returns the session, rewritten on disk when it moved.
 */
function follow (file, session, indexer, log = () => {}) {
  if (session.indexer === indexer) return session
  log(`${file} was connected at ${session.indexer}; the indexer now advertises ${indexer} — following it`)
  const moved = { ...session, indexer, movedAt: new Date().toISOString() }
  saveSession(file, moved)
  return moved
}

/** An Sdk from a stored app key, or null when the indexer no longer accepts it. */
async function open (session, app) {
  const { Builder, AppKey } = sdk()
  const key = new AppKey(Buffer.from(session.appKeyHex, 'hex'))
  return new Builder(session.indexer, appMeta(app)).connected(key)
}

/**
 * First connection. Needs the admin API (to make the service quota and key
 * and approve ourselves) and a writable `file`. Idempotent on the indexer
 * side; refuses to overwrite an existing session file.
 */
async function bootstrap (ix, { indexer, file, app, quota = SERVICE_QUOTA_SPEC }) {
  if (fs.existsSync(file)) throw new Error(`${file} exists; remove it to reconnect`)
  const { Builder } = sdk()
  const { entropyToMnemonic } = require('@scure/bip39')
  const { wordlist } = require('@scure/bip39/wordlists/english')

  await ix.putQuota(SERVICE_QUOTA, quota)
  let key = (await ix.connectKeys()).find((k) => k.description === SERVICE_KEY_DESC)
  if (!key) key = await ix.addConnectKey({ description: SERVICE_KEY_DESC, quota: SERVICE_QUOTA })

  const builder = new Builder(indexer, appMeta(app))
  await builder.requestConnection()
  const requestId = requestIdOf(builder.responseUrl())
  if (!requestId) throw new Error(`no request id in ${builder.responseUrl()}`)
  await ix.approveConnection(requestId, key.key)
  await builder.waitForApproval()

  const entropy = crypto.randomBytes(16)
  let phrase = entropyToMnemonic(entropy, wordlist)
  let session
  try {
    session = await builder.register(phrase)
  } finally {
    phrase = null
  }
  const appKeyHex = Buffer.from(session.appKey().export()).toString('hex')
  saveSession(file, { indexer, appKeyHex, entropyHex: entropy.toString('hex'), connectedAt: new Date().toISOString() })
  return { indexer, publicKey: session.appKey().publicKey() }
}

/* ------------------------------------------------------------- objects */

/**
 * Take a share URL a client made (`sdk.shareObject`), pin the object under
 * the service account, and hand back its id and size. No bytes move: the
 * object is already on hosts. After this the client's own pin can lapse
 * without the public copy going anywhere.
 */
async function pinShared (s, shareUrl) {
  const obj = await s.sharedObject(shareUrl)
  await s.pinObject(obj)
  return { id: obj.id(), size: Number(obj.size()) }
}

/** Re-open a pinned object by id. */
const object = (s, id) => s.object(id)

/** Bytes [offset, offset+length) of an object as one Buffer. */
async function readRange (s, obj, offset, length) {
  const stream = s.download(obj, { offset: BigInt(offset), length: BigInt(length) })
  const chunks = []
  for await (const c of Readable.fromWeb(stream)) chunks.push(Buffer.from(c))
  return Buffer.concat(chunks)
}

/** A Node Readable over [offset, offset+length) of an object. */
function streamRange (s, obj, offset, length) {
  return Readable.fromWeb(s.download(obj, { offset: BigInt(offset), length: BigInt(length) }))
}

/** Upload a Node Readable as a new object under the service account, pinned. */
async function upload (s, readable) {
  const { PinnedObject } = sdk()
  const obj = await s.upload(new PinnedObject(), Readable.toWeb(readable))
  await s.pinObject(obj)
  return { id: obj.id(), size: Number(obj.size()), object: obj }
}

async function unpin (s, id) { await s.deleteObject(id) }

module.exports = { SERVICE_QUOTA, SERVICE_KEY_DESC, SERVICE_QUOTA_SPEC, sdk, appMeta, loadSession, follow, open, bootstrap, pinShared, object, readRange, streamRange, upload, unpin }
