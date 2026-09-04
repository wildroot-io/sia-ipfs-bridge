# HTTP API

Everything `bridged` answers. The IPFS Pinning Service API has its own page:
[PINNING-API.md](PINNING-API.md).

Authentication is one bearer token out of the box
(`Authorization: Bearer $BRIDGE_TOKEN`); see
[Authentication](#authentication) for replacing that with your own accounts.
Endpoints marked **auth** require it and answer `401` without it.

---

## The gateway

### `GET|HEAD /ipfs/<cid>[/path]`

Public, unauthenticated, and CORS-open. Serves two audiences from the same
blocks.

**For people and plain HTTP clients** — the path is resolved through UnixFS
and the file is returned with a guessed content type. A directory serves its
`index.html`, or a listing if it has none. A directory URL without a trailing
slash redirects (`301`).

**For IPFS software** — the trustless gateway, selected by `?format=` or by
`Accept`:

| | | |
|---|---|---|
| `?format=raw` | `application/vnd.ipld.raw` | one block, hash-checkable |
| `?format=car` | `application/vnd.ipld.car; version=1` | the DAG as a CAR |
| `&dag-scope=all` | | the whole DAG (default) |
| `&dag-scope=block` | | the root block only |

Other behaviour:

- `Range` is supported on files: `206` with `Content-Range`, `416` when
  unsatisfiable. Suffix (`bytes=-N`) and open-ended (`bytes=N-`) forms work.
- `ETag` is the CID; `Cache-Control` is `public, max-age=29030400, immutable`,
  because `/ipfs/<cid>` is the same bytes forever.
- `X-Ipfs-Roots` names the root CID.
- HTML is served under a `Content-Security-Policy: sandbox …`, so a stranger's
  page cannot reach your origin's storage or credentials. Give a site its own
  hostname if it needs to run unsandboxed.
- Identity ("inline") CIDs are served from the request itself and are not
  metered — kubo probes `bafkqaaa` before it will fetch from a gateway.

**Status codes**

| | |
|---|---|
| `400` | not a CID |
| `404` | not pinned here, or no such path in the DAG |
| `429` | the owner is past 1.5× their egress allowance (`Retry-After: 3600`) |
| `503` | storage is not connected right now |

### `GET /routing/v1/providers/<cid>`

[Delegated Routing V1](https://specs.ipfs.tech/routing/http-routing-v1/)
(IPIP-337). Answers `200` with one provider record when the CID is pinned
here, `404` with `{"Providers": []}` when it is not.

```json
{ "Providers": [ {
  "Schema": "peer",
  "ID": "12D3KooW…",
  "Addrs": ["/dns/example.com/tcp/443/https"],
  "Protocols": ["transport-ipfs-gateway-http"]
} ] }
```

Point a client at it with kubo's `Routing.DelegatedRouters`, or Helia's
delegated routing config. `/routing/v1/peers/*` and `/routing/v1/ipns/*` are
`404`: they are not ours to answer.

---

## Pins

### `POST /api/pins` — **auth**

Register an object the client **already stored on its own Sia account**. The
shared object is pinned under the service account, so it outlives the client's
device and plan. No bytes are transferred and nothing is paid for twice.

```json
{ "cid": "bafybei…", "shareUrl": "https://indexer.example.com/share/…#key", "name": "my site" }
```

`shareUrl` comes from the SDK's `shareObject` and must be a link from the
indexer this service is connected to. `201`:

```json
{ "cid": "bafybei…", "size": 455525, "url": "https://example.com/ipfs/bafybei…" }
```

| | |
|---|---|
| `400` | missing fields, a foreign indexer, or the CID is not a root of that archive |
| `402` | the owner's plan has no public pins, or this would exceed their space |
| `503` | storage is not connected |

### `GET /api/pins` — **auth**

Every pin this owner has, newest first: `{ "pins": [ { cid, size, indexed, name, created_at } ] }`.

---

## Uploads

For content that is not on IPFS yet. Files are hashed into exactly the DAG
kubo would build (`ipfs add --cid-version=1`, proven block for block in the
test suite), written as a CAR on disk, and queued like any other pin request.

### `PUT /api/upload?filename=<name>&name=<label>` — **auth**

One file, one call. The body is streamed to disk, so it may be far larger than
memory.

```sh
curl -T ./index.html -H "Authorization: Bearer $TOKEN" \
     "https://example.com/api/upload?filename=index.html"
```

`202` — the CID is final and shareable immediately; the bytes are still on
their way to Sia.

```json
{ "cid": "bafkrei…", "url": "https://example.com/ipfs/bafkrei…/", "bytes": 15,
  "request": { "requestid": "…", "status": "queued", … } }
```

### Folders

| | |
|---|---|
| `POST /api/uploads` | `{ "name": "site", "folder": true }` → `201` a session `{id, …}` |
| `PUT /api/uploads/<id>/files/<path>` | raw bytes, streamed; `path` may be nested (`sub/page.html`) |
| `POST /api/uploads/<id>/finish` | `202` — hashes, writes the CAR, queues the pin |

Paths are validated: no absolute paths, no `.` or `..`, no NUL, 255 bytes per
segment. A session expires if it is not finished, and `sweep()` reclaims both
the files and a settled request's CAR.

Sizes are checked against the plan **while bytes arrive** and again,
definitively, at `finish`.

---

## Operational

### `GET /api/health`

Unauthenticated. `{ "ok": true, "storage": true, "pins": 42 }` — `storage` is
whether the Sia session is currently open.

### `GET /api/blocks` — **internal only**

The newline-separated list of every block held, in canonical CID form. This is
what the kubo sidecar reprovides to the DHT.

It must never be reachable from outside. Any request carrying an
`X-Forwarded-For` header gets a `404`, on the reasoning that a reverse proxy
stamps that header on everything it forwards — so its presence means the
request came through the edge. Bind the service to the container network and
proxy only `/ipfs/`, `/routing/v1/`, `/pins` and `/api/…` deliberately.

---

## Authentication

The default is one constant-time bearer token (`BRIDGE_TOKEN`) mapping to one
owner (`BRIDGE_OWNER`). That is a single-tenant deployment.

To put it behind real accounts, replace two functions and change nothing else:

```js
const { App } = require('sia-ipfs-bridge/bin/bridged.cjs')
const policy = require('sia-ipfs-bridge/lib/policy.cjs')

new App({
  cfg, db, ix,
  // request → owner id (a session, an API key, a signature, anything)
  ownerOf: (req) => yourAuth(req),
  // owner id → plan
  policy: policy.of(YOUR_PLANS, (id) => yourAccount(id))
})
```

A plan is four fields:

| | |
|---|---|
| `title` | shown in refusal messages |
| `publicPins` | may this owner have public pins at all |
| `storageBytes` | ceiling on the CAR bytes they are responsible for |
| `publicBytesPerMonth` | egress allowance; `0` means uncapped |

Owner ids are opaque strings. The bridge never interprets them.
