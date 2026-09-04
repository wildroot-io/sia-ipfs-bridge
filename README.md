# sia-ipfs-bridge

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Node](https://img.shields.io/badge/node-%E2%89%A522.13-informational)](package.json)

Serve content-addressed data stored on **Sia** to the **IPFS** network — so a
CID whose only copy lives on Sia hosts resolves in kubo, in a browser, in
Helia, from a public gateway, from anything that speaks IPFS.

It is not a gateway in front of a block store. There is no block store: a pin
is **one CAR file stored as one Sia object**, and an index says where each
block sits inside it. Every block anyone ever asks for is a **range read**
into that archive.

Extracted from [pinthis.cloud](https://pinthis.cloud), which runs it on
mainnet.

## Documentation

| | |
|---|---|
| **[Architecture](docs/ARCHITECTURE.md)** | How it works and why: the storage model, the block index, the read path, how the network finds you, metering, failure modes, and the measurements — including a negative result that will save you a day. |
| **[HTTP API](docs/HTTP-API.md)** | Every endpoint, with status codes, and how to put it behind your own accounts. |
| **[Pinning Service API](docs/PINNING-API.md)** | The IPFS Pinning Service API, so `ipfs pin remote` and Pinata-style clients work unchanged. |
| **[Operations](docs/OPERATIONS.md)** | Deploying it: configuration reference, the Sia float, proxying, backups, monitoring, troubleshooting. |
| **[Contributing](CONTRIBUTING.md)** · **[Security](SECURITY.md)** | |

---

## The shape of it

```
        a client's own Sia account            anyone on IPFS
                    │                        (kubo, Helia, a gateway)
        shareObject │                                  │
                    ▼                        bitswap   │   HTTP
        ┌───────────────────────┐            ┌─────────┴─────────┐
        │  bridged              │            │                   │
        │                       │      ┌─────┴──────┐   ┌────────┴────────┐
        │  pins    cid → object │◄─────┤ kubo       │   │ /ipfs/<cid>     │
        │  blocks  block → off  │ HTTP │ sidecar    │   │ (trustless)     │
        │  cache   RAM + disk   │      │ NO blocks  │   │ /routing/v1     │
        └───────────┬───────────┘      └────────────┘   └─────────────────┘
                    │ range read
                    ▼
              Sia hosts (one CAR per pin)
```

### One CAR per pin, not one object per block

A pin is a single Sia object holding the whole CAR, plus a table:

```sql
pins     cid       → object_id, size          -- the archive on Sia
blocks   block_cid → cid, offset, length      -- a CARv2-style index
pinners  cid × owner                          -- who asked for it
```

Serving a block is `sdk.download(object, { offset, length })`. The reason not
to store a block per Sia object is throughput and overhead: measured on
mainnet, a whole-object read runs at **~27 MB/s** while reading one 256 KiB
block at a time gets **~1 MB/s** — quite apart from per-object metadata and
contract churn.

Blocks are matched **by multihash, not by CID**: `bafk…` (raw) and `bafy…`
(dag-pb) of the same bytes are the same block, and a bitswap peer asks by
multihash alone. `pins.locate()` expands every codec spelling of a hash.

### Indexing verifies, and warms

`indexPin()` walks the CAR exactly once. It requires the claimed root to be
among the archive's roots — a share URL for the wrong file is refused there
and the object released — and, since the bytes are streaming past anyway,
every block goes into the cache on the way. **A fresh pin's first visitor
never touches a Sia host.**

### The cold read: race a small read against a big one

A block that has gone cold is fetched two ways at once:

* its **own bytes** (256 KiB, ~0.22 s) — answers the request;
* the **16 MiB window** of the CAR it sits in — one read at ~27 MB/s, whose
  every block lands in the cache for everything that follows.

`Promise.any` takes whichever arrives first, and a reader approaching the end
of a window triggers the next fill ahead of itself. Measured on the box: a
cold 10 MiB file **0.44 s** to first byte, warm 0.03 s.

The cache is two tiers — RAM LRU for the recent, disk LRU for the popular.
A block earns a place on disk on its **second** ask, so one crawl of a cold
site cannot flush a hot one.

### Two doors onto the same blocks

1. **Delegated Routing V1** (IPIP-337): `GET /routing/v1/providers/<cid>`
   answers "this peer has it, over HTTP", and any client with HTTP retrieval
   (kubo 0.35+, verified-fetch, Helia) then reads hash-checked raw blocks from
   `/ipfs/<cid>?format=raw`.
2. **A kubo sidecar that holds no blocks** (`provider/`). A stock kubo with one
   Go datastore plugin mounted at `/blocks`: `Has` is a HEAD, `Get` is a GET on
   the gateway, and the key list for reproviding streams from `/api/blocks`. It
   runs as **the bridge's own peer identity**, so the routing record and the DHT
   records name the same peer. Everything it serves over bitswap goes through
   the same metering a browser hit does.

New pins are announced at pin time through kubo's `routing/provide` RPC.
Leaving that to kubo's 22-hour reprovide sweep is the difference between a
1.2 s and a 17 s cold first byte from a public gateway — it was measured.

### Metering is structural

Every byte that leaves is counted against the pin's **oldest** pinner —
whichever door it left by, and whether it came from RAM, disk or a host. Past
the plan's monthly allowance the edge slows; past 1.5× it answers 429. It
**never unpins**: data is never at risk from a bandwidth event.

The cache deliberately does not change what is metered. It protects the
operator's Sia float, not the customer's allowance.

There is a second layer this repo does not contain, and you will want it: a
**per-connect-key funding cap enforced in the indexer's funding loop**.
Upstream `indexd` tops every user's host balance up to a target with no
cumulative ceiling, so a client talking straight to hosts is unbillable and
unbounded — and funding is the only lever an indexer holds on that path.
`Indexd#capabilities()` reports whether your indexer has it.

---

## Running it

The whole stack — indexer, bridge, provider — is one compose file:

```sh
cp deploy/env.example deploy/.env
$EDITOR deploy/.env                    # ORIGIN, INDEXD_APP_URL, APP_ID, BRIDGE_TOKEN
docker compose -f deploy/docker-compose.yml up -d postgres indexd
docker compose -f deploy/docker-compose.yml run --rm bridged connect   # once
docker compose -f deploy/docker-compose.yml up -d
```

Or directly, if you already have an indexer:

```sh
npm install
export ORIGIN=https://example.com          # where this gateway answers
export INDEXD_APP_URL=https://indexer.example.com
export INDEXD_ADMIN_URL=http://indexd:9980/api
export INDEXD_ADMIN_PASSWORD=…
export APP_ID=$(openssl rand -hex 32)      # ONCE. Never change it: the app
                                           # key derives from it, so a new id
                                           # is a new account with no pins.
export BRIDGE_TOKEN=$(openssl rand -hex 32)

node bin/bridged.cjs connect               # one time: make the Sia session
node bin/bridged.cjs serve
```

Pin something:

```sh
curl -T ./index.html "https://example.com/api/upload?filename=index.html" \
     -H "Authorization: Bearer $BRIDGE_TOKEN"

# or the IPFS Pinning Service API, so existing tools work unchanged:
ipfs pin remote service add mine https://example.com "$BRIDGE_TOKEN"
ipfs pin remote add --service=mine --name=site bafy…
```

Then, from a machine that has never seen it — the only test that counts:

```sh
ipfs get <the cid>
```

Full configuration reference and the deployment checklist are in
[docs/OPERATIONS.md](docs/OPERATIONS.md).

### Three ways in

| | |
|---|---|
| **Share URL** | `POST /api/pins {cid, shareUrl}` — the client already stored the CAR on **its own** Sia account and shares the object; we pin that object under the service account so it outlives their device and plan. **No bytes move, nothing is paid for twice.** |
| **Pinning Service API** | `POST /pins {cid}` — queued; the worker fetches the CAR and uploads it. |
| **Upload** | `PUT /api/upload`, or a session under `/api/uploads` for folders — hashed into the DAG kubo would build (proven block for block against a real kubo in the tests), written as a CAR, then queued exactly like any other request. |

### Wiring it to your own accounts

The bridge does not know what a customer is. Two seams:

```js
new App({ cfg, db, ix,
  ownerOf: (req) => yourSessionLookup(req),        // request → owner id
  policy: policy.of(yourPlans, (id) => yourAccount(id))
})
```

A plan is four fields: `title`, `publicPins`, `storageBytes`,
`publicBytesPerMonth`. The default (`policy.single()`) is one owner behind one
bearer token with no ceilings. Nothing else in the bridge changes.

---

## Tests

```sh
npm test                # 16 tests
npm run lint            # standard
npm run test:provider   # go vet + go test
```

The CAR fixtures are **real**: a live `ipfs` binary builds a site in a
temporary repo, so the gateway is tested against exactly the bytes the network
produces. Tests needing it skip without `ipfs` on PATH rather than faking one —
"we agree with ourselves about what a DAG is" would prove nothing, so install
[kubo](https://docs.ipfs.tech/install/command-line/) before you trust a green
run.

Two of them drive a whole kubo daemon: one proves a stock client finds a CID
through delegated routing and fetches it over HTTP retrieval, the other that it
fetches over bitswap from the sidecar. They are slow, and they are the only
evidence any of this is universal rather than merely self-consistent.

---

## Things worth knowing before you build this

* **Peering with public gateways bought us nothing.** Adding the ~50 `rainbow/`
  nodes behind ipfs.io to `Peering.Peers` and raising the connection manager to
  250/500/60 s changed a cold first byte not at all (1.52/1.70/1.59 s). boxo's
  broadcast control only broadcasts wants to peers that **recently sent
  blocks**, so a bare connection is not the zero-delay path. From outside, the
  floor is ~1 s of provider-search delay plus ~0.46 s of lookup. The metric is
  won in clients you control.
* **Announce at pin time.** See above; it is the single largest win available.
* **`AutoConf` is a dependency with an owner.** kubo 0.37+ ships `Bootstrap`,
  `Routing.DelegatedRouters`, `Ipns.DelegatedPublishers` and `DNS.Resolvers` as
  the placeholder `"auto"` and expands them at every start from
  `conf.ipfs-mainnet.org`. Interplanetary Shipyard is winding that down —
  ipfs.io and dweb.link retire **2026-09-21**, the libp2p bootstrap nodes and
  `delegated-ipfs.dev` **2026-09-30**. `provider/entrypoint.sh` pins all four
  explicitly. Run at least one bootstrap node of your own.
* **`Routing.Type` must be pinned to `dht`.** The `server` profile does not set
  it, so it stays `auto` and AutoNAT decides. A node demoted to client mode
  answers no DHT queries and hands newcomers an empty routing table — and
  nothing looks wrong until a fresh client cannot bootstrap.
