# Architecture

How a byte gets from a Sia host to somebody's IPFS client, and why each piece
is shaped the way it is. The [README](../README.md) is the summary; this is
the detail you need before changing anything.

## Contents

- [The problem](#the-problem)
- [Storage model: one CAR per pin](#storage-model-one-car-per-pin)
- [The block index](#the-block-index)
- [Indexing: verify once, warm on the way past](#indexing-verify-once-warm-on-the-way-past)
- [The read path](#the-read-path)
- [Reaching the network](#reaching-the-network)
- [Metering and quotas](#metering-and-quotas)
- [Data model](#data-model)
- [Failure modes](#failure-modes)
- [Measurements](#measurements)

---

## The problem

Sia gives you cheap, durable, verifiable storage bought from a market of
hosts. IPFS gives you a naming and retrieval network keyed by the hash of
content. They do not meet: an object on Sia has an object id and a decryption
key, and nothing about it is addressable as a CID or fetchable by a stranger's
kubo.

Bridging them badly is easy and expensive. The obvious design — store each
IPFS block as its own Sia object — collapses under per-object overhead, slab
and contract churn, and terrible small-read throughput. What follows is the
design that survived contact with a production workload.

## Storage model: one CAR per pin

**A pin is one CAR file stored as one Sia object.**

A [CAR](https://ipld.io/specs/transport/car/carv1/) is the canonical way to
serialise a DAG: a header naming the roots, then a sequence of
`varint(length) || CID || block bytes` sections. It is what `ipfs dag export`
produces and what a trustless gateway serves. Storing that archive whole means
one object, one set of slabs, one set of contracts, per pin.

The cost of this choice is that a single block is now *inside* something,
rather than being its own addressable unit. That is what the block index
solves, and the trade is heavily in its favour:

| | |
|---|---|
| Whole-object sequential read | **~27 MB/s** |
| One 256 KiB block at a time | **~1 MB/s** |
| Single 256 KiB block, cold | **~0.22 s** |

*(measured in-container against mainnet hosts, 2026-09)*

Reading a big file, or a site with many small assets, is therefore an order of
magnitude faster if you can arrange to read *ranges of the archive* rather than
blocks one by one. The read path is built around exactly that.

## The block index

While the CAR is walked, every block's position is recorded:

```sql
blocks(block_cid, cid, offset, length)   -- block → which pin, and where in it
```

This is a CARv2 index by another name, kept in SQLite instead of appended to
the file, so it can be queried without touching Sia. Serving a block is then:

```js
const loc = pins.locate(db, cid)                       // one indexed lookup
sia.readRange(sdk, object, loc.offset, loc.length)     // one Sia range read
```

### Blocks are matched by multihash, not by CID

`bafkreib…` (codec `raw`) and `bafybeib…` (codec `dag-pb`) can wrap the *same
multihash*, and they are the same bytes. Worse, bitswap asks for blocks by
multihash alone — the codec is not on the wire. So `pins.locate()` expands the
requested CID into every codec spelling of its hash before querying:

```js
const keys = [c.key(), ...Object.values(CODEC)
  .filter((code) => code !== c.codec)
  .map((code) => new CID(1, code, c.multihash).key())]
```

Miss this and a kubo will ask for a block you demonstrably hold and be told
404 — intermittently, depending on how the DAG was built.

## Indexing: verify once, warm on the way past

`pins.indexPin()` streams the archive exactly once and does three things:

1. **Verifies the claim.** The CID being registered must be among the CAR's
   own roots. A share URL that points at a different file is refused here, and
   the object is released rather than left pinned and paid for.
2. **Records every block's offset and length.** Inside one transaction, so an
   interrupted index leaves no half-built pin — `pins.indexed` stays 0 and the
   gateway will not serve from it.
3. **Warms the cache.** The bytes are already streaming past, so each block is
   handed to `warm(cid, data)`, which puts it in RAM *and* on disk.

That third point is worth stating plainly: **a fresh pin's first visitor never
touches a Sia host.** The most latency-sensitive moment in the product — you
just published something and you are sending someone the link — costs nothing.

## The read path

`Gateway#block(cid)`:

```
                    ┌─────────────┐
   block(cid) ──────┤ RAM  cache  │── hit ──► serve (metered)
                    └──────┬──────┘
                           │ miss
                    ┌──────▼──────┐
                    │ disk cache  │── hit ──► serve (metered), promote in RAM
                    └──────┬──────┘
                           │ miss
                    ┌──────▼───────────────────────────────┐
                    │  pins.locate → offset, length        │
                    │                                      │
                    │   ┌──────────────┐  ┌─────────────┐  │
                    │   │ direct read  │  │ window fill │  │
                    │   │ ~256 KiB     │  │ 16 MiB      │  │
                    │   │ ~0.22 s      │  │ ~27 MB/s    │  │
                    │   └──────┬───────┘  └──────┬──────┘  │
                    │          └── Promise.any ──┘         │
                    └──────────────────┬───────────────────┘
                                       ▼
                             serve, and the window's
                             other blocks are now cached
```

**The race.** The direct read answers *this* request as fast as a single block
can be fetched. The window read — the 16 MiB-aligned slice of the archive the
block falls in, extended to the end of the last block starting inside it —
takes longer to arrive but lands every neighbouring block in the cache. Since a
client that wants one block of a file almost always wants the next one too, the
window turns a sequence of ~1 MB/s block fetches into one ~27 MB/s read.

Concurrent misses inside one window share a single in-flight read
(`Gateway#fills`), so a hundred simultaneous bitswap wants for one file cause
one Sia read, not a hundred.

**Read-ahead.** `Gateway#ahead()` fires when a served block sits in the last
quarter of its window: the next window is fetched before anyone asks for it, so
a large sequential read never stalls at a window boundary.

### The cache is two tiers, and the second one is earned

`lib/cache.cjs`:

- **RAM** — an LRU under a byte budget (default 512 MiB). Holds the recent.
- **Disk** — an LRU under its own budget (default 10 GiB), backed by
  `cache_blocks` rows and files at `<dir>/<aa>/<key>`. Holds the *popular*.

A block is written to disk on its **second** request, not its first. One
crawler walking a large cold site is not an audience, and letting it evict a
genuinely hot site would be a self-inflicted outage. Disk state survives
restarts; a file the table does not know about is swept at startup, and a row
whose file has vanished is forgotten on read.

Neither tier is a store. Every block in the cache is a copy of a pinned CAR on
Sia, and the gateway checks the pin exists before serving from it.

## Reaching the network

Holding the bytes is not enough — the network has to be able to find them.
There are two answers, and you want both, because they reach different clients.

### 1. Delegated Routing V1 (IPIP-337)

`GET /routing/v1/providers/<cid>` returns a provider record naming this
service, with protocol `transport-ipfs-gateway-http`. Any client that supports
HTTP retrieval — kubo 0.35+, verified-fetch, Helia — will then read
hash-checked raw blocks from `/ipfs/<cid>?format=raw`.

This is cheap, exact, and instant: no DHT walk, no announcement lag. Its limit
is that the client has to be *configured* to ask you (`Routing.DelegatedRouters`),
so it serves your own clients, not strangers.

### 2. A kubo sidecar that holds no blocks

`provider/` is a stock kubo with one Go plugin. The plugin registers a
datastore mounted at `/blocks` that stores nothing:

| kubo operation | what actually happens |
|---|---|
| `Has(key)` | `HEAD /ipfs/<cid>?format=raw` — answered from the SQLite index, no Sia access |
| `Get(key)` | `GET /ipfs/<cid>?format=raw`, hash-checked by kubo on receipt |
| `Query` (the key list) | streams `GET /api/blocks` |
| writes | a local LevelDB overlay beside the repo; never sent to the bridge |

The overlay exists because kubo writes a handful of blocks of its own — the
empty MFS root on first start, anything an operator `ipfs add`s in the
container — and those are nobody's pin.

The sidecar runs as **the bridge's own peer identity** (`kubo-identity.json`,
written by `lib/routing.cjs` on first start), so the delegated-routing record
and the DHT records name one peer. It announces with `Provide.Strategy: all`
and speaks bitswap on port 4001. To a stranger's kubo it is an ordinary IPFS
node that happens to have infinite storage and no disk.

Because every `Get` is an HTTP request to the gateway, bitswap traffic goes
through the same metering as a browser hit. There is no unmetered door.

### Announcing at pin time

kubo reprovides its key list on a sweep — every 22 hours by default. Leaving a
new pin to that sweep means it is unfindable by anyone who is not already your
peer for up to a day. `pins.announce()` calls kubo's `routing/provide` RPC as
soon as a pin is indexed, records `pins.announced_at`, and retries on every
tick until the sidecar answers, so a sidecar restart loses nothing.

Only the **root** is announced. A fetcher that has the root's provider asks
that same peer for the children over bitswap; announcing every block would
multiply DHT traffic for no gain.

### Staying close to gateways

`lib/provider.cjs`'s `keepGatewaysClose()` runs every five minutes: it lists
connected peers, picks out the ones running gateway software by agent string
(`rainbow/…`, `…/cloudflare`), and adds them to kubo's `Peering.Peers` so the
connection manager stops trimming them. Peers not seen for a day are dropped.

Read the honest assessment of what this bought us in
[Measurements](#measurements) before you assume it is load-bearing.

## Metering and quotas

Three layers, and they defend different things.

**1. Space, enforced before the bytes are bought.** When the worker fetches a
CAR for a pin request, the source stream is wrapped in `capped(stream, room)`
where `room` is what the owner's plan has left. It fails the moment the limit
is passed. Previously the whole archive was uploaded to Sia and *then* checked,
which meant a 5 GB account could make the service pull and pay for tens of GB
per request, five times over via retries. Over-quota is marked final, never
retried — it is not a transport hiccup.

**2. Egress, metered at the edge.** Every byte served is counted into
`egress(day, account_id, cid, bytes)` against the pin's **oldest** pinner — the
owner who caused the bytes to exist. `Gateway#standing()` returns:

| | |
|---|---|
| `ok` | under the plan's monthly allowance |
| `slow` | over it — a delay is inserted before each response |
| `refuse` | over 1.5× — 429 with `Retry-After` |

It never unpins. A bandwidth event must not put data at risk; the free ride
ends, the content stays. One owner's site gets slow and nobody else notices.

The cache does not change what is metered. A block served from RAM costs the
owner exactly what a block read from a host costs them. The cache protects the
*operator's* Sia float, not the customer's allowance.

**3. Funding, capped in the indexer.** This layer is not in this repository and
you should not deploy without it.

Upstream `indexd` tops every user's host balance up to a target every five
minutes with no cumulative ceiling. A client that talks straight to hosts
never touches your gateway, so layers 1 and 2 cannot see it — and funding is
the only lever an indexer holds on that path. The fix is a per-connect-key
`fundingCap` enforced in the funding loop: past it, that user's host balances
drain, their reads and writes stall with `ErrContractInsufficientFunds`, and
nobody else is affected.

`Indexd#capabilities()` reports whether your indexer exposes the fields this
requires (`fundingCap`, `fundedPeriod`, `fundingCapped`, `fundingPeriodStart`,
`GET /stats/funding`). Check it at boot and refuse to run without them.

## Data model

```sql
pins      cid PRIMARY KEY, object_id, size, car_size, indexed, announced_at, created_at
pinners   (cid, account_id) PRIMARY KEY, name, created_at
blocks    (block_cid, cid) PRIMARY KEY, offset, length
pin_requests  id, account_id, cid, name, origins, meta, status, info,
              attempts, upload_id, created_at, updated_at
uploads   id, account_id, name, folder, bytes, files, cid, car_size,
          created_at, expires_at, finished_at
egress    (day, account_id, cid) PRIMARY KEY, bytes
cache_blocks  key PRIMARY KEY, length, last_hit
```

Two rules do a lot of work:

- **A pin lives while any pinner remains.** Pinning a CID somebody else already
  pinned inserts one row and uploads nothing — deduplication by construction.
  The last `unregister` deletes the object.
- **The oldest pinner owns it.** Usage and metering both resolve ownership with
  the same SQL (`pins.publicUsage`), so quota enforcement and usage reporting
  cannot disagree about the same account.

`node:sqlite` in WAL mode — no native dependency to build on the box. Every
write is a single statement or an explicit transaction, so a crash mid-request
leaves a consistent file.

## Failure modes

Things that will happen in production, and what the code does about them.

| | |
|---|---|
| **The indexer is down at boot** | `bridged` starts anyway and retries the Sia session in the background every minute. The gateway serves cached blocks throughout; uncached reads answer 503. |
| **The sidecar is down** | Announcements are skipped, `announced_at` stays null, and every tick retries. Nothing is lost; new pins are simply not on the DHT yet. Delegated routing keeps working. |
| **The index is interrupted** | The transaction rolls back, the pin is removed and the object released. There is no partial pin: `indexed = 0` is never served. |
| **A share URL names the wrong root** | Refused during indexing, object released. |
| **Two pins name the same Sia object** | `remove()` only deletes the object when no other pin row points at it. Deleting unconditionally destroys the other pin's bytes — this was a real bug. |
| **A pin request fails** | Back to the queue with a growing backoff (`RETRY_MS × attempts`), five attempts, then `failed` with the reason. Over-quota and plan refusals are final immediately. |
| **A replace** | Unpins the CID it replaces in the same call. Not doing so leaves the old object stored and billed forever — a hole that only shows up on an invoice. |
| **An owner blows through their egress allowance** | Slow, then 429. Never unpinned, never deleted. |

## Measurements

Mainnet, 2026-09, 1 MiB and 100 MiB objects.

| | |
|---|---|
| Upload accepted (1 / 10 / 100 MiB) | 0.4 / 0.8 / 4.7 s |
| Stored on Sia and indexed | 9–15 s for 1 MiB |
| On-box gateway, 10 MiB, cold | **0.44 s** to first byte |
| On-box gateway, warm | 0.03 s |
| Public gateway, already-stored object, cold | **1.2–1.6 s** |
| Public gateway, from the moment of storing | ~2.3 s |
| Raw Sia, whole object, in-container | 27 MB/s |
| Raw Sia, one 256 KiB block | 0.22 s (~1 MB/s sustained) |

Before announcing at pin time, that same public-gateway number was **17 s and
truncated**, because the CID had only reached the DHT via kubo's 22-hour
reprovide sweep. Announcement, not indexer speed, was the bottleneck.

### A negative result worth your time

Peering the provider with the ~50 `rainbow/` nodes behind ipfs.io, and raising
kubo's connection manager from 32/96/20 s to 250/500/60 s so they were never
trimmed, produced **no improvement at all**: 1.52 / 1.70 / 1.59 s cold, against
1.2–1.6 s before.

The reason is boxo's broadcast control, on by default in recent kubo and
rainbow: a node only broadcasts its wants to peers that have *recently sent it
blocks*. A merely-connected peer is not on the fast path. From outside, the
floor is roughly 1 s of provider-search delay plus ~0.46 s of DHT lookup.

The conclusion we drew: cold first-byte latency to third-party gateways is
mostly not yours to win. It is won in clients you control, by pointing them at
your delegated router.

### The ground under all of this is moving

Interplanetary Shipyard is winding down IPFS operations. ipfs.io and dweb.link
retire **2026-09-21**; the libp2p bootstrap nodes, `delegated-ipfs.dev` and the
AutoConf document at `conf.ipfs-mainnet.org` go **2026-09-30**.

Three consequences are baked into this repo:

1. `provider/entrypoint.sh` pins `Bootstrap`, `Routing.DelegatedRouters`,
   `Ipns.DelegatedPublishers` and `DNS.Resolvers` explicitly and sets
   `AutoConf.Enabled false`. Left as `"auto"`, a fresh node loses its route
   into the DHT on a date you do not control. Only a literal
   `Bootstrap: ["auto"]` fails loudly; the others degrade to a log line nobody
   reads.
2. `Routing.Type` is pinned to `dht`. The `server` profile does not set it, so
   it stays `auto` and AutoNAT decides — and a node silently demoted to client
   mode answers no DHT queries, handing newcomers an empty routing table.
3. `CAR_GATEWAYS` is a convenience with an expiry date. Fetching a
   not-yet-held CID over bitswap through the sidecar (`ipfs dag export`) is the
   durable design, and was always the better one.

**Run at least one bootstrap node of your own**, on hardware you control, in
DHT server mode.
