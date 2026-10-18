# Operations

Running this for real: what to set, what to fund, what to watch, and what goes
wrong.

- [Prerequisites](#prerequisites)
- [First run](#first-run)
- [Configuration reference](#configuration-reference)
- [The Sia wallet and the float](#the-sia-wallet-and-the-float)
- [The reverse proxy](#the-reverse-proxy)
- [Backups](#backups)
- [Monitoring](#monitoring)
- [Upgrades](#upgrades)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

| | |
|---|---|
| Node | **≥ 22.13** — `node:sqlite` is used, so there is no native module to build |
| Go | 1.26+, only to build the provider sidecar |
| An indexer | A Sia Storage indexer (`indexd`) this service can be an app on |
| A public hostname | With TLS. It goes into every provider record |
| An open swarm port | TCP+UDP, default 4001, reachable from the internet |
| Sia coins | A float in the indexer's wallet — see below |

**Before you take money from anyone**, read
[Metering and quotas](ARCHITECTURE.md#metering-and-quotas). Upstream `indexd`
has no per-user funding ceiling, so a user holding their own connect key on
your indexer can draw on your float without limit and without passing through
this service.

## First run

```sh
cp deploy/env.example deploy/.env
$EDITOR deploy/.env            # ORIGIN, INDEXD_APP_URL, APP_ID, BRIDGE_TOKEN
docker compose -f deploy/docker-compose.yml up -d postgres indexd
# wait for the indexer to sync, then:
docker compose -f deploy/docker-compose.yml run --rm bridged connect
docker compose -f deploy/docker-compose.yml up -d
```

`bridged connect` runs once per box. It creates the service's quota and connect
key on the indexer, approves its own app connection, and writes the session to
`<state>/sia.json` at mode `0600`. It refuses to overwrite an existing session
file — remove it deliberately if you really mean to reconnect.

Two files appear in the state directory on first start, and both matter:

| | |
|---|---|
| `sia.json` | the app key. **Lose it and you lose access to every pinned object.** |
| `kubo-identity.json` | the peer identity, shared with the sidecar so the routing record and the DHT records name one peer |

Verify:

```sh
curl -s localhost:8080/api/health                       # {"ok":true,"storage":true,…}
curl -T ./hello.txt -H "Authorization: Bearer $BRIDGE_TOKEN" \
     "https://example.com/api/upload?filename=hello.txt"
curl -s "https://example.com/routing/v1/providers/<the cid>" | jq .
# then, from a machine that has never seen it:
ipfs get <the cid>
```

That last command is the only test that matters. Everything else can pass while
the thing is invisible to the network.

## Configuration reference

All configuration is environment variables, so the whole deployment is one
compose file.

### Required

| | |
|---|---|
| `ORIGIN` | Public `https://` origin. Goes into every provider record. |
| `INDEXD_APP_URL` | The indexer's public application API. |
| `APP_ID` | 32 bytes of hex, `openssl rand -hex 32`, **generated once and never changed** — the app key derives from it, so a new id is a new account holding none of your pins. Not a secret. |

### Indexer

| | | |
|---|---|---|
| `INDEXD_ADMIN_URL` | `http://indexd:9980/api` | Admin API. Keep it on loopback or a private network — it is basic auth. |
| `INDEXD_ADMIN_PASSWORD` | — | |

### Service

| | | |
|---|---|---|
| `PORT` | `8080` | |
| `BRIDGE_STATE` | `/var/lib/bridge` | Database, Sia session, peer keys, disk cache. |
| `BRIDGE_DB` | `<state>/bridge.db` | |
| `SIA_SESSION` | `<state>/sia.json` | |
| `APP_NAME`, `APP_DESCRIPTION` | | How the app names itself at approval. |
| `PROVIDER_URL` | `http://provider:5001` | The sidecar's kubo RPC. Never publish this port. |
| `CAR_GATEWAYS` | `https://trustless-gateway.link` | Comma-separated. Where a not-yet-held CID is fetched from for a Pinning Service request. A convenience with an expiry date; see [ARCHITECTURE.md](ARCHITECTURE.md#shipyards-public-infrastructure). |

### Cache

| | | |
|---|---|---|
| `CACHE_MEMORY_BYTES` | `536870912` (512 MiB) | RAM tier. |
| `CACHE_BYTES` | `10737418240` (10 GiB) | Disk tier, under `<state>/cache`. |

Both are worth raising if you have the room: every cache hit is a Sia read you
did not pay for. Neither affects what customers are billed.

### Default authentication

| | | |
|---|---|---|
| `BRIDGE_TOKEN` | — | One bearer token. Empty means every write is refused. |
| `BRIDGE_OWNER` | `default` | The owner id that token maps to. |

Single-tenant only. For real accounts, replace `ownerOf` and `policy` — see
[HTTP-API.md](HTTP-API.md#authentication).

### Provider sidecar

| | | |
|---|---|---|
| `BRIDGE_URL` | `http://bridged:8080` | Where the datastore plugin reads blocks. |
| `BRIDGE_IDENTITY` | `/var/lib/bridge/kubo-identity.json` | Mount this **one file**, read-only. Not the directory: it also holds your database and Sia session, and this container is the only thing you run that talks libp2p to the whole internet. |
| `PROVIDER_PORT` | `4001` | Published, TCP and UDP. |

## The Sia wallet and the float

The indexer's wallet is hot on the box by design and cannot be rotated. Treat
it as a float, not a treasury: hold a few weeks of contract spend, top it up
from a reserve you keep somewhere else, and the loss is bounded by what is
there.

Two things caught us out:

- **Contract formation spends confirmed outputs only.** A wallet with one big
  output forms one contract per block. Split the balance into many outputs
  (chained self-sends) before expecting the contract manager to reach its
  target.
- **A dry wallet loses data silently, over weeks.** Upstream has no
  low-balance alert. Build one; see below.

## The reverse proxy

Route deliberately. These paths are public:

```
/ipfs/          the gateway
/routing/v1/    delegated routing
/pins           the Pinning Service API
/api/pins /api/upload /api/uploads /api/health
```

**`/api/blocks` must never be reachable from outside.** It is the sidecar's key
list. The service refuses any request carrying `X-Forwarded-For` as a
second line of defence, but the first line is your proxy config.

Serve untrusted content on its own hostname where you can. `/ipfs/<cid>` HTML
is sandboxed by CSP so a stranger's page cannot touch your origin's storage —
but same-origin is a weaker boundary than a separate origin, and a page that
needs to run scripts properly wants a hostname of its own.

## Backups

| | |
|---|---|
| `sia.json` | **The one irreplaceable file.** Losing it loses access to every pinned object. Back it up off the box, encrypted. |
| `kubo-identity.json` | Losing it changes your peer id; the DHT points at a peer nobody runs until everything is re-announced. |
| `bridge.db` | Rebuildable in principle (the objects are on Sia) but only if you still know which object holds which CID — which is *in* this database. Back it up. |
| the disk cache | Disposable. |

## Monitoring

At minimum:

- `GET /api/health` — `storage: false` means the Sia session is down; pins
  still serve from cache, uncached reads answer 503.
- **Wallet balance and days of runway.** Nothing else warns you.
- **Burn rate per connect key** (`GET /stats/funding`, on an indexer that
  records it). An abuser shows up as a spike.
- **Egress per owner** — the `egress` table, by day.
- **`pins WHERE announced_at IS NULL`** — a backlog means the sidecar is
  unreachable and new pins are not on the DHT.
- **Pin requests in `failed`** — with `info`, which carries the reason.

## Upgrades

The schema is created idempotently at open; there is no migration runner. Adding
a column means adding it to `SCHEMA` and handling the upgrade path explicitly.

The provider sidecar can be restarted freely: it holds no blocks, and
`pins.announce` re-announces anything it has not confirmed. Keep
`kubo-identity.json` across restarts or your peer id changes.

## Troubleshooting

**A CID resolves on your own gateway but nowhere else.**
Check `pins.announced_at` — null means it never reached the DHT. Check the
sidecar is up and dialable from outside (`ipfs id`, then dial it from
elsewhere). A node behind NAT with no reachable address provides nothing.

**A stranger's kubo finds the provider but the fetch is slow or truncated.**
Usually the sidecar answering `Has` under load. `Has` is answered from the
index without touching Sia, but there is one HTTP round trip per block; watch
for refused connections during restarts.

**Everything answers 503.**
The Sia session is not open. The daemon retries every minute and logs each
attempt; check the indexer is up and that `sia.json` still matches its app URL.
A moved indexer is followed automatically and the session file rewritten.

**`bridged connect` says the file exists.**
It refuses to overwrite a session. That is deliberate — reconnecting makes a
new key, and your pins belong to the old one.

**A pin request keeps failing.**
`GET /pins/<id>` carries `info`. Over-quota and plan refusals are final by
design. Transport failures retry five times with a growing backoff.

**Cold reads are slower than the numbers in the docs.**
Confirm the window read is happening: one cold block should cost about one
direct read plus one window read, not one read per block. `CACHE_BYTES` too
small, or a working set much larger than it, will show up here first.
