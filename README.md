# sia-ipfs-bridge

Serves content stored on [Sia](https://sia.tech) to the IPFS network, so a CID
whose only copy is on Sia hosts can be fetched by kubo, Helia or any trustless
gateway client. Each pin is one CAR file stored as one Sia object, with an
index of where each block sits inside it, and every block is served as a range
read of that object.

It was extracted from [pinthis.cloud](https://pinthis.cloud) in September 2026.

Status: pre-1.0. Blocks are not yet checked against their CIDs, and a response
holds every block it serves in memory until it finishes. Accept pins only from
people you trust, and size the host's memory for the largest file you serve.

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

- **Storage.** One CAR per pin. A SQLite table maps each block's multihash to
  its pin, offset and length.
- **Reads.** A RAM cache and a disk cache sit in front of Sia. On a miss, a
  read of the block races a read of the 16 MiB window around it, which fills
  the cache for the blocks that follow.
- **Discovery.** `/routing/v1/providers/<cid>` (Delegated Routing V1) points
  HTTP-retrieval clients at the trustless gateway. The kubo sidecar in
  `provider/` stores no blocks: its datastore plugin reads them from the
  bridge, serves them over bitswap and announces new pins to the DHT.
- **Metering.** Bytes served count against the pin's oldest pinner. Past the
  plan's monthly allowance responses are delayed, and past 1.5 times it they
  get 429. Nothing is unpinned.

## Run

Requires Node 22.13 or later and a Sia indexer (`indexd`) the bridge can
connect to as an app.

```sh
npm install
export ORIGIN=https://example.com             # public origin of this gateway
export INDEXD_APP_URL=https://indexer.example.com
export INDEXD_ADMIN_URL=http://127.0.0.1:9980/api
export INDEXD_ADMIN_PASSWORD=...
export APP_ID=$(openssl rand -hex 32)         # generate once and keep: the app key derives from it
export BRIDGE_TOKEN=$(openssl rand -hex 32)

node bin/bridged.cjs connect                  # once: create the Sia session
node bin/bridged.cjs serve
```

`deploy/` holds a compose file for the indexer, bridge and sidecar. Settings,
the Sia wallet, proxying and monitoring are covered in
[docs/OPERATIONS.md](docs/OPERATIONS.md).

## Pin

```sh
curl -T ./index.html -H "Authorization: Bearer $BRIDGE_TOKEN" \
     "https://example.com/api/upload?filename=index.html"

# or through the IPFS Pinning Service API
ipfs pin remote service add mine https://example.com "$BRIDGE_TOKEN"
ipfs pin remote add --service=mine --name=site <cid>
```

A client that has already stored a CAR on its own Sia account can pin it
without re-uploading, with `POST /api/pins {cid, shareUrl}`. By default the
bridge has one owner behind one bearer token; to serve your own accounts, pass
`ownerOf` and `policy` to `App`. Both are described in
[docs/HTTP-API.md](docs/HTTP-API.md).

## Test

```sh
npm test                # node --test
npm run lint            # standard
npm run test:provider   # go vet and go test for the sidecar
```

The CAR fixtures are built by a real `ipfs` (kubo) binary. Tests that need it
are skipped when it is not installed.

## Documentation

- [Architecture](docs/ARCHITECTURE.md): storage model, read path, discovery,
  metering, failure modes and measurements
- [HTTP API](docs/HTTP-API.md)
- [Pinning Service API](docs/PINNING-API.md)
- [Operations](docs/OPERATIONS.md)
- [Security](SECURITY.md) and [Contributing](CONTRIBUTING.md)

## License

MIT. See [LICENSE](LICENSE).
