# The IPFS Pinning Service API

`bridged` implements the [IPFS Pinning Service
API](https://ipfs.github.io/pinning-services-api-spec/), so existing tools
work against it unchanged.

## Endpoint and token

```
endpoint  https://<your-origin>
token     a bearer token your `ownerOf` accepts (out of the box: BRIDGE_TOKEN)
```

```sh
ipfs pin remote service add mine https://example.com "$BRIDGE_TOKEN"
ipfs pin remote add --service=mine --name=my-site bafybeic…
ipfs pin remote ls --service=mine
ipfs pin remote rm --service=mine --cid=bafybeic…
```

## What is implemented

| | |
|---|---|
| `GET /pins` | `cid`, `name`, `status`, `limit`, `before` filters; `{count, results}` |
| `POST /pins` | `{cid, name, origins, meta}` → 202 `PinStatus`, queued |
| `GET /pins/<requestid>` | one `PinStatus` |
| `POST /pins/<requestid>` | replace — **the old CID is unpinned in the same call**, so a replace does not leave the previous object stored and billed |
| `DELETE /pins/<requestid>` | unpin; the last pinner out deletes the Sia object |

`delegates` is always empty: there is nothing for a client to upload to. The
worker fetches the DAG itself, from a trustless gateway (`CAR_GATEWAYS`) or —
better, and the only option once public gateways go away — over bitswap
through the sidecar.

## Statuses

`queued → pinning → pinned | failed`, as specified. A failure goes back to the
queue with a growing backoff and is retried up to five times; a request that
is over the plan's space is **failed immediately**, because over-quota is not
a transport hiccup to retry.

## The status codes that are not the spec's

* `402` — the owner's plan does not include public pins, or the pin would
  exceed its space.
* `503` — storage is not connected right now (the indexer or the Sia session
  is down). The request is not lost.
