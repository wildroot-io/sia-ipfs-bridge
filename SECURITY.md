# Security

## Reporting a vulnerability

Please report privately, not in a public issue: open a [security
advisory](https://github.com/keptlive/sia-ipfs-bridge/security/advisories/new)
on this repository.

Include what you did, what happened, and what you expected. If it involves data
loss or somebody else's bytes, say so first — that gets read first.

## What this software is responsible for

Running a bridge means running a service that spends money on behalf of
strangers and serves their bytes to the internet. The parts most worth your
attention when reviewing:

**Metering and funding.** Public serving is metered per owner and enforced at
the edge, but that only covers traffic through this service. A client talking
straight to Sia hosts is invisible here, and the only lever is a per-connect-key
funding cap in the indexer. Deploying without one means a single heavy reader
is unbillable and unbounded.

**Content served on your origin.** `/ipfs/<cid>` serves arbitrary content
uploaded by arbitrary people. It is sandboxed with a CSP so a stranger's page
cannot reach your origin's storage or credentials, but same-origin remains a
weaker boundary than a separate origin. Give untrusted content its own hostname
where you can.

**`/api/blocks`.** The sidecar's key list. It must not be reachable from
outside; the service refuses requests carrying `X-Forwarded-For` as a second
line of defence, but your proxy config is the first.

**The state directory.** It holds `sia.json` — the app key for every pinned
object — and the database. The provider container is the only component that
speaks libp2p to the whole internet; mount it `kubo-identity.json` alone,
read-only, and never the directory.

**Deletion paths.** A pin's Sia object is deleted when the last pinner leaves.
Two pins can name the same object, so deletion checks for other references
first. Changes to `pins.remove()` deserve a careful review: getting it wrong
destroys data belonging to someone who did nothing.

## Supported versions

This is pre-1.0. Fixes land on `main`.
