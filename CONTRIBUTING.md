# Contributing

Bug reports and patches are welcome. This is a small, deliberately boring
codebase and the bar is that it stays that way.

## Getting set up

```sh
npm install
npm test                # 16 tests
npm run lint            # standard
npm run test:provider   # go vet + go test
```

Install [kubo](https://docs.ipfs.tech/install/command-line/) before running the
suite. Tests that need it **skip** without it rather than faking one, so a
green run on a machine with no `ipfs` has proved considerably less than it
looks.

## What the tests are for

The CAR fixtures are built by a real kubo, on purpose. "We agree with ourselves
about what a DAG is" proves nothing — the whole point of this software is that
strangers' clients can read what we wrote. If you add a feature that touches
the DAG, the CAR format, or block addressing, test it against bytes kubo
produced.

Two tests drive an entire kubo daemon (HTTP retrieval, and the sidecar over
bitswap). They are slow. They are also the only evidence that any of this is
universal rather than merely self-consistent. Do not delete them.

## Style

- `standard` for JavaScript, `gofmt` for Go. `npm run lint` is not advisory.
- CommonJS, Node built-ins, and as few dependencies as the job allows. Every
  dependency is something that can be compromised and shipped to a box holding
  a hot wallet.
- Comments explain **why**, and especially why not the obvious thing. If you
  removed a comment and a future reader would re-introduce the bug it warns
  about, keep the comment.
- Errors that a user will read should be one plain sentence with no jargon.

## Pull requests

- One change per PR, with a test that fails without it.
- Say what you measured if you claim something is faster. This repo's README
  contains a negative result that cost a day precisely because it was measured
  rather than assumed.
- If you change behaviour that is metered, billed, or deletes data, say so in
  the PR description in those words.

## Security

Do not open a public issue for a vulnerability. See [SECURITY.md](SECURITY.md).
