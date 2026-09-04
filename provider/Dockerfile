# The provider: stock kubo with the siabridge datastore plugin preloaded
# (provider/), built from source because a preloaded plugin is part of the
# binary. Two stages so the runtime image carries the one binary and nothing
# of Go.
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY provider/go.mod provider/go.sum ./
RUN go mod download
COPY provider ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /ipfs ./cmd/ipfs

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --uid 1000 --user-group --home-dir /data/ipfs --create-home --shell /usr/sbin/nologin ipfs
COPY --from=build /ipfs /usr/local/bin/ipfs
COPY provider/entrypoint.sh /usr/local/bin/entrypoint.sh
# uid 1000: the same as bridged (`node`), which owns /var/lib/bridge and
# the 0600 kubo-identity.json in it.
USER ipfs
ENV IPFS_PATH=/data/ipfs
VOLUME /data/ipfs
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
