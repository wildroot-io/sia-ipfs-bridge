#!/bin/sh
# The provider container's start: init the repo once with the siabridge profile
# (identity from bridged, blocks from bridged), then the settings that must
# hold every start, then the daemon. Everything here is idempotent.
set -eu
: "${IPFS_PATH:=/data/ipfs}"
: "${PROVIDER_PORT:=4001}"
: "${BRIDGE_IDENTITY:=/var/lib/bridge/kubo-identity.json}"
: "${BRIDGE_URL:=http://bridged:8080}"
export IPFS_PATH BRIDGE_IDENTITY BRIDGE_URL   # the plugin reads them at init and at every start

[ -r "${BRIDGE_IDENTITY}" ] || { echo "provider: ${BRIDGE_IDENTITY} is not readable — has bridged started once?" >&2; exit 1; }

if [ ! -f "${IPFS_PATH}/config" ]; then
  echo "provider: ipfs init --profile=server,siabridge" >&2
  ipfs init --profile=server,siabridge --empty-repo >&2
fi

# The repo's identity must be the one bridged answers /routing/v1 with; if
# they differ the DHT would point at a peer nobody runs. Never silently fix
# it: the operator restores kubo-identity.json or wipes the repo volume.
want="$(sed -n 's/.*"PeerID"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "${BRIDGE_IDENTITY}" | head -n1)"
have="$(ipfs config Identity.PeerID)"
[ "${want}" = "${have}" ] || { echo "provider: repo identity ${have} is not bridged's ${want}" >&2; exit 1; }

# The swarm port is the published port (the container announces what it
# listens on); no gateway here (bridged is the gateway); the RPC API is on
# the container network for bridged alone (it announces fresh pins through
# it — lib/provider.cjs) and is never published.
ipfs config --json Addresses.Swarm "[\"/ip4/0.0.0.0/tcp/${PROVIDER_PORT}\",\"/ip6/::/tcp/${PROVIDER_PORT}\",\"/ip4/0.0.0.0/udp/${PROVIDER_PORT}/quic-v1\",\"/ip6/::/udp/${PROVIDER_PORT}/quic-v1\"]"
ipfs config --json Addresses.Gateway '[]'
ipfs config --json Addresses.API '["/ip4/0.0.0.0/tcp/5001"]'

# A provider is measured by how fast a gateway that already knows it gets a
# block, and the gateways are the long-lived
# connections kubo's default manager (32/96, 20 s) trims first under the DHT's
# churn. Room for them all; bridged also peers with each one it sees
# (lib/provider.cjs), so they are never the ones trimmed.
ipfs config --json Swarm.ConnMgr '{"Type":"basic","LowWater":250,"HighWater":500,"GracePeriod":"60s"}'

# AUTOCONF OFF, AND EVERY VALUE IT WOULD HAVE FILLED WRITTEN EXPLICITLY.
# kubo 0.37+ ships these four as the placeholder "auto" and expands them at
# every start by downloading https://conf.ipfs-mainnet.org/autoconf.json.
# That document is Interplanetary Shipyard's, and Shipyard has announced the
# end of it, of the libp2p bootstrap nodes and of delegated-ipfs.dev. Left as
# "auto", this provider would lose its route into the DHT on a date nobody
# here controls.
#
# The list is the AminoDHT entries (dnsaddr-indirected, so they stay valid
# for as long as anyone keeps them running). Add a bootstrap node of your own
# to it, on different hardware and in DHT server mode, so the provider does
# not depend on infrastructure somebody else is switching off.
#
# Routing.Type is pinned to the SERVER "dht", not left to autodetection. The
# `server` profile does NOT set it (config/profile.go only touches
# Addresses.NoAnnounce, Swarm.AddrFilters, Discovery.MDNS and
# Swarm.DisableNatPortMap), so without this line the node runs kubo's default
# "auto", which picks server or client from what AutoNAT concludes about
# reachability. That is the one property this design calls load-bearing: a
# bootstrap peer in client mode answers no DHT queries, so it hands a new node
# a connection and an EMPTY routing table -- which is why your own box
# has to be promoted before it can be a rendezvous at all. A firewall change
# or an AutoNAT misdetection on restart would demote this provider silently:
# the gateway would keep serving, every box would still show as connected,
# and nothing would look wrong until a fresh client could not bootstrap.
#
# DelegatedRouters and DelegatedPublishers are emptied rather than repointed:
# this node runs a full DHT, so delegating a lookup to a third party buys it
# nothing and told delegated-ipfs.dev every CID it ever asked for.
#
# All four "auto" values are written because kubo expands each of them through
# AutoConf. Note what it does NOT do: only `Bootstrap: ["auto"]` exactly is
# fatal (config/autoconf_client.go), and every other leftover "auto" is a
# single log line nobody reads. So this list is the whole mitigation -- there
# is no loud failure backing it up, and a fifth "auto" appearing upstream
# would pass silently.
ipfs config --json Bootstrap '[
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
  "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
  "/dnsaddr/va1.bootstrap.libp2p.io/p2p/12D3KooWKnDdG3iXw9eTFijk3EWSunZcFi54Zka4wmtqtt6rPxc8",
  "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
]'
ipfs config --json Routing.DelegatedRouters '[]'
ipfs config --json Ipns.DelegatedPublishers '[]'
ipfs config --json DNS.Resolvers '{}'
ipfs config --json AutoConf.Enabled false
ipfs config Routing.Type dht

exec ipfs daemon
