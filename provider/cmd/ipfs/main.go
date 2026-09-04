// The bridge provider: kubo, unchanged, with the siabridge datastore preloaded.
// Built by deploy/provider/Dockerfile; `ipfs init --profile=server,siabridge`
// then `ipfs daemon` is the whole sidecar.
package main

import (
	"os"

	"github.com/ipfs/kubo/cmd/ipfs/kubo"
	"github.com/ipfs/kubo/plugin/loader"

	"github.com/keptlive/sia-ipfs-bridge/provider"
)

func main() {
	os.Exit(kubo.Start(kubo.BuildEnv(func(l *loader.PluginLoader) error {
		return l.Load(provider.Plugin{})
	})))
}
