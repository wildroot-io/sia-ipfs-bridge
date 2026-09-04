package provider

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/ipfs/kubo/config"
	"github.com/ipfs/kubo/plugin"
	"github.com/ipfs/kubo/repo"
	"github.com/ipfs/kubo/repo/fsrepo"
)

// Environment kubo reads at `ipfs init --profile=server,siabridge`.
const (
	// EnvURL is bridged's origin on the container network.
	EnvURL = "BRIDGE_URL"
	// EnvIdentity is bridged's `kubo-identity.json` ({PeerID, PrivKey}),
	// so this node IS the peer bridged's /routing/v1 answers with.
	EnvIdentity = "BRIDGE_IDENTITY"

	DefaultURL = "http://bridged:8080"
	// TypeName is the datastore type in Datastore.Spec.
	TypeName = "siabridge"
	// ProfileName is the init profile: this datastore, provide everything.
	ProfileName = "siabridge"
)

// Plugin registers the datastore type and the init profile with kubo.
type Plugin struct{}

var _ plugin.PluginDatastore = (*Plugin)(nil)

func (Plugin) Name() string    { return "siabridge-datastore" }
func (Plugin) Version() string { return "0.1.0" }

// Init adds the `siabridge` profile. It runs before any command, `ipfs init`
// included, so the profile is there to be named on the command line.
func (Plugin) Init(env *plugin.Environment) error {
	config.Profiles[ProfileName] = config.Profile{
		Description: `The bridge provider: blocks come from bridged over HTTP, every one
of them is announced to the DHT, and the peer identity is the service's own
(BRIDGE_IDENTITY). Apply on top of 'server': ipfs init --profile=server,siabridge.`,
		InitOnly:  true,
		Transform: transform,
	}
	return nil
}

func transform(c *config.Config) error {
	url := os.Getenv(EnvURL)
	if url == "" {
		url = DefaultURL
	}
	c.Datastore.Spec = Spec(url)
	// Everything held is announced: the whole point of the node.
	c.Provide.Strategy = config.NewOptionalString("all")
	// A provider, not a client: the DHT alone, no delegated lookups.
	c.Routing.Type = config.NewOptionalString("dht")
	if file := os.Getenv(EnvIdentity); file != "" {
		raw, err := os.ReadFile(file)
		if err != nil {
			return fmt.Errorf("%s: %w", EnvIdentity, err)
		}
		var id config.Identity
		if err := json.Unmarshal(raw, &id); err != nil || id.PeerID == "" || id.PrivKey == "" {
			return fmt.Errorf("%s: not a {PeerID, PrivKey} file", EnvIdentity)
		}
		c.Identity = id
	}
	return nil
}

// Spec is kubo's Datastore.Spec with this datastore on /blocks and the usual
// LevelDB for everything else (pins, keystore, provider queue).
func Spec(url string) map[string]any {
	return map[string]any{
		"type": "mount",
		"mounts": []any{
			map[string]any{
				"mountpoint": "/blocks",
				"type":       "measure",
				"prefix":     "siabridge.datastore",
				"child": map[string]any{
					"type": TypeName,
					"url":  url,
				},
			},
			map[string]any{
				"mountpoint": "/",
				"type":       "measure",
				"prefix":     "leveldb.datastore",
				"child": map[string]any{
					"type":        "levelds",
					"path":        "datastore",
					"compression": "none",
				},
			},
		},
	}
}

func (Plugin) DatastoreTypeName() string { return TypeName }

func (Plugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(m map[string]any) (fsrepo.DatastoreConfig, error) {
		url, _ := m["url"].(string)
		if url == "" {
			return nil, fmt.Errorf("%s datastore: no url", TypeName)
		}
		return &datastoreConfig{url: url}, nil
	}
}

type datastoreConfig struct{ url string }

// DiskSpec identifies what is on disk. The URL is where the blocks are read
// from, not what they are, so a renamed service is not a migration.
func (datastoreConfig) DiskSpec() fsrepo.DiskSpec {
	return fsrepo.DiskSpec{"type": TypeName}
}

// Create opens the datastore. BRIDGE_URL in the daemon's environment wins
// over the URL written into the spec at init, so the compose file stays the
// one place the service's address is configured.
func (c *datastoreConfig) Create(path string) (repo.Datastore, error) {
	url := c.url
	if env := os.Getenv(EnvURL); env != "" {
		url = env
	}
	return New(Config{URL: url, Path: path})
}
