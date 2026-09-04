// Package provider is the datastore that makes a stock kubo serve blocks that
// live on Sia, held by the bridge, without storing one of them locally.
//
// kubo keeps blocks under the datastore key `/blocks/<base32 multihash>`. Here
// that mount is not a directory of files but bridged, over HTTP on the
// container network: a block is fetched from the gateway's trustless form
// (`GET /ipfs/<cid>?format=raw`, hash-checked by kubo on receipt), its
// presence and size come from a HEAD (which bridged answers from its index
// without touching a Sia host), and the full key list — what the DHT is told
// we provide — is `GET /api/blocks`. Every GET goes through the gateway's
// metering, so a bitswap peer costs the pin's owner exactly what a browser
// would.
//
// kubo also writes a few blocks of its own (the empty MFS root at first start,
// anything an operator `ipfs add`s inside the container). Those go to a local
// LevelDB beside the repo and are read back from there first; nothing is ever
// written to bridged from here.
package provider

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ipfs/boxo/datastore/dshelp"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	leveldb "github.com/ipfs/go-ds-leveldb"
)

// Config is the datastore's block in kubo's Datastore.Spec:
//
//	{ "type": "siabridge", "url": "http://bridged:8080" }
type Config struct {
	// URL is bridged's origin on the container network.
	URL string
	// Path is where the local overlay lives (kubo passes the repo path).
	Path string
}

// Datastore is the /blocks mount: bridged behind a local overlay.
type Datastore struct {
	url     string
	client  *http.Client
	local   *leveldb.Datastore
	overlay string
}

var _ ds.Batching = (*Datastore)(nil)

// New opens the overlay and returns the datastore. It does not touch bridged:
// the daemon must be able to start while the service is restarting.
func New(cfg Config) (*Datastore, error) {
	if !strings.HasPrefix(cfg.URL, "http://") && !strings.HasPrefix(cfg.URL, "https://") {
		return nil, fmt.Errorf("siabridge datastore: url must be http(s), got %q", cfg.URL)
	}
	overlay := filepath.Join(cfg.Path, "siabridge-local")
	local, err := leveldb.NewDatastore(overlay, nil)
	if err != nil {
		return nil, fmt.Errorf("siabridge datastore: overlay: %w", err)
	}
	return &Datastore{
		url:     strings.TrimRight(cfg.URL, "/"),
		client:  &http.Client{Timeout: 2 * time.Minute},
		local:   local,
		overlay: overlay,
	}, nil
}

func (d *Datastore) localPath() string { return d.overlay }

// blockURL is the gateway's raw-block URL for a datastore key. The CID is
// spelled raw: bridged matches a block by multihash whatever the codec.
func (d *Datastore) blockURL(k ds.Key) (string, error) {
	h, err := dshelp.DsKeyToMultihash(k)
	if err != nil {
		return "", err
	}
	return d.url + "/ipfs/" + cid.NewCidV1(cid.Raw, h).String() + "?format=raw", nil
}

func (d *Datastore) do(ctx context.Context, method, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.ipld.raw")
	return d.client.Do(req)
}

// Get: the overlay, then the gateway.
func (d *Datastore) Get(ctx context.Context, k ds.Key) ([]byte, error) {
	if v, err := d.local.Get(ctx, k); err == nil {
		return v, nil
	} else if !errors.Is(err, ds.ErrNotFound) {
		return nil, err
	}
	url, err := d.blockURL(k)
	if err != nil {
		return nil, ds.ErrNotFound
	}
	res, err := d.do(ctx, http.MethodGet, url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	switch res.StatusCode {
	case http.StatusOK:
		return io.ReadAll(res.Body)
	case http.StatusNotFound:
		return nil, ds.ErrNotFound
	default:
		return nil, fmt.Errorf("siabridge datastore: GET %s: %s", url, res.Status)
	}
}

// Has: the overlay, then a HEAD — no bytes move, nothing is metered.
func (d *Datastore) Has(ctx context.Context, k ds.Key) (bool, error) {
	if ok, err := d.local.Has(ctx, k); err != nil || ok {
		return ok, err
	}
	_, err := d.GetSize(ctx, k)
	if errors.Is(err, ds.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// GetSize: the overlay, then the HEAD's content-length.
func (d *Datastore) GetSize(ctx context.Context, k ds.Key) (int, error) {
	if n, err := d.local.GetSize(ctx, k); err == nil {
		return n, nil
	} else if !errors.Is(err, ds.ErrNotFound) {
		return -1, err
	}
	url, err := d.blockURL(k)
	if err != nil {
		return -1, ds.ErrNotFound
	}
	res, err := d.do(ctx, http.MethodHead, url)
	if err != nil {
		return -1, err
	}
	res.Body.Close()
	switch res.StatusCode {
	case http.StatusOK:
		n, err := strconv.Atoi(res.Header.Get("Content-Length"))
		if err != nil {
			return -1, fmt.Errorf("siabridge datastore: HEAD %s: no content-length", url)
		}
		return n, nil
	case http.StatusNotFound:
		return -1, ds.ErrNotFound
	default:
		return -1, fmt.Errorf("siabridge datastore: HEAD %s: %s", url, res.Status)
	}
}

// Put keeps kubo's own blocks locally. bridged is never written to.
func (d *Datastore) Put(ctx context.Context, k ds.Key, value []byte) error {
	return d.local.Put(ctx, k, value)
}

// Delete forgets a local block; what bridged holds is the pin's business.
func (d *Datastore) Delete(ctx context.Context, k ds.Key) error {
	return d.local.Delete(ctx, k)
}

func (d *Datastore) Sync(ctx context.Context, prefix ds.Key) error {
	return d.local.Sync(ctx, prefix)
}

func (d *Datastore) Batch(ctx context.Context) (ds.Batch, error) {
	return ds.NewBasicBatch(d), nil
}

func (d *Datastore) Close() error {
	d.client.CloseIdleConnections()
	return d.local.Close()
}

// Query lists keys: the overlay's, then every block bridged holds, one CID
// per line from /api/blocks, streamed. Values (rare: a `KeysOnly:false`
// query) are fetched one by one on the way out. This is what kubo's provider
// walks to announce everything to the DHT.
func (d *Datastore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	prefix := ds.NewKey(q.Prefix).String()
	if prefix != "/" {
		// Block keys are flat (`/<base32>`); nothing lives under any deeper prefix.
		return dsq.ResultsWithEntries(q, nil), nil
	}
	localRes, err := d.local.Query(ctx, dsq.Query{KeysOnly: true})
	if err != nil {
		return nil, err
	}
	res, err := d.do(ctx, http.MethodGet, d.url+"/api/blocks")
	if err != nil {
		localRes.Close()
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		localRes.Close()
		return nil, fmt.Errorf("siabridge datastore: GET /api/blocks: %s", res.Status)
	}

	scanner := bufio.NewScanner(res.Body)
	scanner.Buffer(make([]byte, 0, 4096), 4096)
	seen := map[string]struct{}{}
	next := func() (dsq.Result, bool) {
		for {
			// Overlay first: a block kubo wrote is provided like any other.
			if localRes != nil {
				r, ok := localRes.NextSync()
				if ok {
					seen[r.Key] = struct{}{}
					return d.entry(ctx, q, r.Key), true
				}
				localRes.Close()
				localRes = nil
			}
			if !scanner.Scan() {
				res.Body.Close()
				if err := scanner.Err(); err != nil {
					return dsq.Result{Error: err}, true
				}
				return dsq.Result{}, false
			}
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			c, err := cid.Decode(line)
			if err != nil {
				return dsq.Result{Error: fmt.Errorf("siabridge datastore: /api/blocks: %q: %w", line, err)}, true
			}
			key := dshelp.MultihashToDsKey(c.Hash()).String()
			if _, dup := seen[key]; dup {
				continue
			}
			return d.entry(ctx, q, key), true
		}
	}
	iter := dsq.Iterator{
		Next: next,
		Close: func() error {
			res.Body.Close()
			if localRes != nil {
				return localRes.Close()
			}
			return nil
		},
	}
	// Filters, orders, offset and limit as the query asked, over the stream.
	return dsq.NaiveQueryApply(q, dsq.ResultsFromIterator(q, iter)), nil
}

func (d *Datastore) entry(ctx context.Context, q dsq.Query, key string) dsq.Result {
	e := dsq.Entry{Key: key}
	if q.KeysOnly && !q.ReturnsSizes {
		return dsq.Result{Entry: e}
	}
	if q.ReturnsSizes {
		n, err := d.GetSize(ctx, ds.RawKey(key))
		if err != nil {
			return dsq.Result{Error: err}
		}
		e.Size = n
	}
	if !q.KeysOnly {
		v, err := d.Get(ctx, ds.RawKey(key))
		if err != nil {
			return dsq.Result{Error: err}
		}
		e.Value = v
		e.Size = len(v)
	}
	return dsq.Result{Entry: e}
}
