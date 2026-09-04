package provider

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ipfs/boxo/datastore/dshelp"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	mh "github.com/multiformats/go-multihash"
)

// A stand-in bridged: raw blocks by CID (any codec spelling), HEAD from the
// index, and the key list — counting GETs the way the gateway meters them.
type fakeBridged struct {
	blocks map[string][]byte // multihash (as string) → bytes
	gets   atomic.Int64
	heads  atomic.Int64
	lists  atomic.Int64
}

func (f *fakeBridged) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/blocks" {
		f.lists.Add(1)
		for h := range f.blocks {
			c := cid.NewCidV1(cid.DagProtobuf, mh.Multihash(h)) // bridged spells them canonical, dag-pb here
			w.Write([]byte(c.String() + "\n"))
		}
		return
	}
	if !strings.HasPrefix(r.URL.Path, "/ipfs/") || r.URL.Query().Get("format") != "raw" {
		http.Error(w, "not an /ipfs/ path", 404)
		return
	}
	c, err := cid.Decode(strings.TrimPrefix(r.URL.Path, "/ipfs/"))
	if err != nil {
		http.Error(w, "not a CID", 400)
		return
	}
	data, ok := f.blocks[string(c.Hash())]
	if !ok {
		http.Error(w, "not pinned here", 404)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Content-Type", "application/vnd.ipld.raw")
	if r.Method == http.MethodHead {
		f.heads.Add(1)
		return
	}
	f.gets.Add(1)
	w.Write(data)
}

func block(t *testing.T, s string) (mh.Multihash, []byte) {
	t.Helper()
	h, err := mh.Sum([]byte(s), mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	return h, []byte(s)
}

func TestDatastoreReadsBridgedAndWritesLocally(t *testing.T) {
	ctx := context.Background()
	fake := &fakeBridged{blocks: map[string][]byte{}}
	h1, b1 := block(t, "one")
	h2, b2 := block(t, "two")
	fake.blocks[string(h1)] = b1
	fake.blocks[string(h2)] = b2
	srv := httptest.NewServer(fake)
	defer srv.Close()

	d, err := New(Config{URL: srv.URL, Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()

	k1 := dshelp.MultihashToDsKey(h1)
	// Has and GetSize are HEADs: nothing is metered for a want-have.
	if ok, err := d.Has(ctx, k1); err != nil || !ok {
		t.Fatalf("Has: %v %v", ok, err)
	}
	if n, err := d.GetSize(ctx, k1); err != nil || n != len(b1) {
		t.Fatalf("GetSize: %d %v", n, err)
	}
	if fake.gets.Load() != 0 || fake.heads.Load() != 2 {
		t.Fatalf("expected 2 HEADs and no GET, got %d/%d", fake.heads.Load(), fake.gets.Load())
	}
	// Get is the one that moves bytes.
	if v, err := d.Get(ctx, k1); err != nil || string(v) != "one" {
		t.Fatalf("Get: %q %v", v, err)
	}
	if fake.gets.Load() != 1 {
		t.Fatalf("expected 1 GET, got %d", fake.gets.Load())
	}
	// A block nobody holds is ErrNotFound, not an error.
	h3, _ := block(t, "three")
	if _, err := d.Get(ctx, dshelp.MultihashToDsKey(h3)); err != ds.ErrNotFound {
		t.Fatalf("missing block: %v", err)
	}
	if ok, _ := d.Has(ctx, dshelp.MultihashToDsKey(h3)); ok {
		t.Fatal("missing block reported present")
	}
	// A key that is not a multihash at all is simply absent.
	if _, err := d.Get(ctx, ds.NewKey("/not-a-hash")); err != ds.ErrNotFound {
		t.Fatalf("garbage key: %v", err)
	}

	// kubo's own writes stay local and never reach bridged.
	k3 := dshelp.MultihashToDsKey(h3)
	if err := d.Put(ctx, k3, []byte("three")); err != nil {
		t.Fatal(err)
	}
	if v, err := d.Get(ctx, k3); err != nil || string(v) != "three" {
		t.Fatalf("local Get: %q %v", v, err)
	}
	if n, err := d.GetSize(ctx, k3); err != nil || n != 5 {
		t.Fatalf("local GetSize: %d %v", n, err)
	}
	if fake.gets.Load() != 1 {
		t.Fatal("a local block must not be fetched")
	}
	entries, err := os.ReadDir(filepath.Join(d.localPath()))
	if err != nil || len(entries) == 0 {
		t.Fatalf("overlay on disk: %v %d", err, len(entries))
	}

	// The key list: everything bridged holds plus the local block, each once,
	// as the /blocks keys kubo's provider walks.
	res, err := d.Query(ctx, dsq.Query{KeysOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	all, err := res.Rest()
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{}
	for _, e := range all {
		keys[e.Key] = true
	}
	for _, want := range []ds.Key{k1, dshelp.MultihashToDsKey(h2), k3} {
		if !keys[want.String()] {
			t.Fatalf("key list lacks %s: %v", want, keys)
		}
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 keys once each, got %d", len(all))
	}
	if fake.gets.Load() != 1 {
		t.Fatal("listing keys must not fetch blocks")
	}
	// Under any deeper prefix there is nothing.
	res, _ = d.Query(ctx, dsq.Query{Prefix: "/deeper", KeysOnly: true})
	if all, _ := res.Rest(); len(all) != 0 {
		t.Fatalf("prefix query: %d", len(all))
	}
	// With values, the blocks come along.
	res, _ = d.Query(ctx, dsq.Query{Limit: 3})
	all, _ = res.Rest()
	if len(all) != 3 {
		t.Fatalf("query with values: %d", len(all))
	}
	for _, e := range all {
		if len(e.Value) == 0 {
			t.Fatalf("no value for %s", e.Key)
		}
	}
}

func TestDatastoreDeleteOverlayOnly(t *testing.T) {
	ctx := context.Background()
	fake := &fakeBridged{blocks: map[string][]byte{}}
	h1, b1 := block(t, "kept")
	fake.blocks[string(h1)] = b1
	srv := httptest.NewServer(fake)
	defer srv.Close()
	d, err := New(Config{URL: srv.URL, Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	k := dshelp.MultihashToDsKey(h1)
	// Deleting a bridged block is a no-op on it: the pin decides.
	if err := d.Delete(ctx, k); err != nil {
		t.Fatal(err)
	}
	if ok, _ := d.Has(ctx, k); !ok {
		t.Fatal("bridged's block must survive a Delete")
	}
}

func TestSpecAndProfile(t *testing.T) {
	spec := Spec("http://bridged:8080")
	raw, _ := json.Marshal(spec)
	if !strings.Contains(string(raw), `"type":"siabridge"`) || !strings.Contains(string(raw), `"mountpoint":"/blocks"`) {
		t.Fatalf("spec: %s", raw)
	}
	parser := Plugin{}.DatastoreConfigParser()
	if _, err := parser(map[string]any{}); err == nil {
		t.Fatal("a spec without a url must be refused")
	}
	cfg, err := parser(map[string]any{"type": TypeName, "url": "http://x"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DiskSpec()["type"] != TypeName || cfg.DiskSpec()["url"] != nil {
		t.Fatalf("disk spec must name the type only: %v", cfg.DiskSpec())
	}
	if _, err := New(Config{URL: "ftp://x", Path: t.TempDir()}); err == nil {
		t.Fatal("a non-http url must be refused")
	}
}

// The provider walks every key at each reprovide. A million of them must
// stream through — one line in, one key out — without the list ever being
// held whole on either side.
func TestQueryStreamsAMillionKeys(t *testing.T) {
	const n = 1_000_000
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/blocks" {
			http.Error(w, "unexpected", 500)
			return
		}
		bw := bufio.NewWriterSize(w, 1<<16)
		var buf [8]byte
		for i := 0; i < n; i++ {
			binary.BigEndian.PutUint64(buf[:], uint64(i))
			h, _ := mh.Sum(buf[:], mh.SHA2_256, -1)
			bw.WriteString(cid.NewCidV1(cid.Raw, h).String())
			bw.WriteByte('\n')
		}
		bw.Flush()
	}))
	defer srv.Close()
	d, err := New(Config{URL: srv.URL, Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	res, err := d.Query(context.Background(), dsq.Query{KeysOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for r := range res.Next() {
		if r.Error != nil {
			t.Fatal(r.Error)
		}
		if _, err := dshelp.DsKeyToMultihash(ds.RawKey(r.Key)); err != nil {
			t.Fatalf("key %q: %v", r.Key, err)
		}
		count++
	}
	runtime.ReadMemStats(&after)
	if count != n {
		t.Fatalf("streamed %d of %d keys", count, n)
	}
	if grew := after.HeapInuse - before.HeapInuse; grew > 64<<20 {
		t.Fatalf("heap grew by %d MiB walking the list: not streaming", grew>>20)
	}
}
