package registry

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ambroslabs/malcom/internal/durable"
)

// tarEntry is one fake chain.json plus the relative path it should
// land at inside the synthetic chain-registry tarball (omitting the
// chain-registry-master/ prefix that tarballs always start with).
type tarEntry struct {
	rel  string // e.g. "cosmoshub/chain.json"
	body []byte
}

func mkChainJSON(t *testing.T, chainID, status string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"chain_id": chainID,
		"status":   status,
		"apis": map[string]any{
			"rpc": []map[string]any{
				{"address": "https://rpc." + chainID + "/", "provider": "test"},
			},
		},
		"peers": map[string]any{
			"seeds": []map[string]any{
				{"id": strings.Repeat("a", 40), "address": "seed-a." + chainID + ":26656", "provider": "test"},
			},
			"persistent_peers": []map[string]any{
				{"id": strings.Repeat("b", 40), "address": "peer-b." + chainID + ":26656", "provider": "test"},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal chain.json: %v", err)
	}
	return body
}

// makeTarball builds an in-memory gzipped tarball containing the
// supplied entries plus a directory entry for the root, mimicking the
// real chain-registry tarball's shape.
func makeTarball(t *testing.T, entries []tarEntry) []byte {
	t.Helper()
	var raw bytes.Buffer
	gz := gzip.NewWriter(&raw)
	tw := tar.NewWriter(gz)
	const root = "chain-registry-master"
	if err := tw.WriteHeader(&tar.Header{Name: root + "/", Typeflag: tar.TypeDir, Mode: 0o755}); err != nil {
		t.Fatalf("write root header: %v", err)
	}
	for _, e := range entries {
		if err := tw.WriteHeader(&tar.Header{
			Name:     root + "/" + e.rel,
			Mode:     0o644,
			Size:     int64(len(e.body)),
			Typeflag: tar.TypeReg,
		}); err != nil {
			t.Fatalf("write tar header %s: %v", e.rel, err)
		}
		if _, err := tw.Write(e.body); err != nil {
			t.Fatalf("write tar body %s: %v", e.rel, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("tw close: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gz close: %v", err)
	}
	return raw.Bytes()
}

// serveTarball stands up a one-request HTTPS server that returns the
// supplied tarball bytes, returning the URL.
func serveTarball(t *testing.T, body []byte) (url string, cleanup func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-gzip")
		_, _ = w.Write(body)
	}))
	return srv.URL, srv.Close
}

// TestExtractFiltersAndIndexes verifies the per-entry filter (suffix,
// underscore segments, status, malformed JSON) and the chain_id →
// directory index produced from a synthetic tarball.
func TestExtractFiltersAndIndexes(t *testing.T) {
	cosmoshub := mkChainJSON(t, "cosmoshub-4", "live")
	dydx := mkChainJSON(t, "dydx-mainnet-1", "live")
	testnet := mkChainJSON(t, "cosmoshubtestnet-4", "live")
	dead := mkChainJSON(t, "old-chain-1", "killed")

	tarBytes := makeTarball(t, []tarEntry{
		// kept
		{rel: "cosmoshub/chain.json", body: cosmoshub},
		{rel: "dydx/chain.json", body: dydx},
		{rel: "testnets/cosmoshubtestnet/chain.json", body: testnet},
		// dropped: status killed
		{rel: "oldchain/chain.json", body: dead},
		// dropped: underscore segments at any depth
		{rel: "_template/chain.json", body: cosmoshub},
		{rel: "_non-cosmos/ethereum/chain.json", body: cosmoshub},
		{rel: "testnets/_template/chain.json", body: cosmoshub},
		// dropped: not a chain.json
		{rel: "cosmoshub/assetlist.json", body: []byte(`{}`)},
		// dropped: malformed JSON
		{rel: "broken/chain.json", body: []byte(`{not json`)},
	})

	cacheDir := t.TempDir()
	res, err := extractTarball(context.Background(), bytes.NewReader(tarBytes), cacheDir)
	if err != nil {
		t.Fatalf("extractTarball: %v", err)
	}
	chains, collisions := resolveIndex(res.byChainID)
	if len(collisions) != 0 {
		t.Fatalf("unexpected collisions: %+v", collisions)
	}
	want := map[string]string{
		"cosmoshub-4":         "cosmoshub",
		"dydx-mainnet-1":      "dydx",
		"cosmoshubtestnet-4":  "testnets/cosmoshubtestnet",
	}
	if len(chains) != len(want) {
		t.Fatalf("indexed=%d, want=%d (got=%v)", len(chains), len(want), chains)
	}
	for cid, dir := range want {
		if got := chains[cid]; got != dir {
			t.Errorf("chain %q: got dir %q, want %q", cid, got, dir)
		}
	}
	if res.result.DroppedKilled != 1 {
		t.Errorf("DroppedKilled=%d, want 1", res.result.DroppedKilled)
	}
	if res.result.DroppedFiltered != 3 {
		t.Errorf("DroppedFiltered=%d, want 3 (_template/, _non-cosmos/ethereum/, testnets/_template/)", res.result.DroppedFiltered)
	}

	// Cached chain.json files must be readable from the expected paths.
	for cid, dir := range want {
		body, err := os.ReadFile(filepath.Join(cacheDir, filepath.FromSlash(dir), "chain.json"))
		if err != nil {
			t.Fatalf("read cached %s: %v", cid, err)
		}
		var raw chainJSON
		if err := json.Unmarshal(body, &raw); err != nil {
			t.Fatalf("parse cached %s: %v", cid, err)
		}
		if raw.ChainID != cid {
			t.Errorf("cached %s: ChainID=%q, want %q", cid, raw.ChainID, cid)
		}
	}
}

// TestExtractRejectsPathTraversal feeds a tarball whose entry name
// climbs out of the root via ".." segments. extractTarball must
// refuse the entry and write nothing outside the cache dir.
func TestExtractRejectsPathTraversal(t *testing.T) {
	good := mkChainJSON(t, "cosmoshub-4", "live")
	evil := mkChainJSON(t, "evil-1", "live")

	tarBytes := makeTarball(t, []tarEntry{
		// rel = "../escape/chain.json" — has no underscore segments
		// (so the existing filter accepts it) but escapes cacheDir.
		{rel: "../escape/chain.json", body: evil},
		{rel: "cosmoshub/chain.json", body: good},
	})

	cacheDir := t.TempDir()
	_, err := extractTarball(context.Background(), bytes.NewReader(tarBytes), cacheDir)
	if err == nil {
		t.Fatalf("extractTarball accepted ../escape/chain.json")
	}
	if !strings.Contains(err.Error(), "escapes") {
		t.Fatalf("err=%v, want a path-escape diagnostic", err)
	}

	// The escape sibling dir must not exist on disk.
	parent := filepath.Dir(cacheDir)
	if _, err := os.Stat(filepath.Join(parent, "escape")); !os.IsNotExist(err) {
		t.Fatalf("escape dir created outside cache: stat err = %v", err)
	}
}

// TestResolveIndexCollisions confirms multi-directory live entries for
// the same chain_id are excluded from the index AND surfaced as
// Collisions, rather than the silent last-writer-wins behavior.
func TestResolveIndexCollisions(t *testing.T) {
	in := map[string][]string{
		"cosmoshub-4":     {"cosmoshub"},
		"morocco-1":       {"chronicnetwork", "terpnetwork"},
		"andromeda-1":     {"andromeda1", "andromeda"},
		"dydx-mainnet-1":  {"dydx"},
	}
	chains, collisions := resolveIndex(in)
	if _, present := chains["morocco-1"]; present {
		t.Errorf("collision chain_id leaked into index: %v", chains)
	}
	if _, present := chains["andromeda-1"]; present {
		t.Errorf("collision chain_id leaked into index: %v", chains)
	}
	if got := chains["cosmoshub-4"]; got != "cosmoshub" {
		t.Errorf("non-collision chain dropped: %v", chains)
	}

	// Collisions must be sorted by chain_id and have sorted Paths.
	if len(collisions) != 2 {
		t.Fatalf("collisions=%d, want 2 (got %+v)", len(collisions), collisions)
	}
	if collisions[0].ChainID != "andromeda-1" || collisions[1].ChainID != "morocco-1" {
		t.Errorf("collisions not sorted by chain_id: %+v", collisions)
	}
	if !sort.StringsAreSorted(collisions[0].Paths) || !sort.StringsAreSorted(collisions[1].Paths) {
		t.Errorf("collision paths not sorted: %+v", collisions)
	}
}

// TestSyncIndexRoundTrip exercises the full Sync → Lookup path against
// a synthetic tarball server: index.json is written, IndexAge is fresh,
// and Lookup recovers ChainInfo for indexed chains.
func TestSyncIndexRoundTrip(t *testing.T) {
	cosmoshub := mkChainJSON(t, "cosmoshub-4", "live")
	dydx := mkChainJSON(t, "dydx-mainnet-1", "live")
	tarBytes := makeTarball(t, []tarEntry{
		{rel: "cosmoshub/chain.json", body: cosmoshub},
		{rel: "dydx/chain.json", body: dydx},
	})

	url, cleanup := serveTarball(t, tarBytes)
	defer cleanup()

	// Sync hits a hard-coded URL; route the test through it via a
	// constant override using a per-test helper.
	cacheDir := t.TempDir()
	if err := syncFromURL(context.Background(), cacheDir, url); err != nil {
		t.Fatalf("syncFromURL: %v", err)
	}

	age, err := IndexAge(cacheDir)
	if err != nil {
		t.Fatalf("IndexAge: %v", err)
	}
	if age < 0 || age > 5*time.Second {
		t.Errorf("age=%v, want fresh", age)
	}

	info, err := Lookup(cacheDir, "cosmoshub-4")
	if err != nil {
		t.Fatalf("Lookup cosmoshub-4: %v", err)
	}
	if info.ChainID != "cosmoshub-4" {
		t.Errorf("ChainID=%q, want cosmoshub-4", info.ChainID)
	}
	if len(info.RPCs) == 0 {
		t.Errorf("RPCs empty")
	}

	if _, err := Lookup(cacheDir, "nope-1"); err == nil {
		t.Errorf("expected ErrNotFound for unknown chain id")
	}
}

// TestLookupMissingIndex verifies the absent-cache code path surfaces
// ErrIndexMissing rather than a generic I/O error.
func TestLookupMissingIndex(t *testing.T) {
	if _, err := Lookup(t.TempDir(), "cosmoshub-4"); err == nil {
		t.Fatalf("expected error for empty cache dir")
	} else if !errors.Is(err, ErrIndexMissing) {
		t.Fatalf("got %v, want ErrIndexMissing", err)
	}
}

// TestLookupChainIDMismatch verifies the cached chain.json is
// chain_id-checked against the lookup key — guards against silent
// corruption if the cache is hand-edited or upstream renames.
func TestLookupChainIDMismatch(t *testing.T) {
	cacheDir := t.TempDir()
	// Write a chain.json whose internal chain_id disagrees with the
	// one the index will point at.
	if err := os.MkdirAll(filepath.Join(cacheDir, "cosmoshub"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cacheDir, "cosmoshub", "chain.json"),
		mkChainJSON(t, "cosmoshub-OLD", "live"), 0o644); err != nil {
		t.Fatalf("write chain.json: %v", err)
	}
	idx := chainIndex{
		Version: 1,
		Chains:  map[string]string{"cosmoshub-4": "cosmoshub"},
	}
	body, _ := json.MarshalIndent(idx, "", "  ")
	if err := os.WriteFile(filepath.Join(cacheDir, "index.json"), body, 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}

	_, err := Lookup(cacheDir, "cosmoshub-4")
	if err == nil {
		t.Fatalf("expected mismatch error")
	}
	if !strings.Contains(err.Error(), "chain_id") || !strings.Contains(err.Error(), "cosmoshub-OLD") {
		t.Fatalf("err=%v, want chain_id mismatch mentioning cosmoshub-OLD", err)
	}
}

// syncFromURL is the test variant of Sync that hits a caller-supplied
// URL instead of the hard-coded chain-registry one. Mirrors the body
// of Sync for the post-extract index-write path so we exercise the
// real format end-to-end.
func syncFromURL(ctx context.Context, cacheDir, url string) error {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(cacheDir, indexFilename)); err != nil && !os.IsNotExist(err) {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	res, err := extractTarball(ctx, resp.Body, cacheDir)
	if err != nil {
		return err
	}
	chains, _ := resolveIndex(res.byChainID)
	idx := chainIndex{
		Version:   indexVersion,
		FetchedAt: time.Now().UTC(),
		Chains:    chains,
	}
	body, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return err
	}
	return durable.WriteFile(filepath.Join(cacheDir, indexFilename), body, 0o644)
}
