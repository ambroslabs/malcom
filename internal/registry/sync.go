package registry

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ambroslabs/malcom/internal/durable"
)

// tarballURL is GitHub's codeload endpoint for chain-registry's master
// branch. We use codeload (not the api.github.com tarball/zipball
// endpoints) to avoid the 60 req/h unauthenticated rate limit.
const tarballURL = "https://codeload.github.com/cosmos/chain-registry/tar.gz/refs/heads/master"

// tarballMaxBytes caps the tarball read so a runaway response can't fill
// the cache disk. Real chain-registry tarball is ~60 MiB; 256 MiB leaves
// generous headroom for growth.
const tarballMaxBytes = 256 << 20

// chainJSONMaxBytes caps one chain.json's size during extraction.
// Real entries are <30 KiB; 1 MiB is a sane upper bound.
const chainJSONMaxBytes = 1 << 20

const (
	indexFilename = "index.json"
	chainJSONName = "chain.json"
	indexVersion  = 1
)

// Collision describes a chain_id that appeared in multiple registry
// directories with status != killed. Sync drops every entry in a
// collision from the index — the chain_id is ambiguous, so we'd rather
// fail loud than silently pick one.
type Collision struct {
	ChainID string
	Paths   []string // registry-relative dirs (sorted), e.g. ["odin", "odin1"]
}

// SyncResult summarises what Sync did. Callers (CLI commands) format it
// for the operator; the registry package itself never prints.
type SyncResult struct {
	TotalSeen      int         // chain.json entries the filter accepted (live, non-_)
	IndexedChains  int         // entries that made it into the index (post-collision filter)
	DroppedKilled  int         // entries skipped because status == killed
	DroppedFiltered int        // entries skipped because path was under _template/_non-cosmos/etc.
	Collisions     []Collision // dropped chain_ids that appeared more than once live
}

// Sync streams the chain-registry tarball into cacheDir, keeping only
// `*/chain.json` files for cosmos chains, then writes index.json last.
// A crash mid-extract leaves the cache without an index, which callers
// treat as "needs re-sync."
func Sync(ctx context.Context, cacheDir string) (*SyncResult, error) {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir cache: %w", err)
	}
	indexPath := filepath.Join(cacheDir, indexFilename)
	// Remove the old index up front so a partial extract can't leave
	// the cache appearing fresh.
	if err := os.Remove(indexPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("remove old index: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tarballURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", tarballURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("GET %s: HTTP %d: %s", tarballURL, resp.StatusCode, body)
	}

	res, err := extractTarball(ctx, io.LimitReader(resp.Body, tarballMaxBytes), cacheDir)
	if err != nil {
		return nil, err
	}

	// Build the index map after the extract so multi-directory
	// collisions (live vs live) can be detected and excluded as a
	// group rather than chosen between.
	chains, collisions := resolveIndex(res.byChainID)
	res.result.IndexedChains = len(chains)
	res.result.Collisions = collisions

	idx := chainIndex{
		Version:   indexVersion,
		FetchedAt: time.Now().UTC(),
		Chains:    chains,
	}
	body, err := json.MarshalIndent(idx, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal index: %w", err)
	}
	if err := durable.WriteFile(indexPath, append(body, '\n'), 0o644); err != nil {
		return nil, fmt.Errorf("write index: %w", err)
	}
	return res.result, nil
}

// extractResult is sync.go-internal scratch state for the tar walk.
type extractResult struct {
	byChainID map[string][]string // chain_id → list of relative dirs (live, non-killed)
	result    *SyncResult
}

// extractTarball reads a gzipped tarball from r and writes the
// `*/chain.json` files we keep into cacheDir, returning a per-chain_id
// directory map for the caller to resolve into an index.
func extractTarball(ctx context.Context, r io.Reader, cacheDir string) (*extractResult, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("gunzip: %w", err)
	}
	defer gz.Close()

	out := &extractResult{
		byChainID: map[string][]string{},
		result:    &SyncResult{},
	}

	tr := tar.NewReader(gz)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("tar read: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		rel, ok := stripTarballRoot(hdr.Name)
		if !ok {
			continue
		}
		if !isChainJSONPath(rel) {
			continue
		}
		if !isKeepablePath(rel) {
			out.result.DroppedFiltered++
			continue
		}

		body, err := io.ReadAll(io.LimitReader(tr, chainJSONMaxBytes))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", rel, err)
		}
		var raw chainJSON
		if err := json.Unmarshal(body, &raw); err != nil {
			// Skip malformed entries rather than aborting the whole
			// sync — a single broken chain.json upstream shouldn't
			// brick the index.
			continue
		}
		if raw.ChainID == "" {
			continue
		}
		if raw.Status == "killed" {
			out.result.DroppedKilled++
			continue
		}

		dir := path.Dir(rel)
		dst := filepath.Join(cacheDir, filepath.FromSlash(dir), chainJSONName)
		// Defense in depth against a malicious tarball that smuggles a
		// "../" segment past the underscore filter. Today the source
		// is GitHub codeload over TLS for a repo we trust, but a future
		// mirror or fork shouldn't get to bypass this check.
		if err := ensureWithin(cacheDir, dst); err != nil {
			return nil, fmt.Errorf("tar entry %q: %w", rel, err)
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
		}
		if err := durable.WriteFile(dst, body, 0o644); err != nil {
			return nil, fmt.Errorf("write %s: %w", dst, err)
		}
		out.byChainID[raw.ChainID] = append(out.byChainID[raw.ChainID], dir)
		out.result.TotalSeen++
	}
	return out, nil
}

// resolveIndex collapses the per-chain_id directory list into a
// definitive chain_id → directory map. Chain ids advertised by more
// than one live entry are dropped from the map and reported as
// Collisions so the CLI can surface them; the operator can still
// inspect the on-disk chain.json files but `malcom add` will refuse
// to use the ambiguous id.
func resolveIndex(byChainID map[string][]string) (map[string]string, []Collision) {
	chains := make(map[string]string, len(byChainID))
	var collisions []Collision
	for cid, dirs := range byChainID {
		if len(dirs) == 1 {
			chains[cid] = dirs[0]
			continue
		}
		sorted := append([]string(nil), dirs...)
		sort.Strings(sorted)
		collisions = append(collisions, Collision{ChainID: cid, Paths: sorted})
	}
	sort.Slice(collisions, func(i, j int) bool {
		return collisions[i].ChainID < collisions[j].ChainID
	})
	return chains, collisions
}

// stripTarballRoot drops the top-level "chain-registry-master/" prefix
// from a tarball entry name. Returns the remainder + ok=true on match.
// Anything that doesn't have a slash is rejected (e.g. the root dir
// entry itself).
func stripTarballRoot(name string) (string, bool) {
	idx := strings.IndexByte(name, '/')
	if idx < 0 || idx == len(name)-1 {
		return "", false
	}
	return name[idx+1:], true
}

// isChainJSONPath reports whether rel ends in chain.json (the only
// file class we extract).
func isChainJSONPath(rel string) bool {
	return strings.HasSuffix(rel, "/"+chainJSONName)
}

// isKeepablePath rejects paths under metadata directories (any
// segment starting with "_") — these hold IBC metadata, the chain
// template, _non-cosmos chains, etc., none of which the cosmos-sdk/
// cometbft stack can bootstrap.
func isKeepablePath(rel string) bool {
	for _, seg := range strings.Split(rel, "/") {
		if strings.HasPrefix(seg, "_") {
			return false
		}
	}
	return true
}

// ensureWithin verifies that target resolves inside root after symbolic
// `..` components are flattened by filepath.Clean. We don't EvalSymlinks
// because we expect to write to a path that doesn't exist yet; the
// remaining attack vector — a symlink already in the cache pointing
// out of the tree — is not within the threat model for a personal
// chain-registry mirror.
func ensureWithin(root, target string) error {
	cleanRoot := filepath.Clean(root)
	cleanTarget := filepath.Clean(target)
	if cleanTarget == cleanRoot {
		return nil
	}
	sep := string(os.PathSeparator)
	if !strings.HasPrefix(cleanTarget, cleanRoot+sep) {
		return fmt.Errorf("path %q escapes %q", cleanTarget, cleanRoot)
	}
	return nil
}

