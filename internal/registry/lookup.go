package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// chainIndex is the on-disk shape of $cache/index.json.
type chainIndex struct {
	Version   int               `json:"version"`
	FetchedAt time.Time         `json:"fetched_at"`
	Chains    map[string]string `json:"chains"` // chain_id → registry-relative directory
}

// ErrIndexMissing means the cache's index.json is absent. Callers
// should call Sync to populate the cache before retrying.
var ErrIndexMissing = errors.New("chain-registry index missing")

// ErrNotFound means chain_id is not in the cached index. Either the
// id is wrong, or upstream added it after the last Sync.
var ErrNotFound = errors.New("chain id not in cached chain-registry index")

// IndexAge returns how long ago the cached index was fetched.
// Returns ErrIndexMissing if the index file is absent. Use this to
// decide whether to call Sync on the auto-refresh path.
func IndexAge(cacheDir string) (time.Duration, error) {
	idx, err := readIndex(cacheDir)
	if err != nil {
		return 0, err
	}
	return time.Since(idx.FetchedAt), nil
}

// Lookup returns ChainInfo for chainID, reading the cached index and
// the per-chain chain.json. The cached chain.json's chain_id field is
// verified against the lookup key — a mismatch means the cache is
// corrupt and the caller is told to re-run `malcom registry refresh`.
func Lookup(cacheDir, chainID string) (*ChainInfo, error) {
	idx, err := readIndex(cacheDir)
	if err != nil {
		return nil, err
	}
	rel, ok := idx.Chains[chainID]
	if !ok {
		return nil, fmt.Errorf("%s: %w", chainID, ErrNotFound)
	}
	chainPath := filepath.Join(cacheDir, filepath.FromSlash(rel), chainJSONName)
	body, err := os.ReadFile(chainPath)
	if err != nil {
		return nil, fmt.Errorf("read %s (cache may be corrupt — run `malcom registry refresh`): %w", chainPath, err)
	}
	var raw chainJSON
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("parse %s: %w", chainPath, err)
	}
	if raw.ChainID != chainID {
		return nil, fmt.Errorf(
			"cached %s declares chain_id %q but index points there for %q (run `malcom registry refresh`)",
			chainPath, raw.ChainID, chainID)
	}
	return chainInfoFromRaw(&raw), nil
}

// readIndex loads index.json from cacheDir. Translates absent file
// into ErrIndexMissing so callers can distinguish "needs sync" from
// "real I/O error."
func readIndex(cacheDir string) (*chainIndex, error) {
	p := filepath.Join(cacheDir, indexFilename)
	body, err := os.ReadFile(p)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrIndexMissing
		}
		return nil, err
	}
	var idx chainIndex
	if err := json.Unmarshal(body, &idx); err != nil {
		return nil, fmt.Errorf("parse %s (run `malcom registry refresh`): %w", p, err)
	}
	if idx.Version != indexVersion {
		return nil, fmt.Errorf("index %s is version %d, expected %d (run `malcom registry refresh`)",
			p, idx.Version, indexVersion)
	}
	return &idx, nil
}
