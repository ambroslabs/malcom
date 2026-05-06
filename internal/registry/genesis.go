// Hardcoded genesis URLs + downloader.
//
// chain-registry's codebase.genesis.genesis_url is in principle
// authoritative for every chain, but in practice the URL points
// off to per-chain repos that vary in stability and gzip status.
// To keep `malcom init` from silently producing a broken setup, we
// only auto-download for chains that have a known-good URL listed
// here. New chains start with `genesis = ""` and a comment pointing
// at chain-registry; the user fills it in or we add another entry.

package registry

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// HardcodedGenesisURLs maps chain_id → known-good genesis URL. The
// URL may point at a `.json` or `.json.gz`; DownloadGenesis sniffs
// by suffix and gunzips on the fly.
var HardcodedGenesisURLs = map[string]string{
	"cosmoshub-4": "https://github.com/cosmos/mainnet/raw/master/genesis/genesis.cosmoshub-4.json.gz",
}

// DownloadGenesis fetches url, decompresses it if the URL ends in
// .gz, and writes the result to destPath. Idempotent: if destPath
// exists and is non-empty, it's left alone (assumed correct from a
// prior run).
func DownloadGenesis(url, destPath string) error {
	if info, err := os.Stat(destPath); err == nil && info.Size() > 0 {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(destPath), err)
	}

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("GET %s: HTTP %d: %s", url, resp.StatusCode, body)
	}

	var src io.Reader = resp.Body
	if strings.HasSuffix(url, ".gz") {
		gz, err := gzip.NewReader(resp.Body)
		if err != nil {
			return fmt.Errorf("gunzip %s: %w", url, err)
		}
		defer gz.Close()
		src = gz
	}

	// Write via temp file + rename so a partial download doesn't
	// leave a half-written genesis.json behind.
	tmp, err := os.CreateTemp(filepath.Dir(destPath), ".genesis-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err := io.Copy(tmp, src); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write %s: %w", tmpPath, err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close %s: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, destPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename to %s: %w", destPath, err)
	}
	return nil
}
