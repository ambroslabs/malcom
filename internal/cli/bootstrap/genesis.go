// Lazy genesis resolution. The chain.toml `genesis` field is either a
// URL or a local filesystem path; this resolver normalises it to an
// on-disk path that the rest of bootstrap can read.

package bootstrap

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	"github.com/zrbecker/cosmos-p2p/internal/registry"
)

// resolveGenesis returns a local path to the chain's genesis.json,
// downloading from a URL into $XDG_DATA_HOME/malcom/<chain>/genesis.json
// if the chain.toml value is an http(s) URL. Idempotent: a previous
// download is reused.
func resolveGenesis(ch config.Chain) (string, error) {
	g := ch.Genesis
	if g == "" {
		return "", fmt.Errorf("genesis is empty")
	}

	if config.IsGenesisURL(g) {
		dataDir, err := config.DataDir()
		if err != nil {
			return "", fmt.Errorf("resolve data dir: %w", err)
		}
		dest := filepath.Join(dataDir, ch.ChainID, "genesis.json")
		fmt.Printf("[bootstrap] genesis  source=%s\n", g)
		if err := registry.DownloadGenesis(g, dest); err != nil {
			return "", fmt.Errorf("download genesis: %w", err)
		}
		if info, err := os.Stat(dest); err == nil {
			fmt.Printf("[bootstrap] genesis  cached at %s (%d bytes)\n", dest, info.Size())
		}
		return dest, nil
	}

	if _, err := os.Stat(g); err != nil {
		return "", fmt.Errorf("genesis %s: %w", g, err)
	}
	return g, nil
}
