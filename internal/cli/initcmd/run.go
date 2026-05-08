// Package initcmd is the `malcom init` subcommand: seed an XDG-compliant
// state tree with malcom's shared defaults.
//
// On a first run it creates:
//
//	$XDG_CONFIG_HOME/malcom/config.toml          shared defaults: [fetch], [import], [bootstrap]
//	$XDG_CONFIG_HOME/malcom/chains/              per-chain configs land here (one file per chain)
//	$XDG_STATE_HOME/malcom/                      per-chain state (node keys, addrbooks, banlists)
//	$XDG_CACHE_HOME/malcom/                      ephemeral cache
//	$XDG_DATA_HOME/malcom/                       data dir (genesis, etc.)
//
// init does not register any chains — it only sets up the tree.
// Add a chain with `malcom add <chain-id>` (e.g. `malcom add cosmoshub-4`).
//
// Subsequent runs are idempotent: existing files are kept. -force
// overwrites config.toml.
package initcmd

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/zrbecker/cosmos-p2p/internal/config"
)

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom init", flag.ContinueOnError)
	force := fs.Bool("force", false, "overwrite existing config.toml")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfgPath, err := config.DefaultConfigPath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve config path: %v\n", err)
		return 1
	}
	configRoot := filepath.Dir(cfgPath)
	chainsDir := filepath.Join(configRoot, "chains")

	stateDir, err := config.StateDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve state dir: %v\n", err)
		return 1
	}
	cacheDir, err := config.CacheDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve cache dir: %v\n", err)
		return 1
	}
	dataDir, err := config.DataDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve data dir: %v\n", err)
		return 1
	}

	for _, d := range []string{configRoot, chainsDir, stateDir, cacheDir, dataDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", d, err)
			return 1
		}
	}

	cfgWritten, err := writeIfMissing(cfgPath, config.GlobalTemplate(), *force)
	if err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", cfgPath, err)
		return 1
	}
	switch {
	case !cfgWritten:
		fmt.Printf("[init] config exists at %s — keeping (pass -force to overwrite)\n", cfgPath)
	case *force:
		fmt.Printf("[init] wrote config %s (forced)\n", cfgPath)
	default:
		fmt.Printf("[init] wrote config %s\n", cfgPath)
	}

	fmt.Println()
	fmt.Println("[init] done. layout:")
	fmt.Printf("  config:   %s\n", cfgPath)
	fmt.Printf("  chains:   %s/\n", chainsDir)
	fmt.Printf("  state:    %s/\n", stateDir)
	fmt.Printf("  cache:    %s/\n", cacheDir)
	fmt.Printf("  data:     %s/\n", dataDir)
	fmt.Println()
	fmt.Println("[init] add a chain with: malcom add <chain-id>  (e.g. malcom add cosmoshub-4)")
	return 0
}

// writeIfMissing writes body to path. If the file exists and force is
// false, it leaves it alone and returns (false, nil). On a successful
// write, returns (true, nil).
func writeIfMissing(path, body string, force bool) (bool, error) {
	if _, err := os.Stat(path); err == nil && !force {
		return false, nil
	}
	return true, os.WriteFile(path, []byte(body), 0o644)
}
