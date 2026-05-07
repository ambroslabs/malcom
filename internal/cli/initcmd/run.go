// Package initcmd is the `malcom init` subcommand: seed an XDG-compliant
// state tree for every chain malcom currently supports (today: just
// cosmoshub-4).
//
// On a first run it creates:
//
//	$XDG_CONFIG_HOME/malcom/config.toml          shared defaults
//	$XDG_CONFIG_HOME/malcom/chains/<id>.toml     per-chain identity (per supported chain)
//	$XDG_STATE_HOME/malcom/<id>/node_key.json    cometbft p2p ed25519 identity
//	$XDG_STATE_HOME/malcom/<id>/addrbook.json    PEX-managed peer database (built up across runs)
//	$XDG_STATE_HOME/malcom/<id>/deadpeers.json   cross-run dead-peer tombstones
//	$XDG_CACHE_HOME/malcom/<id>/                 ephemeral cache (block snapshots, etc.)
//	$XDG_DATA_HOME/malcom/<id>/                  data dir (genesis lands here lazily)
//
// Unless -offline is passed, init populates each chain's RPC and peer
// list from the cosmos chain-registry. Genesis is NOT downloaded here —
// bootstrap fetches it lazily when needed so init stays fast +
// network-light.
//
// Subsequent runs are idempotent: existing files are kept. -force
// overwrites config.toml and chains/<id>.toml (the node key is never
// overwritten).
package initcmd

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	"github.com/zrbecker/cosmos-p2p/internal/helpers/nodekey"
	"github.com/zrbecker/cosmos-p2p/internal/registry"
)

// supportedChains lists every chain `malcom init` initialises. Order
// matters: the first entry becomes default_chain in a fresh
// config.toml.
var supportedChains = []string{
	"cosmoshub-4",
}

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom init", flag.ContinueOnError)
	force := fs.Bool("force", false, "overwrite existing config.toml and chains/<id>.toml (node key + downloaded genesis are never overwritten)")
	offline := fs.Bool("offline", false, "skip cosmos chain-registry fetch + genesis download; write blank chain templates")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	// ─── resolve XDG roots ──────────────────────────────────────────
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

	// ─── create top-level dirs ──────────────────────────────────────
	for _, d := range []string{configRoot, chainsDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", d, err)
			return 1
		}
	}

	// ─── write global config.toml ───────────────────────────────────
	cfgWritten, err := writeIfMissing(cfgPath, config.GlobalTemplate(supportedChains[0]), *force)
	if err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", cfgPath, err)
		return 1
	}
	reportFile("config", cfgPath, cfgWritten, *force)

	// ─── per-chain setup ────────────────────────────────────────────
	rc := 0
	for _, chain := range supportedChains {
		fmt.Println()
		fmt.Printf("[init] === chain %s ===\n", chain)

		chainStateDir := filepath.Join(stateDir, chain)
		chainCacheDir := filepath.Join(cacheDir, chain)
		chainDataDir := filepath.Join(dataDir, chain)
		nodeKeyPath := filepath.Join(chainStateDir, "node_key.json")
		genesisPath := filepath.Join(chainDataDir, "genesis.json")
		chainCfgPath := filepath.Join(chainsDir, chain+".toml")

		for _, d := range []string{chainStateDir, chainCacheDir, chainDataDir} {
			if err := os.MkdirAll(d, 0o755); err != nil {
				fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", d, err)
				rc = 1
				continue
			}
		}

		// Fetch chain-registry data unless offline.
		var info *registry.ChainInfo
		if !*offline {
			i, err := registry.Fetch(chain)
			if err != nil {
				if errors.Is(err, registry.ErrNotFound) {
					fmt.Printf("[init] %s: not in cosmos chain-registry — leaving rpcs/peers blank\n", chain)
				} else {
					fmt.Printf("[init] %s: chain-registry fetch failed (%v) — leaving rpcs/peers blank\n", chain, err)
				}
			} else {
				info = i
				fmt.Printf("[init] %s: registry rpcs=%d peers=%d\n",
					chain, len(info.RPCs), len(info.PersistentPeers)+len(info.Seeds))
			}
		}

		_ = genesisPath // reserved for bootstrap's lazy download cache

		// Write chains/<id>.toml. Genesis is set to the default URL for
		// the chain (if known); bootstrap downloads it lazily on first
		// run. The user can edit chain.toml to point at a local file or
		// a different URL.
		defaultGenesis := registry.HardcodedGenesisURLs[chain]
		body := config.ChainTemplate(chain, info, defaultGenesis)
		written, err := writeIfMissing(chainCfgPath, body, *force)
		if err != nil {
			fmt.Fprintf(os.Stderr, "write %s: %v\n", chainCfgPath, err)
			rc = 1
		}
		reportFile("chain", chainCfgPath, written, *force)

		// Generate node key (never overwritten).
		if _, err := os.Stat(nodeKeyPath); err == nil {
			fmt.Printf("[init] %s: node key exists at %s — keeping\n", chain, nodeKeyPath)
		} else {
			if _, err := nodekey.LoadOrGen(nodeKeyPath); err != nil {
				fmt.Fprintf(os.Stderr, "%s: generate node key: %v\n", chain, err)
				rc = 1
			} else {
				fmt.Printf("[init] %s: generated node key %s\n", chain, nodeKeyPath)
			}
		}
	}

	fmt.Println()
	fmt.Println("[init] done. layout:")
	fmt.Printf("  config:   %s\n", cfgPath)
	fmt.Printf("  chains:   %s/\n", chainsDir)
	fmt.Printf("  state:    %s/\n", stateDir)
	fmt.Printf("  cache:    %s/\n", cacheDir)
	fmt.Printf("  data:     %s/\n", dataDir)
	return rc
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

func reportFile(label, path string, written, force bool) {
	switch {
	case !written:
		fmt.Printf("[init] %s exists at %s — keeping (pass -force to overwrite)\n", label, path)
	case force:
		fmt.Printf("[init] wrote %s %s (forced)\n", label, path)
	default:
		fmt.Printf("[init] wrote %s %s\n", label, path)
	}
}
