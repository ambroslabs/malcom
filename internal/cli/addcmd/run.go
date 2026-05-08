// Package addcmd is the `malcom add <chain-id>` subcommand: register a
// chain into a previously-initialised malcom tree.
//
// Requires `malcom init` to have run first (config.toml must exist).
// On a first add for a chain it creates:
//
//	$XDG_CONFIG_HOME/malcom/chains/<id>.toml     per-chain config (chain_id, genesis URL, rpcs, peers)
//	$XDG_STATE_HOME/malcom/<id>/node_key.json    cometbft p2p ed25519 identity
//	$XDG_STATE_HOME/malcom/<id>/                 (parent dir for addrbook/banlist/served, populated lazily)
//	$XDG_CACHE_HOME/malcom/<id>/                 ephemeral cache (block snapshots, etc.)
//	$XDG_DATA_HOME/malcom/<id>/                  data dir (genesis lands here lazily)
//
// Unless -offline is passed, add populates the chain's RPC and peer list
// from the cosmos chain-registry. Genesis is NOT downloaded here —
// bootstrap fetches it lazily so add stays fast + network-light.
//
// Subsequent runs are a no-op: existing files are kept. -force
// overwrites chains/<id>.toml (the node key is never overwritten).
package addcmd

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	"github.com/zrbecker/cosmos-p2p/internal/helpers/nodekey"
	"github.com/zrbecker/cosmos-p2p/internal/registry"
)

// chainIDPattern bounds the accepted shape of a <chain-id> argument so
// it can't smuggle path separators or other surprises into chains/<id>.toml
// or the per-chain XDG dirs. Real cosmos chain IDs (cosmoshub-4,
// osmosis-1, neutron-1, gravity-bridge-3, ...) all fit this shape.
var chainIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom add", flag.ContinueOnError)
	force := fs.Bool("force", false, "overwrite existing chains/<id>.toml (node key is never overwritten)")
	offline := fs.Bool("offline", false, "skip cosmos chain-registry fetch; write a blank chain template")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	rest := fs.Args()
	if len(rest) != 1 || rest[0] == "" {
		fmt.Fprintln(os.Stderr, "usage: malcom add [-force] [-offline] <chain-id>")
		fmt.Fprintln(os.Stderr, "note: flags must precede <chain-id> (e.g. `malcom add -offline cosmoshub-4`)")
		return 2
	}
	chain := rest[0]
	if !chainIDPattern.MatchString(chain) {
		fmt.Fprintf(os.Stderr, "invalid chain id %q: must match %s\n", chain, chainIDPattern)
		return 2
	}

	cfgPath, err := config.DefaultConfigPath()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve config path: %v\n", err)
		return 1
	}
	if _, err := os.Stat(cfgPath); err != nil {
		if os.IsNotExist(err) {
			fmt.Fprintf(os.Stderr, "no malcom config at %s — run `malcom init` first\n", cfgPath)
			return 1
		}
		fmt.Fprintf(os.Stderr, "stat %s: %v\n", cfgPath, err)
		return 1
	}
	chainsDir := filepath.Join(filepath.Dir(cfgPath), "chains")

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

	chainStateDir := filepath.Join(stateDir, chain)
	chainCacheDir := filepath.Join(cacheDir, chain)
	chainDataDir := filepath.Join(dataDir, chain)
	nodeKeyPath := filepath.Join(chainStateDir, "node_key.json")
	chainCfgPath := filepath.Join(chainsDir, chain+".toml")

	for _, d := range []string{chainsDir, chainStateDir, chainCacheDir, chainDataDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", d, err)
			return 1
		}
	}

	fmt.Printf("[add] === chain %s ===\n", chain)

	var info *registry.ChainInfo
	if !*offline {
		i, err := registry.Fetch(chain)
		if err != nil {
			if errors.Is(err, registry.ErrNotFound) {
				fmt.Printf("[add] %s: not in cosmos chain-registry — leaving rpcs/peers blank\n", chain)
			} else {
				fmt.Printf("[add] %s: chain-registry fetch failed (%v) — leaving rpcs/peers blank\n", chain, err)
			}
		} else {
			info = i
			fmt.Printf("[add] %s: registry rpcs=%d peers=%d\n",
				chain, len(info.RPCs), len(info.PersistentPeers)+len(info.Seeds))
		}
	}

	defaultGenesis := registry.HardcodedGenesisURLs[chain]
	body := config.ChainTemplate(chain, info, defaultGenesis)
	written, err := writeIfMissing(chainCfgPath, body, *force)
	if err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", chainCfgPath, err)
		return 1
	}
	switch {
	case !written:
		fmt.Printf("[add] chain config exists at %s — keeping (pass -force to overwrite)\n", chainCfgPath)
	case *force:
		fmt.Printf("[add] wrote chain %s (forced)\n", chainCfgPath)
	default:
		fmt.Printf("[add] wrote chain %s\n", chainCfgPath)
	}

	if _, err := os.Stat(nodeKeyPath); err == nil {
		fmt.Printf("[add] %s: node key exists at %s — keeping\n", chain, nodeKeyPath)
	} else {
		if _, err := nodekey.LoadOrGen(nodeKeyPath); err != nil {
			fmt.Fprintf(os.Stderr, "%s: generate node key: %v\n", chain, err)
			return 1
		}
		fmt.Printf("[add] %s: generated node key %s\n", chain, nodeKeyPath)
	}

	fmt.Println()
	fmt.Printf("[add] done. layout for %s:\n", chain)
	fmt.Printf("  chain config: %s\n", chainCfgPath)
	fmt.Printf("  node key:     %s\n", nodeKeyPath)
	fmt.Printf("  state:        %s/\n", chainStateDir)
	fmt.Printf("  cache:        %s/\n", chainCacheDir)
	fmt.Printf("  data:         %s/\n", chainDataDir)
	return 0
}

// writeIfMissing writes body to path. If the file exists and force is
// false, it leaves it alone and returns (false, nil).
func writeIfMissing(path, body string, force bool) (bool, error) {
	if _, err := os.Stat(path); err == nil && !force {
		return false, nil
	}
	return true, os.WriteFile(path, []byte(body), 0o644)
}
