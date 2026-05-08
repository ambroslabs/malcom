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
// Unless -offline is passed, add resolves the chain id against the
// cached cosmos chain-registry snapshot under
// $XDG_CACHE_HOME/malcom/chain-registry/. The cache auto-syncs on
// first add and whenever it's older than registry.DefaultIndexMaxAge.
// `malcom registry refresh` forces a re-sync. Unknown chain ids fail
// with a clear error rather than silently writing a blank or
// mis-attributed config. Genesis is NOT downloaded here — bootstrap
// fetches it lazily so add stays fast.
//
// Subsequent runs are a no-op: existing files are kept. -force
// overwrites chains/<id>.toml (the node key is never overwritten).
package addcmd

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	"github.com/zrbecker/cosmos-p2p/internal/helpers/nodekey"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
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
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
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

	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return 2
	}
	log := malcomlog.New(malcomlog.Options{
		Writer: os.Stderr, Mode: mode, Level: slog.LevelInfo,
	}).With("module", "add", "chain", chain)

	cfgPath, err := config.DefaultConfigPath()
	if err != nil {
		log.Error("resolve config path", "err", err)
		return 1
	}
	if _, err := os.Stat(cfgPath); err != nil {
		if os.IsNotExist(err) {
			log.Error("no malcom config — run `malcom init` first", "path", cfgPath)
			return 1
		}
		log.Error("stat config", "path", cfgPath, "err", err)
		return 1
	}
	chainsDir := filepath.Join(filepath.Dir(cfgPath), "chains")

	stateDir, err := config.StateDir()
	if err != nil {
		log.Error("resolve state dir", "err", err)
		return 1
	}
	cacheDir, err := config.CacheDir()
	if err != nil {
		log.Error("resolve cache dir", "err", err)
		return 1
	}
	dataDir, err := config.DataDir()
	if err != nil {
		log.Error("resolve data dir", "err", err)
		return 1
	}

	chainStateDir := filepath.Join(stateDir, chain)
	chainCacheDir := filepath.Join(cacheDir, chain)
	chainDataDir := filepath.Join(dataDir, chain)
	nodeKeyPath := filepath.Join(chainStateDir, "node_key.json")
	chainCfgPath := filepath.Join(chainsDir, chain+".toml")

	for _, d := range []string{chainsDir, chainStateDir, chainCacheDir, chainDataDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			log.Error("mkdir failed", "dir", d, "err", err)
			return 1
		}
	}

	var info *registry.ChainInfo
	if !*offline {
		regCacheDir, err := config.RegistryCacheDir()
		if err != nil {
			log.Error("resolve registry cache dir", "err", err)
			return 1
		}
		i, lerr := lookupChain(regCacheDir, chain, log)
		if lerr != nil {
			log.Error("registry lookup failed", "err", lerr)
			return 1
		}
		info = i
		log.Info("registry lookup",
			"rpcs", len(info.RPCs),
			"peers", len(info.PersistentPeers)+len(info.Seeds))
	}

	defaultGenesis := registry.HardcodedGenesisURLs[chain]
	body := config.ChainTemplate(chain, info, defaultGenesis)
	written, err := writeIfMissing(chainCfgPath, body, *force)
	if err != nil {
		log.Error("write chain config failed", "path", chainCfgPath, "err", err)
		return 1
	}
	switch {
	case !written:
		log.Info("chain config exists, keeping (pass -force to overwrite)", "path", chainCfgPath)
	case *force:
		log.Info("chain config written (forced)", "path", chainCfgPath)
	default:
		log.Info("chain config written", "path", chainCfgPath)
	}

	if _, err := os.Stat(nodeKeyPath); err == nil {
		log.Info("node key exists, keeping", "path", nodeKeyPath)
	} else {
		if _, err := nodekey.LoadOrGen(nodeKeyPath); err != nil {
			log.Error("generate node key failed", "err", err)
			return 1
		}
		log.Info("generated node key", "path", nodeKeyPath)
	}

	log.Info("layout",
		"chain_config", chainCfgPath,
		"node_key", nodeKeyPath,
		"state", chainStateDir,
		"cache", chainCacheDir,
		"data", chainDataDir)
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

// lookupChain resolves chain via the cached chain-registry index,
// auto-syncing the cache if it's missing or older than
// registry.DefaultIndexMaxAge.
func lookupChain(cacheDir, chain string, log *slog.Logger) (*registry.ChainInfo, error) {
	age, err := registry.IndexAge(cacheDir)
	switch {
	case errors.Is(err, registry.ErrIndexMissing):
		log.Info("no chain-registry cache yet — syncing")
		if err := syncRegistry(cacheDir, log); err != nil {
			return nil, fmt.Errorf("sync chain-registry: %w", err)
		}
	case err != nil:
		return nil, fmt.Errorf("read chain-registry index: %w", err)
	case age > registry.DefaultIndexMaxAge:
		log.Info("chain-registry cache stale — refreshing",
			"age", age.Truncate(time.Minute),
			"hint", "override with `malcom registry refresh`")
		if err := syncRegistry(cacheDir, log); err != nil {
			return nil, fmt.Errorf("sync chain-registry: %w", err)
		}
	}

	info, err := registry.Lookup(cacheDir, chain)
	if err == nil {
		return info, nil
	}
	if errors.Is(err, registry.ErrNotFound) {
		return nil, fmt.Errorf(
			"chain id %q is not in the cached chain-registry index\n"+
				"  - if upstream added it recently, run `malcom registry refresh`\n"+
				"  - if you don't want a registry lookup, re-run with -offline (you'll need to fill in chains/%s.toml by hand)",
			chain, chain)
	}
	return nil, fmt.Errorf("registry lookup: %w", err)
}

// syncRegistry runs registry.Sync against cacheDir, surfacing collisions
// through log. Cancellable via SIGINT/SIGTERM and bounded by
// registry.DefaultSyncTimeout so a hung TCP connection can't wedge
// `malcom add`.
func syncRegistry(cacheDir string, log *slog.Logger) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, cancelTimeout := context.WithTimeout(ctx, registry.DefaultSyncTimeout)
	defer cancelTimeout()
	res, err := registry.Sync(ctx, cacheDir)
	if err != nil {
		return err
	}
	log.Info("chain-registry synced",
		"indexed", res.IndexedChains,
		"killed", res.DroppedKilled,
		"filtered", res.DroppedFiltered)
	if len(res.Collisions) > 0 {
		log.Warn("chain_id collisions excluded from index", "count", len(res.Collisions))
		for _, c := range res.Collisions {
			log.Warn("collision", "chain_id", c.ChainID, "paths", c.Paths)
		}
	}
	return nil
}
