// Package registrycmd is the `malcom registry` subcommand: manage the
// cached cosmos chain-registry snapshot used by `malcom add`.
//
// Subcommands:
//
//	malcom registry refresh   re-fetch the chain-registry tarball into
//	                           $XDG_CACHE_HOME/malcom/chain-registry/
//
// The cache is a flat mirror of upstream's `*/chain.json` files for
// cosmos chains (mainnets, testnets, devnets — status != killed),
// plus an index.json mapping chain_id → relative directory.
package registrycmd

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/registry"
)

// Run is the malcom registry dispatch entry point. args excludes the
// "registry" token; args[0] is the subcommand (refresh / -h / help).
func Run(args []string) int {
	if len(args) < 1 {
		usage()
		return 2
	}
	rest := args[1:]
	switch args[0] {
	case "refresh":
		return runRefresh(rest)
	case "-h", "--help", "help":
		usage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "malcom registry: unknown subcommand %q\n\n", args[0])
		usage()
		return 2
	}
}

func runRefresh(args []string) int {
	fs := flag.NewFlagSet("malcom registry refresh", flag.ContinueOnError)
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return 2
	}
	log := malcomlog.New(malcomlog.Options{
		Writer: os.Stderr, Mode: mode, Level: slog.LevelInfo,
	}).With("module", "registry")

	cacheDir, err := config.RegistryCacheDir()
	if err != nil {
		log.Error("resolve registry cache", "err", err)
		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, cancelTimeout := context.WithTimeout(ctx, registry.DefaultSyncTimeout)
	defer cancelTimeout()

	log.Info("syncing chain-registry", "cache", cacheDir)
	t0 := time.Now()
	res, err := registry.Sync(ctx, cacheDir)
	if err != nil {
		log.Error("registry sync failed", "err", err)
		return 1
	}
	log.Info("done",
		"elapsed", time.Since(t0).Truncate(10*time.Millisecond),
		"indexed", res.IndexedChains,
		"killed", res.DroppedKilled,
		"filtered", res.DroppedFiltered)
	if len(res.Collisions) > 0 {
		log.Warn("chain_id collisions excluded from index — `malcom add` will refuse these until upstream disambiguates",
			"count", len(res.Collisions))
		for _, c := range res.Collisions {
			log.Warn("collision", "chain_id", c.ChainID, "paths", c.Paths)
		}
	}
	return 0
}

func usage() {
	fmt.Fprintln(os.Stderr, `malcom registry — manage the cached cosmos chain-registry snapshot

usage: malcom registry <subcommand> [args...]

subcommands:
  refresh    re-fetch the chain-registry tarball into the XDG cache`)
}
