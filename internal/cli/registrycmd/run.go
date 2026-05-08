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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/config"
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
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cacheDir, err := config.RegistryCacheDir()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve registry cache: %v\n", err)
		return 1
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	ctx, cancelTimeout := context.WithTimeout(ctx, registry.DefaultSyncTimeout)
	defer cancelTimeout()

	fmt.Printf("[registry] syncing chain-registry → %s\n", cacheDir)
	t0 := time.Now()
	res, err := registry.Sync(ctx, cacheDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "registry sync: %v\n", err)
		return 1
	}
	fmt.Printf("[registry] done in %s: indexed=%d killed=%d filtered=%d\n",
		time.Since(t0).Truncate(10*time.Millisecond),
		res.IndexedChains, res.DroppedKilled, res.DroppedFiltered)
	if len(res.Collisions) > 0 {
		fmt.Fprintf(os.Stderr, "\n[registry] %d chain_id(s) advertised by multiple live registry entries — excluded from the index:\n",
			len(res.Collisions))
		for _, c := range res.Collisions {
			fmt.Fprintf(os.Stderr, "  %s: %v\n", c.ChainID, c.Paths)
		}
		fmt.Fprintln(os.Stderr, "  (`malcom add <chain-id>` will refuse these until upstream disambiguates.)")
	}
	return 0
}

func usage() {
	fmt.Fprintln(os.Stderr, `malcom registry — manage the cached cosmos chain-registry snapshot

usage: malcom registry <subcommand> [args...]

subcommands:
  refresh    re-fetch the chain-registry tarball into the XDG cache`)
}
