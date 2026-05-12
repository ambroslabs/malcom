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
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/ambroslabs/malcom/internal/cli/cliexit"
	"github.com/ambroslabs/malcom/internal/config"
	malcomlog "github.com/ambroslabs/malcom/internal/log"
	"github.com/ambroslabs/malcom/internal/registry"
)

// NewCmd returns the `malcom registry` cobra command group.
func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "registry",
		Short: "manage the cached cosmos chain-registry snapshot",
	}
	cmd.AddCommand(newRefreshCmd())
	return cmd
}

func newRefreshCmd() *cobra.Command {
	var logMode string
	cmd := &cobra.Command{
		Use:   "refresh",
		Short: "re-fetch the chain-registry tarball into the XDG cache",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRefresh(logMode)
		},
	}
	cmd.Flags().StringVar(&logMode, "log", "", "log output: auto (default), pretty, text, json")
	return cmd
}

func runRefresh(logMode string) error {
	mode, ok := malcomlog.ParseMode(logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid --log %q (want auto/pretty/text/json)\n", logMode)
		return &cliexit.Error{Code: 2}
	}
	log := malcomlog.New(malcomlog.Options{
		Writer: os.Stderr, Mode: mode, Level: slog.LevelInfo,
	}).With("module", "registry")

	cacheDir, err := config.RegistryCacheDir()
	if err != nil {
		log.Error("resolve registry cache", "err", err)
		return &cliexit.Error{Code: 1}
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
		return &cliexit.Error{Code: 1}
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
	return nil
}
