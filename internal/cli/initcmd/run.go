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
	"log/slog"
	"os"
	"path/filepath"

	"github.com/ambroslabs/malcom/internal/config"
	malcomlog "github.com/ambroslabs/malcom/internal/log"
)

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom init", flag.ContinueOnError)
	force := fs.Bool("force", false, "overwrite existing config.toml")
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
	}).With("module", "init")

	cfgPath, err := config.DefaultConfigPath()
	if err != nil {
		log.Error("resolve config path", "err", err)
		return 1
	}
	configRoot := filepath.Dir(cfgPath)
	chainsDir := filepath.Join(configRoot, "chains")

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

	for _, d := range []string{configRoot, chainsDir, stateDir, cacheDir, dataDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			log.Error("mkdir failed", "dir", d, "err", err)
			return 1
		}
	}

	cfgWritten, err := writeIfMissing(cfgPath, config.GlobalTemplate(), *force)
	if err != nil {
		log.Error("write config failed", "path", cfgPath, "err", err)
		return 1
	}
	switch {
	case !cfgWritten:
		log.Info("config exists, keeping (pass -force to overwrite)", "path", cfgPath)
	case *force:
		log.Info("config written (forced)", "path", cfgPath)
	default:
		log.Info("config written", "path", cfgPath)
	}

	log.Info("layout",
		"config", cfgPath,
		"chains", chainsDir,
		"state", stateDir,
		"cache", cacheDir,
		"data", dataDir)
	log.Info("done — add a chain with `malcom add <chain-id>` (e.g. cosmoshub-4)")
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
