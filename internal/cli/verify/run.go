// Package verify is the `malcom verify` subcommand: read a CommitInfo
// from an application.db produced by `malcom snapshot import`, compute
// the cosmos-sdk MultiStore AppHash, fetch the consensus AppHash from a
// cometbft RPC, and compare.
//
// The actual checking lives in verify.go (exported as CheckAppHash) so
// snapshotfetch and snapshotimport can chain into it after their
// pipelines complete. This file is the thin CLI: parse flags, resolve
// chain config, hand off.
//
// Pebble-only — the goleveldb path was dropped along with the
// internal/snapshotappdb importer that depended on github.com/cosmos/iavl.
package verify

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotimport"
)

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom verify", flag.ContinueOnError)
	chain := fs.String("chain", "", "chain id (override; required if appdb meta.json is missing or omits chain_id)")
	appdb := fs.String("appdb", "", "path to the application.db parent dir (required)")
	height := fs.Int64("height", 0, "height override (required if appdb meta.json is missing or omits height)")
	rpcURL := fs.String("rpc", "", "cometbft RPC endpoint (defaults to chains/<id>.toml rpcs; tried first-success)")
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	debug := fs.Bool("debug", false, "verbose logging")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *appdb == "" {
		fs.Usage()
		return 2
	}

	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return 2
	}

	// Read meta.json from the appdb dir if present. chain id and
	// height are normally drawn from here so the user doesn't have to
	// repeat themselves; the -chain / -height flags only kick in when
	// meta.json is missing or pre-dates the field, in which case they
	// are required overrides. Any flag value that's set must match.
	appdbMeta, metaErr := snapshotimport.ReadAppDBMeta(*appdb)
	switch {
	case metaErr != nil && !os.IsNotExist(metaErr):
		fmt.Fprintf(os.Stderr, "read appdb meta: %v\n", metaErr)
		return 1
	case metaErr != nil:
		if *chain == "" || *height == 0 {
			fmt.Fprintf(os.Stderr, "no meta.json in %s; pass -chain and -height to override\n", *appdb)
			return 2
		}
	default:
		if appdbMeta.ChainID != "" && *chain != "" && appdbMeta.ChainID != *chain {
			fmt.Fprintf(os.Stderr, "meta.json chain_id %q does not match -chain %q\n", appdbMeta.ChainID, *chain)
			return 1
		}
		if appdbMeta.Height != 0 && *height != 0 && appdbMeta.Height != *height {
			fmt.Fprintf(os.Stderr, "meta.json height %d does not match -height %d\n", appdbMeta.Height, *height)
			return 1
		}
		if *chain == "" {
			if appdbMeta.ChainID == "" {
				fmt.Fprintln(os.Stderr, "meta.json has no chain_id; pass -chain to override")
				return 2
			}
			*chain = appdbMeta.ChainID
		}
		if *height == 0 {
			if appdbMeta.Height == 0 {
				fmt.Fprintln(os.Stderr, "meta.json has no height; pass -height to override")
				return 2
			}
			*height = appdbMeta.Height
		}
	}

	// Pull [log] from config when available; verify is also runnable
	// with -rpc and no config, in which case fall back to defaults.
	var logTuning malcomlog.Tuning
	var ch config.Chain
	cfg, cfgErr := config.Load()
	if cfgErr == nil {
		if resolved, err := cfg.Resolve(*chain); err == nil {
			ch = resolved
			logTuning = malcomlog.Tuning{Level: ch.Log.Level, Modules: ch.Log.Modules}
		} else {
			logTuning = malcomlog.Tuning{Level: cfg.Log.Level, Modules: cfg.Log.Modules}
		}
	}
	logOpts, err := malcomlog.BuildOptions(logTuning, mode, *debug, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log config: %v\n", err)
		return 2
	}
	log := malcomlog.New(logOpts).With("module", "verify")

	// Build the RPC list: -rpc flag wins outright; otherwise use the
	// chain's configured rpcs. Either path must yield at least one URL.
	var rpcs []string
	if *rpcURL != "" {
		rpcs = []string{*rpcURL}
	} else if ch.ChainID != "" {
		rpcs = ch.RPCs
	}
	if len(rpcs) == 0 {
		log.Error("no rpc; pass -rpc or set chains.<id>.rpcs in config", "chain", *chain)
		return 1
	}

	log.Info("starting", "appdb", *appdb, "height", *height, "rpcs", len(rpcs))

	res, err := CheckAppHash(*appdb, *height, rpcs, log)
	switch {
	case errors.Is(err, ErrMismatch):
		log.Error("MISMATCH",
			"height", res.Height,
			"local", fmt.Sprintf("%X", res.LocalHash),
			"consensus", fmt.Sprintf("%X", res.ConsensusHash),
			"rpc", res.UsedRPC)
		return 1
	case err != nil:
		log.Error("verify failed", "err", err)
		return 1
	}
	log.Info("MATCH — application.db is consensus-correct",
		"height", res.Height,
		"apphash", fmt.Sprintf("%X", res.LocalHash),
		"rpc", res.UsedRPC)
	return 0
}
