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
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/ambroslabs/malcom/internal/cli/cliexit"
	"github.com/ambroslabs/malcom/internal/config"
	malcomlog "github.com/ambroslabs/malcom/internal/log"
	"github.com/ambroslabs/malcom/internal/snapshotimport"
)

// NewCmd returns the `malcom verify` cobra command.
func NewCmd() *cobra.Command {
	var (
		chain   string
		appdb   string
		height  int64
		rpcURL  string
		logMode string
		debug   bool
	)
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "check the imported AppHash against a cometbft RPC",
		Long:  "Read a CommitInfo from an application.db produced by `malcom snapshot import`, compute the cosmos-sdk MultiStore AppHash, fetch the consensus AppHash from a cometbft RPC, and compare.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(chain, appdb, height, rpcURL, logMode, debug)
		},
	}
	cmd.Flags().StringVar(&chain, "chain", "", "chain id (override; required if appdb meta.json is missing or omits chain_id)")
	cmd.Flags().StringVar(&appdb, "appdb", "", "path to the application.db parent dir (required)")
	cmd.Flags().Int64Var(&height, "height", 0, "height override (required if appdb meta.json is missing or omits height)")
	cmd.Flags().StringVar(&rpcURL, "rpc", "", "cometbft RPC endpoint (defaults to chains/<id>.toml rpcs; tried first-success)")
	cmd.Flags().StringVar(&logMode, "log", "", "log output: auto (default), pretty, text, json")
	cmd.Flags().BoolVar(&debug, "debug", false, "verbose logging")
	return cmd
}

func run(chain, appdb string, height int64, rpcURL, logMode string, debug bool) error {
	if appdb == "" {
		fmt.Fprintln(os.Stderr, "required: --appdb <dir>")
		return &cliexit.Error{Code: 2}
	}

	mode, ok := malcomlog.ParseMode(logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid --log %q (want auto/pretty/text/json)\n", logMode)
		return &cliexit.Error{Code: 2}
	}

	// Read meta.json from the appdb dir if present. chain id and
	// height are normally drawn from here so the user doesn't have to
	// repeat themselves; the --chain / --height flags only kick in when
	// meta.json is missing or pre-dates the field, in which case they
	// are required overrides. Any flag value that's set must match.
	appdbMeta, metaErr := snapshotimport.ReadAppDBMeta(appdb)
	switch {
	case metaErr != nil && !os.IsNotExist(metaErr):
		fmt.Fprintf(os.Stderr, "read appdb meta: %v\n", metaErr)
		return &cliexit.Error{Code: 1}
	case metaErr != nil:
		if chain == "" || height == 0 {
			fmt.Fprintf(os.Stderr, "no meta.json in %s; pass --chain and --height to override\n", appdb)
			return &cliexit.Error{Code: 2}
		}
	default:
		if appdbMeta.ChainID != "" && chain != "" && appdbMeta.ChainID != chain {
			fmt.Fprintf(os.Stderr, "meta.json chain_id %q does not match --chain %q\n", appdbMeta.ChainID, chain)
			return &cliexit.Error{Code: 1}
		}
		if appdbMeta.Height != 0 && height != 0 && appdbMeta.Height != height {
			fmt.Fprintf(os.Stderr, "meta.json height %d does not match --height %d\n", appdbMeta.Height, height)
			return &cliexit.Error{Code: 1}
		}
		if chain == "" {
			if appdbMeta.ChainID == "" {
				fmt.Fprintln(os.Stderr, "meta.json has no chain_id; pass --chain to override")
				return &cliexit.Error{Code: 2}
			}
			chain = appdbMeta.ChainID
		}
		if height == 0 {
			if appdbMeta.Height == 0 {
				fmt.Fprintln(os.Stderr, "meta.json has no height; pass --height to override")
				return &cliexit.Error{Code: 2}
			}
			height = appdbMeta.Height
		}
	}

	// Pull [log] from config when available; verify is also runnable
	// with --rpc and no config, in which case fall back to defaults.
	var logTuning malcomlog.Tuning
	var ch config.Chain
	cfg, cfgErr := config.Load()
	if cfgErr == nil {
		if resolved, err := cfg.Resolve(chain); err == nil {
			ch = resolved
			logTuning = malcomlog.Tuning{Level: ch.Log.Level, Modules: ch.Log.Modules}
		} else {
			logTuning = malcomlog.Tuning{Level: cfg.Log.Level, Modules: cfg.Log.Modules}
		}
	}
	logOpts, err := malcomlog.BuildOptions(logTuning, mode, debug, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log config: %v\n", err)
		return &cliexit.Error{Code: 2}
	}
	log := malcomlog.New(logOpts).With("module", "verify")

	// Build the RPC list: --rpc flag wins outright; otherwise use the
	// chain's configured rpcs. Either path must yield at least one URL.
	var rpcs []string
	if rpcURL != "" {
		rpcs = []string{rpcURL}
	} else if ch.ChainID != "" {
		rpcs = ch.RPCs
	}
	if len(rpcs) == 0 {
		log.Error("no rpc; pass --rpc or set chains.<id>.rpcs in config", "chain", chain)
		return &cliexit.Error{Code: 1}
	}

	log.Info("starting", "appdb", appdb, "height", height, "rpcs", len(rpcs))

	res, err := CheckAppHash(appdb, height, rpcs, log)
	switch {
	case errors.Is(err, ErrMismatch):
		log.Error("MISMATCH",
			"height", res.Height,
			"local", fmt.Sprintf("%X", res.LocalHash),
			"consensus", fmt.Sprintf("%X", res.ConsensusHash),
			"rpc", res.UsedRPC)
		return &cliexit.Error{Code: 1}
	case err != nil:
		log.Error("verify failed", "err", err)
		return &cliexit.Error{Code: 1}
	}
	log.Info("MATCH — application.db is consensus-correct",
		"height", res.Height,
		"apphash", fmt.Sprintf("%X", res.LocalHash),
		"rpc", res.UsedRPC)
	return nil
}
