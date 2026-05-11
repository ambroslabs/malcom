// `malcom heal` lives in the bootstrap package because it shares the
// chain-binary subprocess primitives (findBinary, probeStatesyncSubcommand,
// runDaemon) with `malcom bootstrap`. From the operator's perspective
// it's a separate top-level subcommand wired in cmd/malcom/main.go.
//
// Why this exists: cometbft consumes its OfflineStateSyncHeight signal
// on the first `<binary> start`, then resets it to 0 *before* any
// block lands in the blockstore. If that first start fails after
// NewNode but before block sync makes progress (config error, peer
// auth glitch, supervisor SIGKILL — any of these), the home is
// permanently bricked with a `state and store height mismatch` panic
// on every subsequent start. malcom owns the bootstrap step so it
// should own the recovery story. See #95.

package bootstrap

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/registry"
)

// RunHeal is the entry point for `malcom heal`.
func RunHeal(args []string) int {
	fs := flag.NewFlagSet("malcom heal", flag.ContinueOnError)
	home := fs.String("home", "", "chain home dir to heal (the panicking gaia home; required)")
	chain := fs.String("chain", "", "chain id; only consulted for binary lookup. Default: read from <home>/.malcom-bootstrap-height's neighbouring config/genesis.json, or fall through to -binary.")
	heightFlag := fs.Int64("height", 0, "bootstrap height override. Default: read from <home>/.malcom-bootstrap-height (written by `malcom bootstrap` since #95).")
	binary := fs.String("binary", "", "path to chain binary; default = $PATH lookup of daemon_name from chain-registry")
	yes := fs.Bool("yes", false, "skip the interactive confirmation. heal removes blockstore.db, state.db, and evidence.db under <home>/data/ before re-running `bootstrap-state`; without -yes the run is dry (prints the plan and exits with usage).")
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	debug := fs.Bool("debug", false, "verbose logging")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *home == "" {
		fmt.Fprintln(os.Stderr, "required: -home <dir>")
		fs.Usage()
		return 2
	}

	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return 2
	}

	// Resolve the height: marker file first, -height override second.
	// Flag value if set must match the marker; a mismatch is more
	// likely to be operator error (wrong -height for this home) than
	// a deliberate override.
	height := *heightFlag
	markerHeight, markerErr := ReadHeightMarker(*home)
	switch {
	case markerErr == nil:
		if height != 0 && height != markerHeight {
			fmt.Fprintf(os.Stderr,
				"marker says height %d but -height %d was passed; "+
					"resolve the discrepancy (or delete %s to force -height)\n",
				markerHeight, height, filepath.Join(*home, BootstrapHeightMarker))
			return 2
		}
		height = markerHeight
	case markerErr != nil && height == 0:
		// No marker and no -height; we have no signal to pass to
		// bootstrap-state. Print both possibilities so the operator
		// can pick the right fix.
		fmt.Fprintf(os.Stderr,
			"no bootstrap-height marker and no -height flag: %v\n"+
				"  fix: pass -height <H> where H is the original `malcom bootstrap` height\n",
			markerErr)
		return 2
	}

	// Build the logger from chain config when we have one, fall back
	// to defaults when -chain isn't set (recovery from a half-built
	// home shouldn't be gated on having a usable malcom config).
	var (
		logTuning malcomlog.Tuning
		ch        config.Chain
		regInfo   *registry.ChainInfo
	)
	if *chain != "" {
		if cfg, err := config.Load(); err == nil {
			if resolved, err := cfg.Resolve(*chain); err == nil {
				ch = resolved
				logTuning = malcomlog.Tuning{Level: ch.Log.Level, Modules: ch.Log.Modules}
				if cacheDir, err := config.RegistryCacheDir(); err == nil {
					if rinfo, err := registry.Lookup(cacheDir, ch.ChainID); err == nil {
						regInfo = rinfo
					}
				}
			} else {
				logTuning = malcomlog.Tuning{Level: cfg.Log.Level, Modules: cfg.Log.Modules}
			}
		}
	}
	logOpts, err := malcomlog.BuildOptions(logTuning, mode, *debug, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log config: %v\n", err)
		return 2
	}
	log := malcomlog.New(logOpts).With("module", "heal")

	// Locate the chain binary. `daemon_name` from the chain-registry
	// is the usual source; -binary wins outright when set.
	daemon := ""
	if regInfo != nil {
		daemon = regInfo.DaemonName
	}
	binPath, err := findBinary(*binary, daemon)
	if err != nil {
		log.Error("locate chain binary", "err", err)
		return 1
	}
	log.Info("chain binary", "path", binPath)

	// Probe for the right statesync subcommand path.
	ctx := context.Background()
	sub, err := probeStatesyncSubcommand(ctx, binPath)
	if err != nil {
		log.Error("probe bootstrap-state subcommand", "err", err)
		return 1
	}

	dataDir := filepath.Join(*home, "data")
	victims := []string{
		filepath.Join(dataDir, "blockstore.db"),
		filepath.Join(dataDir, "state.db"),
		filepath.Join(dataDir, "evidence.db"),
	}

	// Detect the panic precondition for an informative log line
	// before either dry-running or removing anything. The signature
	// from the issue is `state (H) and store (0) height mismatch` —
	// in filesystem terms, blockstore.db is empty/missing while
	// state.db has been populated. We don't open the dbs here (that
	// would risk read locks fighting a running gaiad); just confirm
	// the dirs exist as a sanity check.
	for _, p := range victims {
		if _, err := os.Stat(p); err != nil {
			log.Warn("victim dir not present; heal will create it from scratch via bootstrap-state",
				"path", p, "err", err)
		}
	}

	log.Info("heal plan",
		"home", *home,
		"height", height,
		"will_remove", victims,
		"will_run", fmt.Sprintf("%s %s bootstrap-state --home %s --height %d", binPath, sub, *home, height))

	if !*yes {
		log.Info("dry-run (no -yes); rerun with -yes to execute")
		log.Info("operator pre-flight checklist",
			"step_1", "stop gaiad / systemctl stop <unit>",
			"step_2", "verify no other process holds locks on the above paths",
			"step_3", "rerun this command with -yes")
		return 0
	}

	// Belt-and-braces: refuse to operate on a home that still has an
	// active process holding locks. lsof / fuser checks are platform-
	// specific and not bulletproof; we leave this to the operator's
	// pre-flight (step_2 above). If the dbs are open, the rm will
	// fail and we surface that to the operator.
	for _, p := range victims {
		if err := os.RemoveAll(p); err != nil {
			log.Error("remove failed; stop gaiad first then retry", "path", p, "err", err)
			return 1
		}
		log.Info("removed", "path", p)
	}

	// Re-apply bootstrap-state. This populates state.db with
	// LastBlockHeight == height and writes OfflineStateSyncHeight,
	// putting the home back in the freshly-bootstrapped state.
	bsArgs := []string{sub, "bootstrap-state", "--home", *home, "--height", fmt.Sprintf("%d", height)}
	if err := runDaemon(ctx, log, binPath, bsArgs...); err != nil {
		log.Error("bootstrap-state failed", "err", err)
		return 1
	}

	// Refresh the marker (height didn't change, but the timestamp
	// will — useful breadcrumb for operators noticing repeat heals).
	if err := WriteHeightMarker(*home, height); err != nil {
		log.Warn("refresh bootstrap-height marker failed", "err", err)
	}

	log.Info("healed — start gaiad now",
		"home", *home,
		"height", height,
		"start_command", fmt.Sprintf("%s start --home %s", binPath, *home))
	return 0
}
