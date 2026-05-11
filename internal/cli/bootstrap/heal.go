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
	"strings"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/registry"
)

// resolveHealHeight reconciles the marker file and the -height flag.
// Pure (no logger, no os.Exit) so the height-resolution policy is
// unit-testable without setting up the rest of the CLI surface.
//
// Returns:
//   - resolved height, no error: caller proceeds to the heal
//   - 0, error: caller prints err.Error() to stderr and exits 2.
//
// Policy:
//   - marker present + flag zero        → use marker
//   - marker present + flag matches     → use marker (equivalent)
//   - marker present + flag differs     → error (operator confusion)
//   - marker missing + flag set         → use flag (no marker is OK
//     if the operator knows what they're doing; the bootstrap may
//     pre-date the marker, or the file may have been lost)
//   - marker missing + flag zero        → error with usage hint
func resolveHealHeight(home string, flagHeight int64) (int64, error) {
	markerHeight, markerErr := ReadHeightMarker(home)
	switch {
	case markerErr == nil:
		if flagHeight != 0 && flagHeight != markerHeight {
			return 0, fmt.Errorf(
				"marker says height %d but -height %d was passed; "+
					"resolve the discrepancy (or delete %s to force -height)",
				markerHeight, flagHeight,
				filepath.Join(home, BootstrapHeightMarker))
		}
		return markerHeight, nil
	case flagHeight != 0:
		return flagHeight, nil
	default:
		return 0, fmt.Errorf(
			"no bootstrap-height marker and no -height flag: %v\n"+
				"  fix: pass -height <H> where H is the original `malcom bootstrap` height",
			markerErr)
	}
}

// blockstoreLooksPopulated reports whether <dataDir>/blockstore.db
// has been written to past initial pebble setup. A bricked home
// looks like: state.db populated, blockstore.db missing OR present
// with only the pebble init files (CURRENT, MANIFEST, OPTIONS, no
// .sst). A healthy synced home has .sst files.
//
// Heuristic — not opening pebble — to avoid lock contention if
// gaiad is still running. The operator pre-flight is supposed to
// have stopped gaiad already; this check is defence-in-depth
// against running `heal -yes` on a home the operator forgot is
// actively syncing.
//
// Conservative direction: returns false on read errors (missing dir,
// permission denied, etc.) so the recovery path stays open in the
// expected bricked state. The caller is responsible for separately
// validating the home is actually a chain home (config/genesis.json
// present); we shouldn't be operating on /tmp/typo regardless of
// what this returns.
func blockstoreLooksPopulated(dataDir string) bool {
	entries, err := os.ReadDir(filepath.Join(dataDir, "blockstore.db"))
	if err != nil {
		return false
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sst") {
			return true
		}
	}
	return false
}

// RunHeal is the entry point for `malcom heal`.
func RunHeal(args []string) int {
	fs := flag.NewFlagSet("malcom heal", flag.ContinueOnError)
	home := fs.String("home", "", "chain home dir to heal (the panicking gaia home; required)")
	chain := fs.String("chain", "", "chain id; only consulted for chain-registry binary lookup. Default: rely on -binary or $PATH lookup of daemon_name.")
	heightFlag := fs.Int64("height", 0, "bootstrap height override. Default: read from <home>/.malcom-bootstrap-height (written by `malcom bootstrap` since #95).")
	binary := fs.String("binary", "", "path to chain binary; default = $PATH lookup of daemon_name from chain-registry")
	yes := fs.Bool("yes", false, "skip the dry-run. heal removes blockstore.db, state.db, and evidence.db under <home>/data/ before re-running `bootstrap-state`; without -yes the run is dry (prints the plan and exits with usage).")
	force := fs.Bool("force", false, "proceed even when <home>/data/blockstore.db has data (blocks already synced). Default behaviour refuses, to protect operators who run heal on a home that isn't actually bricked.")
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

	// Sanity-check the path actually looks like a chain home before
	// doing anything destructive. A typo in -home would otherwise
	// proceed all the way through marker resolution and `gaiad
	// bootstrap-state` invocation before failing with a less
	// obvious error.
	if _, err := os.Stat(filepath.Join(*home, "config", "genesis.json")); err != nil {
		fmt.Fprintf(os.Stderr,
			"home %s doesn't contain config/genesis.json — is this really a chain home? (%v)\n",
			*home, err)
		return 2
	}

	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return 2
	}

	// Resolve the height. Pure helper so the policy is unit-testable
	// (see heal_test.go).
	height, err := resolveHealHeight(*home, *heightFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	// Build the logger from chain config when we have one, fall back
	// to defaults when -chain isn't set (recovery from a half-built
	// home shouldn't be gated on having a usable malcom config).
	var (
		logTuning malcomlog.Tuning
		regInfo   *registry.ChainInfo
	)
	if *chain != "" {
		if cfg, err := config.Load(); err == nil {
			if resolved, err := cfg.Resolve(*chain); err == nil {
				logTuning = malcomlog.Tuning{Level: resolved.Log.Level, Modules: resolved.Log.Modules}
				if cacheDir, err := config.RegistryCacheDir(); err == nil {
					if rinfo, err := registry.Lookup(cacheDir, resolved.ChainID); err == nil {
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

	// Footgun guard: refuse to nuke a populated blockstore unless
	// the operator opts in with -force. heal exists to recover a
	// home where the first start failed *before* any block landed;
	// running it against a home with synced blocks would silently
	// discard them. This check is heuristic (fs-only, doesn't open
	// pebble — see the comment on blockstoreLooksPopulated) but
	// catches the common case where an operator runs heal "just to
	// be safe" on a working home.
	if blockstoreLooksPopulated(dataDir) && !*force {
		log.Error("refusing to heal: blockstore.db contains .sst files (home appears healthy, not bricked)",
			"path", filepath.Join(dataDir, "blockstore.db"),
			"hint", "if you really mean to discard those blocks, re-run with -force")
		return 1
	}

	// Informational: a missing dir isn't an error — bootstrap-state
	// will recreate state.db / evidence.db, and a healthy bricked
	// state often has no blockstore.db at all (cometbft hadn't got
	// far enough to create one before crashing).
	for _, p := range victims {
		if _, err := os.Stat(p); err != nil {
			log.Debug("victim dir not present; heal will create it from scratch via bootstrap-state",
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
