// Package snapshotserve is the `malcom snapshot serve` subcommand:
// run a wire-only state-sync server backed by one or more locally-fetched
// snapshot directories.
//
// The server speaks the cometbft state-sync wire protocol on channels
// 0x60 / 0x61, advertises the snapshots it holds in response to
// SnapshotsRequest, and serves chunk_NNNNN.bin bytes in response to
// ChunkRequest. No consensus, no block sync, no application.db.
//
// Two source modes:
//
//   - Static (\`-snapshot <dir>\` repeatable): explicit list, loaded
//     once at startup, no rescan. Best for one-shot benchmarks where
//     you know exactly what to serve.
//
//   - Dir-watch (\`-snapshots <root>\`): scan a parent directory once
//     at startup and periodically (or on SIGHUP) thereafter. Wrong-
//     chain entries and incomplete fetches are skipped. Operator
//     workflow: drop a finished snapshot into the root, send SIGHUP
//     (or wait for the next rescan), and it goes live.
//
// Examples:
//
//	# benchmark fetch against a local server
//	malcom snapshot serve -chain cosmoshub-4 \
//	    -snapshot /data/snapshots/snapshot_cosmoshub-4_<H>
//
//	# advertise everything under /data/snapshots for this chain
//	malcom snapshot serve -chain cosmoshub-4 \
//	    -snapshots /data/snapshots
//
// See \`malcom snapshot serve -h\` for flags. Tuning knobs that overlap
// with snapfetch (PEX wave size, addrbook ban duration, etc.) are
// shared via the same [chains.<id>.fetch] section in config.toml.
package snapshotserve

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/logctx"
	"github.com/zrbecker/cosmos-p2p/internal/snapserve"
)

const (
	ExitSuccess     = 0
	ExitGeneric     = 1
	ExitConfig      = 2
	ExitNoSnapshots = 3
	ExitVerifyFail  = 4
	ExitInterrupted = 130
)

// stringSliceFlag accumulates -snapshot occurrences. flag.Value
// implementation lets the user repeat the flag instead of stuffing
// commas into a single string.
type stringSliceFlag []string

func (s *stringSliceFlag) String() string     { return strings.Join(*s, ",") }
func (s *stringSliceFlag) Set(v string) error { *s = append(*s, v); return nil }

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom snapshot serve", flag.ContinueOnError)
	chain := fs.String("chain", "", "chain id (required; must have been added with `malcom add <chain-id>`)")
	listen := fs.String("listen", "", "override the listen addr from config.fetch.listen (e.g. tcp://0.0.0.0:26656)")
	moniker := fs.String("moniker", "", "override the moniker from config.fetch.moniker")
	bootstrap := fs.String("bootstrap", "", "comma-separated extra bootstrap_peers (id@host:port). Appended to the chain's configured peers.")
	pexDisabled := fs.Bool("pex-disabled", false, "override config.fetch.pex_disabled. When true, the server only dials peers in bootstrap_peers and won't accept addrbook entries — useful for isolated benchmarks.")

	var snapshots stringSliceFlag
	fs.Var(&snapshots, "snapshot", "snapshot directory to serve (repeatable; static mode — no rescan). Mutually exclusive with -snapshots.")
	snapshotsRoot := fs.String("snapshots", "", "parent directory to scan for snapshot subdirs (dir-watch mode; rescans on -rescan-interval and on SIGHUP). Mutually exclusive with -snapshot.")
	rescanInterval := fs.Duration("rescan-interval", 0, "(with -snapshots) how often to rescan the root dir for new/removed snapshots. Default 30s; 0 disables periodic rescan (SIGHUP-only refresh).")

	verifyMode := fs.String("verify", "aggregate", "verify on startup: 'metadata' (cheap, no chunk reads), 'aggregate' (one read pass, checks SHA256 of concatenated chunks), or 'per-chunk' (one read pass, checks per-chunk hashes). Default 'aggregate'. In dir-watch mode, applied to every rescan.")
	debug := fs.Bool("debug", false, "verbose snapserve logging")
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	if err := fs.Parse(args); err != nil {
		return ExitConfig
	}

	if *chain == "" {
		fmt.Fprintln(os.Stderr, "required: -chain <id>")
		return ExitConfig
	}
	if len(snapshots) == 0 && *snapshotsRoot == "" {
		fmt.Fprintln(os.Stderr, "required: -snapshot <dir> (repeatable) or -snapshots <root>")
		return ExitConfig
	}
	if len(snapshots) > 0 && *snapshotsRoot != "" {
		fmt.Fprintln(os.Stderr, "-snapshot and -snapshots are mutually exclusive")
		return ExitConfig
	}

	mode, err := parseVerifyMode(*verifyMode)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return ExitConfig
	}

	logModeParsed, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return ExitConfig
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return ExitConfig
	}
	ch, err := cfg.Resolve(*chain)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return ExitConfig
	}

	logOpts, err := malcomlog.BuildOptions(malcomlog.Tuning{
		Level:   ch.Log.Level,
		Modules: ch.Log.Modules,
	}, logModeParsed, *debug, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log config: %v\n", err)
		return ExitConfig
	}
	logger := malcomlog.New(logOpts)
	serveLog := logger.With("module", "serve-cli")

	if err := os.MkdirAll(filepath.Dir(ch.NodeKey), 0o755); err != nil {
		serveLog.Error("mkdir node-key dir failed", "err", err, "dir", filepath.Dir(ch.NodeKey))
		return ExitGeneric
	}

	// Resolve absolute paths up-front so the server's logs match what
	// the operator typed (or made absolute) from the CLI.
	absDirs := make([]string, len(snapshots))
	for i, d := range snapshots {
		abs, err := filepath.Abs(d)
		if err != nil {
			serveLog.Error("resolve snapshot dir failed", "err", err, "dir", d)
			return ExitConfig
		}
		absDirs[i] = abs
	}
	absRoot := ""
	if *snapshotsRoot != "" {
		abs, err := filepath.Abs(*snapshotsRoot)
		if err != nil {
			serveLog.Error("resolve snapshots root failed", "err", err, "dir", *snapshotsRoot)
			return ExitConfig
		}
		absRoot = abs
	}

	listenAddr := ch.Fetch.Listen
	if *listen != "" {
		listenAddr = *listen
	}
	monikerVal := ch.Fetch.Moniker
	if *moniker != "" {
		monikerVal = *moniker
	}

	// Append any -bootstrap entries onto the chain-configured list.
	bootstrapPeers := append([]string{}, ch.Fetch.BootstrapPeers...)
	for _, s := range strings.Split(*bootstrap, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			bootstrapPeers = append(bootstrapPeers, s)
		}
	}

	// pex_disabled flag override: explicit false from CLI takes
	// precedence; default-false at the flag level can't override a
	// config-true. So we only honor the flag when it's set true OR
	// the config doesn't set it. Simpler: any explicit -pex-disabled
	// wins (only takes effect when set on the command line).
	pexDisabledVal := ch.Fetch.PEXDisabled
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "pex-disabled" {
			pexDisabledVal = *pexDisabled
		}
	})

	scfg := snapserve.Config{
		ChainID:             ch.ChainID,
		NodeKeyPath:         ch.NodeKey,
		Listen:              listenAddr,
		Moniker:             monikerVal,
		AddrBook:            ch.AddrBook,
		Banlist:             ch.Banlist,
		BootstrapPeers:      bootstrapPeers,
		SnapshotDirs:        absDirs,
		SnapshotsRoot:       absRoot,
		RescanInterval:      *rescanInterval,
		VerifyMode:          mode,
		MaxOutboundPeers:    ch.Fetch.MaxOutboundPeers,
		AllowDuplicateIP:    ch.Fetch.AllowDuplicateIP,
		PEXTargetPeers:      ch.Fetch.PEXTargetPeers,
		PEXMaxPerWave:       ch.Fetch.PEXMaxPerWave,
		PEXDisabled:         pexDisabledVal,
		AddrBookBanDuration: ch.Fetch.AddrBookBanDuration.Duration(),
		MaxDialFailures:     ch.Fetch.MaxDialFailures,
		PeerRedialBackoff:   ch.Fetch.RedialBackoff.Duration(),
		MaxRedials:          ch.Fetch.PeerRedials,
	}

	serveLog.Info("config", "path", cfg.Path())
	serveLog.Info("chain", "id", ch.ChainID)
	if absRoot != "" {
		serveLog.Info("snapshots", "mode", "dir-watch", "root", absRoot)
	} else {
		serveLog.Info("snapshots", "mode", "static", "dirs", absDirs)
	}
	serveLog.Info("node key", "path", ch.NodeKey)

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rootCtx = logctx.With(rootCtx, logger)

	// Shutdown channel — SIGINT/SIGTERM only. Two signals force exit.
	sigShutdown := make(chan os.Signal, 2)
	signal.Notify(sigShutdown, os.Interrupt, syscall.SIGTERM)
	var interrupted atomic.Bool
	go func() {
		<-sigShutdown
		interrupted.Store(true)
		serveLog.Info("interrupted, shutting down (press Ctrl-C again to force exit)")
		cancel()
		<-sigShutdown
		fmt.Fprintln(os.Stderr, "snapserve: forced exit on second signal")
		os.Exit(130)
	}()

	// Reload channel — SIGHUP triggers an immediate catalog rescan in
	// dir-watch mode. Wired via OnReloader: RunServe hands us the
	// catalog's Trigger fn once the catalog is up, and we forward
	// every HUP to it. In static mode (no -snapshots) the reloader is
	// never invoked, so HUP is a no-op.
	sigReload := make(chan os.Signal, 1)
	signal.Notify(sigReload, syscall.SIGHUP)
	var trigger atomic.Pointer[func()]
	go func() {
		for range sigReload {
			if t := trigger.Load(); t != nil {
				serveLog.Info("SIGHUP — triggering catalog rescan")
				(*t)()
			} else {
				serveLog.Info("SIGHUP — no catalog to rescan (static mode); ignoring")
			}
		}
	}()
	scfg.OnReloader = func(t func()) { trigger.Store(&t) }

	if err := snapserve.RunServe(rootCtx, scfg); err != nil {
		if interrupted.Load() || errors.Is(err, context.Canceled) {
			return ExitInterrupted
		}
		serveLog.Error("snapserve failed", "err", err)
		// Best-effort classification: store-load errors look like
		// "load store: ..." and almost always come from missing
		// .complete or hash mismatch. Other errors are p2p stack
		// or filesystem issues.
		if isStoreErr(err) {
			return ExitVerifyFail
		}
		return ExitGeneric
	}
	if interrupted.Load() {
		return ExitInterrupted
	}
	return ExitSuccess
}

func parseVerifyMode(s string) (snapserve.VerifyMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "metadata", "metadata-only", "none":
		return snapserve.VerifyMetadataOnly, nil
	case "aggregate", "aggregate-hash", "":
		return snapserve.VerifyAggregateHash, nil
	case "per-chunk", "per_chunk", "perchunk", "chunks":
		return snapserve.VerifyPerChunkHash, nil
	default:
		return 0, fmt.Errorf("invalid -verify %q (want metadata|aggregate|per-chunk)", s)
	}
}

func isStoreErr(err error) bool {
	return strings.HasPrefix(err.Error(), "load store:")
}
