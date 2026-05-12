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
//   - Static (`--snapshot <dir>` repeatable): explicit list, loaded
//     once at startup, no rescan. Best for one-shot benchmarks where
//     you know exactly what to serve.
//
//   - Dir-watch (`--snapshots <root>`): scan a parent directory once
//     at startup and periodically (or on SIGHUP) thereafter. Wrong-
//     chain entries and incomplete fetches are skipped. Operator
//     workflow: drop a finished snapshot into the root, send SIGHUP
//     (or wait for the next rescan), and it goes live.
//
// Examples:
//
//	# benchmark fetch against a local server
//	malcom snapshot serve --chain cosmoshub-4 \
//	    --snapshot /data/snapshots/snapshot_cosmoshub-4_<H>
//
//	# advertise everything under /data/snapshots for this chain
//	malcom snapshot serve --chain cosmoshub-4 \
//	    --snapshots /data/snapshots
//
// See `malcom snapshot serve -h` for flags. Tuning knobs that overlap
// with snapfetch (PEX wave size, addrbook ban duration, etc.) are
// shared via the same [chains.<id>.fetch] section in config.toml.
package snapshotserve

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/ambroslabs/malcom/internal/cli/cliexit"
	"github.com/ambroslabs/malcom/internal/config"
	malcomlog "github.com/ambroslabs/malcom/internal/log"
	"github.com/ambroslabs/malcom/internal/logctx"
	"github.com/ambroslabs/malcom/internal/snapserve"
)

const (
	ExitSuccess     = 0
	ExitGeneric     = 1
	ExitConfig      = 2
	ExitNoSnapshots = 3
	ExitVerifyFail  = 4
	ExitInterrupted = 130
)

type serveFlags struct {
	chain             string
	listen            string
	moniker           string
	bootstrap         string
	pexDisabled       bool
	snapshots         []string
	snapshotsRoot     string
	rescanInterval    time.Duration
	peerRedials       int
	chunkRatePerPeer  float64
	chunkBurstPerPeer int
	chunkRateGlobal   float64
	chunkBurstGlobal  int
	shutdownDrain     time.Duration
	persistInterval   time.Duration
	verifyMode        string
	debug             bool
	logMode           string
}

// NewCmd returns the `malcom snapshot serve` cobra command.
func NewCmd() *cobra.Command {
	f := &serveFlags{}
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "advertise a local snapshot dir over the state-sync P2P protocol",
		Long:  "Run a wire-only state-sync server backed by one or more locally-fetched snapshot directories.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd, f)
		},
	}
	cmd.Flags().StringVar(&f.chain, "chain", "", "chain id (required; must have been added with 'malcom add <chain-id>')")
	cmd.Flags().StringVar(&f.listen, "listen", "", "override the listen addr from config.fetch.listen (e.g. tcp://0.0.0.0:26656)")
	cmd.Flags().StringVar(&f.moniker, "moniker", "", "override the moniker from config.fetch.moniker")
	cmd.Flags().StringVar(&f.bootstrap, "bootstrap", "", "comma-separated extra bootstrap_peers (id@host:port). Appended to the chain's configured peers.")
	cmd.Flags().BoolVar(&f.pexDisabled, "pex-disabled", false, "override config.fetch.pex_disabled. When true, the server only dials peers in bootstrap_peers and won't accept addrbook entries — useful for isolated benchmarks.")
	cmd.Flags().StringArrayVar(&f.snapshots, "snapshot", nil, "snapshot directory to serve (repeatable; static mode — no rescan). Mutually exclusive with --snapshots.")
	cmd.Flags().StringVar(&f.snapshotsRoot, "snapshots", "", "parent directory to scan for snapshot subdirs (dir-watch mode; rescans on --rescan-interval and on SIGHUP). Mutually exclusive with --snapshot.")
	cmd.Flags().DurationVar(&f.rescanInterval, "rescan-interval", 0, "(with --snapshots) how often to rescan the root dir for new/removed snapshots. Default 30s; 0 disables periodic rescan (SIGHUP-only refresh).")
	cmd.Flags().IntVar(&f.peerRedials, "peer-redials", 0, "cap on consecutive disconnect/redial cycles before connect.Manager auto-bans a pinned peer for the run. 0 = unlimited (the serve default — a long-running daemon shouldn't permanently bench legitimate peers with intermittent connectivity). Pass a positive value to opt into the fetch-style cap.")
	cmd.Flags().Float64Var(&f.chunkRatePerPeer, "chunk-rate-per-peer", 4, "max sustained ChunkRequest per second from any one peer. 0 disables the per-peer bucket. See #85.")
	cmd.Flags().IntVar(&f.chunkBurstPerPeer, "chunk-burst-per-peer", 8, "max burst (token-bucket capacity) of ChunkRequest from any one peer.")
	cmd.Flags().Float64Var(&f.chunkRateGlobal, "chunk-rate-global", 16, "safety-net cap on total ChunkRequest per second across all peers (catches the many-peers-each-below-per-peer case). 0 disables.")
	cmd.Flags().IntVar(&f.chunkBurstGlobal, "chunk-burst-global", 32, "max burst (token-bucket capacity) of ChunkRequest across all peers.")
	cmd.Flags().DurationVar(&f.shutdownDrain, "shutdown-drain", 30*time.Second, "on SIGINT/SIGTERM, how long to fast-fail inbound ChunkRequest with Missing=true while in-flight ChunkResponse sends flush. 0 disables the drain (legacy behaviour: peers mid-transfer get torn off when the socket closes).")
	cmd.Flags().DurationVar(&f.persistInterval, "persist-interval", 5*time.Minute, "how often the addrbook + banlist are persisted to disk by a background goroutine. Without this, a crash/OOM/SIGKILL loses every PEX-learned peer since the last clean shutdown.")
	cmd.Flags().StringVar(&f.verifyMode, "verify", "aggregate", "verify on startup: 'metadata' (cheap, no chunk reads), 'aggregate' (one read pass, checks SHA256 of concatenated chunks), or 'per-chunk' (one read pass, checks per-chunk hashes). Default 'aggregate'. In dir-watch mode, applied to every rescan.")
	cmd.Flags().BoolVar(&f.debug, "debug", false, "verbose snapserve logging")
	cmd.Flags().StringVar(&f.logMode, "log", "", "log output: auto (default), pretty, text, json")
	return cmd
}

func run(cmd *cobra.Command, f *serveFlags) error {
	if f.chain == "" {
		fmt.Fprintln(os.Stderr, "required: --chain <id>")
		return &cliexit.Error{Code: ExitConfig}
	}
	if len(f.snapshots) == 0 && f.snapshotsRoot == "" {
		fmt.Fprintln(os.Stderr, "required: --snapshot <dir> (repeatable) or --snapshots <root>")
		return &cliexit.Error{Code: ExitConfig}
	}
	if len(f.snapshots) > 0 && f.snapshotsRoot != "" {
		fmt.Fprintln(os.Stderr, "--snapshot and --snapshots are mutually exclusive")
		return &cliexit.Error{Code: ExitConfig}
	}

	mode, err := parseVerifyMode(f.verifyMode)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return &cliexit.Error{Code: ExitConfig}
	}

	logModeParsed, ok := malcomlog.ParseMode(f.logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid --log %q (want auto/pretty/text/json)\n", f.logMode)
		return &cliexit.Error{Code: ExitConfig}
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return &cliexit.Error{Code: ExitConfig}
	}
	ch, err := cfg.Resolve(f.chain)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return &cliexit.Error{Code: ExitConfig}
	}

	logOpts, err := malcomlog.BuildOptions(malcomlog.Tuning{
		Level:   ch.Log.Level,
		Modules: ch.Log.Modules,
	}, logModeParsed, f.debug, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log config: %v\n", err)
		return &cliexit.Error{Code: ExitConfig}
	}
	logger := malcomlog.New(logOpts)
	serveLog := logger.With("module", "serve-cli")

	if err := os.MkdirAll(filepath.Dir(ch.NodeKey), 0o755); err != nil {
		serveLog.Error("mkdir node-key dir failed", "err", err, "dir", filepath.Dir(ch.NodeKey))
		return &cliexit.Error{Code: ExitGeneric}
	}

	// Resolve absolute paths up-front so the server's logs match what
	// the operator typed (or made absolute) from the CLI.
	absDirs := make([]string, len(f.snapshots))
	for i, d := range f.snapshots {
		abs, err := filepath.Abs(d)
		if err != nil {
			serveLog.Error("resolve snapshot dir failed", "err", err, "dir", d)
			return &cliexit.Error{Code: ExitConfig}
		}
		absDirs[i] = abs
	}
	absRoot := ""
	if f.snapshotsRoot != "" {
		abs, err := filepath.Abs(f.snapshotsRoot)
		if err != nil {
			serveLog.Error("resolve snapshots root failed", "err", err, "dir", f.snapshotsRoot)
			return &cliexit.Error{Code: ExitConfig}
		}
		absRoot = abs
	}

	listenAddr := ch.Fetch.Listen
	if f.listen != "" {
		listenAddr = f.listen
	}
	monikerVal := ch.Fetch.Moniker
	if f.moniker != "" {
		monikerVal = f.moniker
	}

	// Append any --bootstrap entries onto the chain-configured list.
	bootstrapPeers := append([]string{}, ch.Fetch.BootstrapPeers...)
	for _, s := range strings.Split(f.bootstrap, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			bootstrapPeers = append(bootstrapPeers, s)
		}
	}

	// pex-disabled flag override: explicit --pex-disabled wins, unset
	// flag keeps the config value. cmd.Flags().Changed reports whether
	// the operator typed --pex-disabled on the command line.
	pexDisabledVal := ch.Fetch.PEXDisabled
	if cmd.Flags().Changed("pex-disabled") {
		pexDisabledVal = f.pexDisabled
	}

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
		RescanInterval:      f.rescanInterval,
		VerifyMode:          mode,
		MaxOutboundPeers:    ch.Fetch.MaxOutboundPeers,
		AllowDuplicateIP:    ch.Fetch.AllowDuplicateIP,
		PEXTargetPeers:      ch.Fetch.PEXTargetPeers,
		PEXMaxPerWave:       ch.Fetch.PEXMaxPerWave,
		PEXDisabled:         pexDisabledVal,
		AddrBookBanDuration: ch.Fetch.AddrBookBanDuration.Duration(),
		MaxDialFailures:     ch.Fetch.MaxDialFailures,
		PeerRedialBackoff:   ch.Fetch.RedialBackoff.Duration(),
		// MaxRedials is deliberately *not* read from ch.Fetch.PeerRedials
		// — serve defaults to 0 (unlimited) so we don't permanently
		// bench legitimate peers with intermittent connectivity over
		// weeks of uptime. --peer-redials lets operators opt back into
		// the fetch-style cap if they want it.
		MaxRedials:        f.peerRedials,
		PersistInterval:   f.persistInterval,
		ShutdownDrain:     f.shutdownDrain,
		ChunkRatePerPeer:  f.chunkRatePerPeer,
		ChunkBurstPerPeer: f.chunkBurstPerPeer,
		ChunkRateGlobal:   f.chunkRateGlobal,
		ChunkBurstGlobal:  f.chunkBurstGlobal,

		MaxPacketMsgPayloadSize: ch.Fetch.MaxPacketMsgPayloadSize,
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
	// every HUP to it. In static mode (no --snapshots) the reloader is
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
			return &cliexit.Error{Code: ExitInterrupted}
		}
		serveLog.Error("snapserve failed", "err", err)
		// Best-effort classification: store-load errors look like
		// "load store: ..." and almost always come from missing
		// .complete or hash mismatch. Other errors are p2p stack
		// or filesystem issues.
		if isStoreErr(err) {
			return &cliexit.Error{Code: ExitVerifyFail}
		}
		return &cliexit.Error{Code: ExitGeneric}
	}
	if interrupted.Load() {
		return &cliexit.Error{Code: ExitInterrupted}
	}
	return nil
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
		return 0, fmt.Errorf("invalid --verify %q (want metadata|aggregate|per-chunk)", s)
	}
}

func isStoreErr(err error) bool {
	return strings.HasPrefix(err.Error(), "load store:")
}
