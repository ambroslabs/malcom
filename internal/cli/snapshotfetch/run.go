// Package snapshotfetch is the `malcom snapshot fetch` subcommand:
// download a state-sync snapshot for the configured chain.
//
// Output: <-out>/snapshot_<chain>_<height>/ (chunks + meta.json +
// metadata.bin + .complete marker). Default -out is the current
// working directory.
//
// Tuning knobs (timeouts, parallelism, peer-selection rules) live in
// the [chains.<id>.fetch] section of config.toml; only operational
// flags survive on the CLI.
//
// Run returns one of the documented Exit* codes so wrapping
// orchestrators can distinguish failure modes (retry vs. pivot vs.
// escalate). See exit.go for the contract.
package snapshotfetch

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ambroslabs/malcom/internal/cli/verify"
	"github.com/ambroslabs/malcom/internal/config"
	malcomlog "github.com/ambroslabs/malcom/internal/log"
	"github.com/ambroslabs/malcom/internal/logctx"
	"github.com/ambroslabs/malcom/internal/snapfetch"
)

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom snapshot fetch", flag.ContinueOnError)
	chain := fs.String("chain", "", "chain id (required; must have been added with `malcom add <chain-id>`)")
	out := fs.String("out", ".", "parent dir for the snapshot output (subdir snapshot_<chain>_<height>/ created inside)")
	targetHeight := fs.Uint64("target-height", 0, "lock to this exact height; otherwise pick the best candidate")
	maxHeightFlag := fs.Uint64("max-height", 0, "upper bound for snapshot selection; skips the RPC /status lookup (useful when RPCs are stale or unreachable). Defaults to the chain's current height.")
	maxAge := fs.Uint64("max-age", 0, fmt.Sprintf("freshness floor in blocks (default %d; override in config.fetch.max_age_blocks)", config.DefaultMaxAgeBlocks))
	noVerifyHash := fs.Bool("no-verify-hash", false, "skip the post-download SHA256(chunks) == offer.Hash check; per-chunk hashes are still verified against metadata. Run `malcom verify` afterwards if you skip.")
	debug := fs.Bool("debug", false, "verbose snapfetch logging")
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	pipelineImport := fs.Bool("import", false, "after the snapshot dir + chunk count are known, start `malcom snapshot import` in parallel (parallel/chunk-ring path) and stream chunks to it as they land. Snapshot is still persisted to <-out>; cancel with Ctrl-C aborts both stages.")
	importOut := fs.String("import-out", "", "(with -import) parent dir for the appdb output. Default: same as -out.")
	importNoExtensions := fs.Bool("import-no-extensions", false, "(with -import) skip writing extension payloads")
	importWorkers := fs.Int("import-workers", 0, "(with -import) parallel-import store workers; 0 = NumCPU")
	importChunkMB := fs.Int("import-chunk-mb", 0, "(with -import) chunk-ring budget in MiB; 0 = default (512)")
	importWaveParallel := fs.Bool("import-wave-parallel", false, "(with -import) within-store wave-parallel hashing")
	importFastIngest := fs.Bool("import-fast-ingest", true, "(with -import) bulk-ingest the f/ fast-storage entries via per-store sstable.Writer")
	noVerify := fs.Bool("no-verify", false, "(with -import) skip the post-import AppHash check against the chain's configured rpcs. Default behaviour: when -import is set and the chain has rpcs, run the same check `malcom verify` performs and exit non-zero on mismatch.")
	if err := fs.Parse(args); err != nil {
		return ExitConfig
	}

	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return ExitConfig
	}

	if *chain == "" {
		fmt.Fprintln(os.Stderr, "required: -chain <id>")
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

	// Build the logger from the resolved [log] / [log.modules] config.
	// -debug lowers the global threshold to debug AND bumps every
	// non-"silent" entry in [log.modules] to debug.
	logOpts, err := malcomlog.BuildOptions(malcomlog.Tuning{
		Level:   ch.Log.Level,
		Modules: ch.Log.Modules,
	}, mode, *debug, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log config: %v\n", err)
		return ExitConfig
	}
	logger := malcomlog.New(logOpts)
	fetchLog := logger.With("module", "fetch-cli")

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fetchLog.Error("mkdir out failed", "err", err, "dir", *out)
		return ExitDiskFailed
	}
	// Ensure node key + cache dirs exist (config.Resolve gave us paths
	// but didn't create them).
	if err := os.MkdirAll(filepath.Dir(ch.NodeKey), 0o755); err != nil {
		fetchLog.Error("mkdir node-key dir failed", "err", err, "dir", filepath.Dir(ch.NodeKey))
		return ExitDiskFailed
	}

	// Resolve maxHeight. Three paths:
	//   - -target-height set: skip lookup entirely (we lock to that one height).
	//   - -max-height set: use it as-is, skip the RPC.
	//   - else: query configured RPCs in order; first success wins.
	// Failure to resolve when needed is fatal (the walking algorithm
	// requires a maxHeight to pick its starting target).
	var maxHeight uint64
	var heightSource string
	switch {
	case *targetHeight != 0:
		// no lookup needed
	case *maxHeightFlag != 0:
		maxHeight = *maxHeightFlag
		heightSource = "-max-height flag"
	default:
		if len(ch.RPCs) == 0 {
			fetchLog.Error("config has no rpcs; pass -target-height or -max-height, or fix chains config",
				"chain", ch.ChainID)
			return ExitConfig
		}
		h, src, err := fetchCurrentHeightVerbose(ch.RPCs, fetchLog)
		if err != nil {
			fetchLog.Error("all rpcs failed; cannot determine chain head (use -max-height to override)",
				"chain", ch.ChainID, "rpc_count", len(ch.RPCs))
			return ExitGeneric
		}
		// A successful response of latest_block_height=0 means the
		// RPC believes the chain has no blocks (brand-new test net,
		// freshly reset node, misconfigured endpoint). The walk
		// phase has nothing to enumerate; surface that here with a
		// clearer message than the downstream ErrWalkFailed.
		if h == 0 {
			fetchLog.Error("rpc reports latest_block_height=0 (chain not started or stale node); pass -max-height to override",
				"chain", ch.ChainID, "source", src)
			return ExitGeneric
		}
		maxHeight = h
		heightSource = src
	}

	// Freshness floor. 0 = "use config value" (which itself defaults
	// to DefaultMaxAgeBlocks via applyFetchDefaults if unset).
	effMaxAge := ch.Fetch.MaxAgeBlocks
	if *maxAge != 0 {
		effMaxAge = *maxAge
	}
	var minHeight uint64
	if effMaxAge > 0 && maxHeight > effMaxAge {
		minHeight = maxHeight - effMaxAge
	}
	if maxHeight > 0 {
		fetchLog.Info("freshness floor",
			"max_height", maxHeight,
			"source", heightSource,
			"max_age_blocks", effMaxAge,
			"min_height", minHeight)
	}

	scfg := snapfetch.Config{
		ChainID:                  ch.ChainID,
		NodeKeyPath:              ch.NodeKey,
		Listen:                   ch.Fetch.Listen,
		Moniker:                  ch.Fetch.Moniker,
		AddrBook:                 ch.AddrBook,
		Banlist:                  ch.Banlist,
		Served:                   ch.Served,
		BootstrapPeers:           ch.Fetch.BootstrapPeers,
		DiscoverFor:              ch.Fetch.Discover.Duration(),
		DialParallel:             ch.Fetch.DialParallel,
		MaxCandidates:            ch.Fetch.MaxCandidates,
		ProbeTimeout:             ch.Fetch.ProbeTimeout.Duration(),
		MinGoodPeers:             ch.Fetch.MinPeers,
		PerPeerLimit:             ch.Fetch.PerPeer,
		ChunkTimeout:             ch.Fetch.ChunkTimeout.Duration(),
		PeerFailLimit:            ch.Fetch.PeerFails,
		MaxRedials:               ch.Fetch.PeerRedials,
		PeerRedialBackoff:        ch.Fetch.RedialBackoff.Duration(),
		TargetHeight:             *targetHeight,
		MaxHeight:                maxHeight,
		MinHeight:                minHeight,
		SnapshotInterval:         ch.Fetch.SnapshotInterval,
		PerHeightTimeout:         ch.Fetch.PerHeightTimeout.Duration(),
		MaxOutboundPeers:         ch.Fetch.MaxOutboundPeers,
		AllowDuplicateIP:         ch.Fetch.AllowDuplicateIP,
		PEXTargetPeers:           ch.Fetch.PEXTargetPeers,
		PEXMaxPerWave:            ch.Fetch.PEXMaxPerWave,
		PEXDisabled:              ch.Fetch.PEXDisabled,
		ChurnGrace:               ch.Fetch.ChurnGrace.Duration(),
		RequireStateSyncChannel:  ch.Fetch.RequireStateSyncChannel,
		AddrBookBanDuration:      ch.Fetch.AddrBookBanDuration.Duration(),
		ProvisionalProbeStrikes:  ch.Fetch.ProvisionalProbeStrikes,
		ProvisionalProbeInflight: ch.Fetch.ProvisionalProbeInflight,
		MaxDialFailures:          ch.Fetch.MaxDialFailures,
		MaxDiskWriteFailures:     ch.Fetch.MaxDiskWriteFailures,
		MaxRescans:               ch.Fetch.MaxRescans,
		RescanDiscoverFor:        ch.Fetch.RescanDiscover.Duration(),
		SkipVerifyHash:           *noVerifyHash,
	}

	fetchLog.Info("config", "path", cfg.Path())
	fetchLog.Info("chain", "id", ch.ChainID)
	fetchLog.Info("out", "dir", *out)
	fetchLog.Info("node key", "path", ch.NodeKey)

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rootCtx = logctx.With(rootCtx, logger)
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	var interrupted atomic.Bool
	go func() {
		<-sigCh
		interrupted.Store(true)
		fetchLog.Info("interrupted, shutting down (press Ctrl-C again to force exit)")
		cancel()
		// A second signal forces immediate exit, skipping deferred
		// cleanup (addrbook/banlist saves). Without this, the user has
		// no escape hatch if shutdown stalls — SIGKILL is the only
		// alternative. Exit 130 = 128 + SIGINT, by bash convention.
		<-sigCh
		fmt.Fprintln(os.Stderr, "snapfetch: forced exit on second signal")
		os.Exit(130)
	}()

	// Pipelined-import orchestrator. Wires snapfetch's
	// OnDownloadReady/OnChunkReady callbacks to a TailingChunkSource
	// that the parallel importer consumes as its decompressed-stream
	// source. See pipeline.go for the rationale.
	var pipeline *pipelineState
	if *pipelineImport {
		appdbOut := *importOut
		if appdbOut == "" {
			appdbOut = *out
		}
		if err := os.MkdirAll(appdbOut, 0o755); err != nil {
			fetchLog.Error("mkdir import-out failed", "err", err, "dir", appdbOut)
			return ExitDiskFailed
		}
		pipeline = &pipelineState{
			chain:          ch,
			importOutDir:   appdbOut,
			noExtensions:   *importNoExtensions,
			cancelFetchCtx: cancel,
			logger:         logger,
			parallelOpts: pipelineImportTuning{
				Workers:      *importWorkers,
				ChunkMB:      *importChunkMB,
				WaveParallel: *importWaveParallel,
				FastIngest:   *importFastIngest,
			},
		}
		scfg.OnDownloadReady = pipeline.onDownloadReady
		scfg.OnChunkReady = pipeline.onChunkReady
		fetchLog.Info("pipelined import enabled",
			"appdb_out", appdbOut,
			"workers", *importWorkers,
			"chunk_mb", *importChunkMB,
			"wave_parallel", *importWaveParallel,
			"fast_ingest", *importFastIngest)
	}

	fetchErr := snapfetch.RunFetch(rootCtx, scfg, *out)

	if pipeline != nil {
		stats, err := pipeline.finalize(fetchErr)
		if err != nil {
			if fetchErr != nil {
				fetchLog.Error("snapfetch failed", "err", fetchErr)
			} else {
				fetchLog.Error("import failed", "err", err)
			}
			return mapExitCode(err, interrupted.Load())
		}
		if stats != nil {
			fetchLog.Info("pipeline complete",
				"import_elapsed", stats.Elapsed,
				"items", stats.Items,
				"stores", len(stats.Stores))
		}
		return runPostImportVerify(fetchLog, pipeline.appdbOut, pipeline.appdbHeight, ch.RPCs, *noVerify)
	}

	if fetchErr != nil {
		fetchLog.Error("snapfetch failed", "err", fetchErr)
		return mapExitCode(fetchErr, interrupted.Load())
	}
	fetchLog.Info("WARNING: snapshot contents are not authenticated by p2p — run `malcom verify` against a trusted RPC before using this snapshot in production")
	return ExitSuccess
}

// runPostImportVerify runs the same AppHash check `malcom verify`
// performs against the pipelined-import output, returning the
// appropriate exit code. Decoupled from inline so the import-side and
// the no-import-skip paths share one place.
//
// Skip conditions and their reasoning:
//   - noVerify flag set: explicit operator opt-out.
//   - rpcs empty: the chain config has no rpcs configured, so we have
//     no trust anchor to check against. Falls back to the historic
//     "WARNING run malcom verify yourself" log line.
//   - appdb empty: shouldn't happen in the import path, but if the
//     pipeline never started (walk failure before OnDownloadReady)
//     we have nothing to verify.
func runPostImportVerify(log *slog.Logger, appdb string, height int64, rpcs []string, noVerify bool) int {
	if appdb == "" {
		log.Info("WARNING: snapshot contents are not authenticated by p2p — run `malcom verify` against a trusted RPC before using this snapshot in production")
		return ExitSuccess
	}
	if noVerify {
		log.Info("WARNING: -no-verify set; snapshot contents are not authenticated by p2p — run `malcom verify` against a trusted RPC before using this snapshot in production")
		return ExitSuccess
	}
	if len(rpcs) == 0 {
		log.Info("WARNING: chain config has no rpcs; snapshot contents are not authenticated by p2p — populate chains/<id>.toml rpcs or run `malcom verify -rpc <url>` against a trusted RPC before using this snapshot in production")
		return ExitSuccess
	}
	verifyLog := log.With("module", "verify")
	verifyLog.Info("verifying apphash against rpc", "appdb", appdb, "height", height, "rpcs", len(rpcs))
	res, err := verify.CheckAppHash(appdb, height, rpcs, verifyLog)
	switch {
	case errors.Is(err, verify.ErrMismatch):
		verifyLog.Error("MISMATCH — local apphash disagrees with consensus",
			"height", res.Height,
			"local", fmt.Sprintf("%X", res.LocalHash),
			"consensus", fmt.Sprintf("%X", res.ConsensusHash),
			"rpc", res.UsedRPC,
			"action", "the imported db is left in place; re-fetch is the typical recovery")
		return ExitVerifyFailed
	case err != nil:
		// No RPC was reachable or the check itself failed for an
		// unrelated reason. Don't fail the pipeline just because the
		// trust anchor wasn't reachable — surface a loud warning so
		// the operator can re-run `malcom verify` themselves.
		verifyLog.Warn("post-import verify did not complete; snapshot is not authenticated",
			"err", err,
			"hint", "run `malcom verify -appdb "+appdb+"` once an rpc is reachable")
		return ExitSuccess
	}
	verifyLog.Info("MATCH — application.db is consensus-correct",
		"height", res.Height,
		"apphash", fmt.Sprintf("%X", res.LocalHash),
		"rpc", res.UsedRPC)
	return ExitSuccess
}

// fetchCurrentHeightVerbose is fetchCurrentHeight with per-URL
// failure logging — each unreachable RPC gets an Error line so the
// user can see which to prune from chains/<id>.toml. Returns the
// successful URL alongside the height so the caller can surface
// where the value came from (some RPCs cache and are minutes stale).
func fetchCurrentHeightVerbose(rpcs []string, logger *slog.Logger) (uint64, string, error) {
	if len(rpcs) == 0 {
		return 0, "", errors.New("no RPC URLs configured")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	for _, base := range rpcs {
		url := strings.TrimRight(base, "/") + "/status"
		h, err := tryStatus(client, url)
		if err == nil {
			return h, url, nil
		}
		logger.Error("rpc unreachable", "url", url, "err", err)
	}
	return 0, "", fmt.Errorf("all %d RPCs failed", len(rpcs))
}

func tryStatus(client *http.Client, url string) (uint64, error) {
	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}
	var raw struct {
		Result struct {
			SyncInfo struct {
				LatestBlockHeight string `json:"latest_block_height"`
			} `json:"sync_info"`
		} `json:"result"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return 0, fmt.Errorf("decode: %w", err)
	}
	h, err := strconv.ParseUint(raw.Result.SyncInfo.LatestBlockHeight, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse height %q: %w", raw.Result.SyncInfo.LatestBlockHeight, err)
	}
	return h, nil
}

// fetchCurrentHeight queries cometbft's /status endpoint on the
// configured RPCs (in order) and returns the latest block height.
// Tries each URL with a 5s timeout; returns the first success.
func fetchCurrentHeight(rpcs []string) (uint64, error) {
	if len(rpcs) == 0 {
		return 0, errors.New("no RPC URLs configured")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	var lastErr error
	for _, base := range rpcs {
		url := strings.TrimRight(base, "/") + "/status"
		resp, err := client.Get(url)
		if err != nil {
			lastErr = err
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			lastErr = fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
			continue
		}
		var raw struct {
			Result struct {
				SyncInfo struct {
					LatestBlockHeight string `json:"latest_block_height"`
				} `json:"sync_info"`
			} `json:"result"`
		}
		if err := json.Unmarshal(body, &raw); err != nil {
			lastErr = fmt.Errorf("%s: decode: %w", url, err)
			continue
		}
		h, err := strconv.ParseUint(raw.Result.SyncInfo.LatestBlockHeight, 10, 64)
		if err != nil {
			lastErr = fmt.Errorf("%s: parse height %q: %w", url, raw.Result.SyncInfo.LatestBlockHeight, err)
			continue
		}
		return h, nil
	}
	return 0, fmt.Errorf("all RPCs failed: %w", lastErr)
}
