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
package snapshotfetch

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/logctx"
	"github.com/zrbecker/cosmos-p2p/internal/snapfetch"
)

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom snapshot fetch", flag.ContinueOnError)
	chain := fs.String("chain", "", fmt.Sprintf("chain id (default %q; override in config.default_chain)", config.DefaultChainID))
	out := fs.String("out", ".", "parent dir for the snapshot output (subdir snapshot_<chain>_<height>/ created inside)")
	targetHeight := fs.Uint64("target-height", 0, "lock to this exact height; otherwise pick the best candidate")
	maxHeightFlag := fs.Uint64("max-height", 0, "upper bound for snapshot selection; skips the RPC /status lookup (useful when RPCs are stale or unreachable). Defaults to the chain's current height.")
	maxAge := fs.Uint64("max-age", 0, fmt.Sprintf("freshness floor in blocks (default %d; override in config.fetch.max_age_blocks)", config.DefaultMaxAgeBlocks))
	noVerifyHash := fs.Bool("no-verify-hash", false, "skip the post-download SHA256(chunks) == offer.Hash check; per-chunk hashes are still verified against metadata. Run `malcom verify` afterwards if you skip.")
	debug := fs.Bool("debug", false, "verbose snapfetch logging")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	chainName := *chain
	if chainName == "" {
		chainName = cfg.DefaultChain
	}
	ch, err := cfg.Resolve(chainName)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir out: %v\n", err)
		return 1
	}
	// Ensure node key + cache dirs exist (config.Resolve gave us paths
	// but didn't create them).
	if err := os.MkdirAll(filepath.Dir(ch.NodeKey), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir node-key dir: %v\n", err)
		return 1
	}

	// Default mode silences cometbft's per-peer EOF spam — the p2p
	// module logs every disconnect at Error level even when it's a
	// normal seed-mode close. -debug flips to AllowDebug (everything).
	// Pass os.Stderr directly — malcomlog handles its own
	// serialization, and we need the *os.File for TTY detection.
	logger := malcomlog.New(os.Stderr)
	if *debug {
		logger = cmtlog.NewFilter(logger, cmtlog.AllowDebug())
	} else {
		logger = cmtlog.NewFilter(logger,
			cmtlog.AllowInfoWith("module", "fetch-cli"),
			cmtlog.AllowInfoWith("module", "fetch"),
			cmtlog.AllowErrorWith("module", "addrbook"),
			// pex / p2p / mconnection silenced by default
		)
	}

	fetchLog := logger.With("module", "fetch-cli")

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
			return 1
		}
		h, src, err := fetchCurrentHeightVerbose(ch.RPCs, fetchLog)
		if err != nil {
			fetchLog.Error("all rpcs failed; cannot determine chain head (use -max-height to override)",
				"chain", ch.ChainID, "rpc_count", len(ch.RPCs))
			return 1
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
		ChainID:           ch.ChainID,
		NodeKeyPath:       ch.NodeKey,
		Listen:            ch.Fetch.Listen,
		Moniker:           ch.Fetch.Moniker,
		AddrBook:          ch.AddrBook,
		Banlist:           ch.Banlist,
		BootstrapPeers:    ch.Fetch.BootstrapPeers,
		DiscoverFor:       ch.Fetch.Discover.Duration(),
		DialParallel:      ch.Fetch.DialParallel,
		MaxCandidates:     ch.Fetch.MaxCandidates,
		ProbeTimeout:      ch.Fetch.ProbeTimeout.Duration(),
		MinGoodPeers:      ch.Fetch.MinPeers,
		PerPeerLimit:      ch.Fetch.PerPeer,
		ChunkTimeout:      ch.Fetch.ChunkTimeout.Duration(),
		MaxFetchTime:      ch.Fetch.MaxFetch.Duration(),
		PeerFailLimit:            ch.Fetch.PeerFails,
		MaxRedials:               ch.Fetch.PeerRedials,
		PeerRedialBackoff:        ch.Fetch.RedialBackoff.Duration(),
		TargetHeight:             *targetHeight,
		MaxHeight:                maxHeight,
		MinHeight:                minHeight,
		SnapshotInterval:         ch.Fetch.SnapshotInterval,
		PerHeightTimeout:         ch.Fetch.PerHeightTimeout.Duration(),
		MaxOutboundPeers:         ch.Fetch.MaxOutboundPeers,
		PEXTargetPeers:           ch.Fetch.PEXTargetPeers,
		PEXMaxPerWave:            ch.Fetch.PEXMaxPerWave,
		ChurnGrace:               ch.Fetch.ChurnGrace.Duration(),
		RequireStateSyncChannel:  ch.Fetch.RequireStateSyncChannel,
		AddrBookBanDuration:      ch.Fetch.AddrBookBanDuration.Duration(),
		ProvisionalProbeStrikes:  ch.Fetch.ProvisionalProbeStrikes,
		ProvisionalProbeInflight: ch.Fetch.ProvisionalProbeInflight,
		MaxDialFailures:          ch.Fetch.MaxDialFailures,
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
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fetchLog.Info("interrupted, shutting down")
		cancel()
	}()

	if err := snapfetch.RunFetch(rootCtx, scfg, *out); err != nil {
		fmt.Fprintf(os.Stderr, "snapfetch: %v\n", err)
		return 1
	}
	fetchLog.Info("WARNING: snapshot contents are not authenticated by p2p — run `malcom verify` against a trusted RPC before using this snapshot in production")
	return 0
}

// fetchCurrentHeightVerbose is fetchCurrentHeight with per-URL
// failure logging — each unreachable RPC gets an Error line so the
// user can see which to prune from chains/<id>.toml. Returns the
// successful URL alongside the height so the caller can surface
// where the value came from (some RPCs cache and are minutes stale).
func fetchCurrentHeightVerbose(rpcs []string, logger cmtlog.Logger) (uint64, string, error) {
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

