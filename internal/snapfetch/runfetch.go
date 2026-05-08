package snapfetch

import (
	"context"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/p2p/conn"

	"github.com/zrbecker/cosmos-p2p/internal/logctx"
)

// RunFetch is the library entry point. It builds a p2p.Switch, walks
// candidate heights, downloads chunks, and writes everything under
// <outRoot>/snapshot_<chain>_<height>/ (chunks + metadata.bin +
// meta.json + .complete marker).
//
// On error any partial output under outRoot is left in place — no
// .complete marker is written. The importer refuses to run on a dir
// without .complete, so the user can safely inspect / remove the
// partial output before re-running fetch.
func RunFetch(ctx context.Context, c Config, outRoot string) error {
	ctx = logctx.WithFields(ctx, "module", "fetch")
	s, cleanup, err := newFetchSession(ctx, c)
	if err != nil {
		return err
	}
	defer cleanup()

	offer, good, chunk0, err := s.walk(ctx)
	if err != nil {
		return err
	}

	snapDir, chunkHashes, err := s.prepareSnapshotDir(outRoot, offer)
	if err != nil {
		return err
	}

	// Walk verified chunk-0 against metadata.chunk_hashes[0] before
	// accepting the offer. Seed it on disk so download.resumeFromDisk
	// picks it up and skips refetching. Best-effort: a write error
	// just means download will fetch chunk-0 again.
	if len(chunk0) > 0 {
		path := filepath.Join(snapDir, "chunk_00000.bin")
		if err := writeFileAtomic(path, chunk0, 0o644); err != nil {
			s.log.Error("seed verified chunk-0 failed; download will refetch", "err", err)
		}
	}

	fetchCtx, fetchCancel := context.WithTimeout(ctx, c.MaxFetchTime)
	bytesTotal, err := s.download(fetchCtx, offer, good, chunkHashes, snapDir)
	fetchCancel()
	if err != nil {
		// Distinguish our timeout from a parent cancel (Ctrl-C). Only
		// the former wants the config-knob hint.
		if fetchCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
			return fmt.Errorf("%w: exceeded max_fetch=%s — raise [chains.%s.fetch] max_fetch in config.toml: %w",
				ErrDownloadFailed, c.MaxFetchTime, c.ChainID, err)
		}
		return fmt.Errorf("%w: %w", ErrDownloadFailed, err)
	}

	if c.SkipVerifyHash {
		s.log.Info("snapshot integrity check skipped",
			"flag", "-no-verify-hash")
	} else {
		s.log.Info("verifying snapshot hash", "chunks", offer.Chunks, "skip_with", "-no-verify-hash")
		if err := verifySnapshotHash(ctx, snapDir, offer); err != nil {
			return fmt.Errorf("snapshot hash verify: %w", err)
		}
		s.log.Info("snapshot hash verified", "hash", hex.EncodeToString(offer.Hash))
	}

	return s.writeMeta(snapDir, offer, good, bytesTotal)
}

// buildP2PConfig centralizes snapfetch-specific overrides to cometbft's
// default P2PConfig.
func buildP2PConfig(maxOutbound int, allowDuplicateIP bool) *cfg.P2PConfig {
	p := cfg.DefaultP2PConfig()
	// Default false (cometbft default) rejects the eclipse vector
	// where one attacker IP fills many of our outbound slots.
	// Operators on shared-egress peer sets can set true via
	// [chains.<id>.fetch] allow_duplicate_ip.
	p.AllowDuplicateIP = allowDuplicateIP
	p.HandshakeTimeout = 5 * time.Second // drop slow peers fast (cometbft default: 20s)
	p.DialTimeout = 5 * time.Second      // same — fail fast over politeness
	p.MaxNumOutboundPeers = maxOutbound
	return p
}

// buildMConnConfig centralizes snapfetch-specific overrides to cometbft's
// default MConnConfig.
func buildMConnConfig() conn.MConnConfig {
	mConfig := conn.DefaultMConnConfig()
	mConfig.MaxPacketMsgPayloadSize = 256 * 1024 // cometbft's 1024B default is below some peers' framing size
	mConfig.SendRate = 10 * 1024 * 1024          // 500KB/s default caps us well below typical peer-side limits
	mConfig.RecvRate = 10 * 1024 * 1024          // matched to SendRate
	return mConfig
}
