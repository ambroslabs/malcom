package snapfetch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"log/slog"

	"github.com/cometbft/cometbft/p2p"
	pexcb "github.com/cometbft/cometbft/p2p/pex"
	"github.com/cometbft/cometbft/version"

	"github.com/ambroslabs/malcom/internal/connect"
	"github.com/ambroslabs/malcom/internal/durable"
	"github.com/ambroslabs/malcom/internal/helpers/addrbook"
	"github.com/ambroslabs/malcom/internal/helpers/banlist"
	"github.com/ambroslabs/malcom/internal/helpers/nodekey"
	"github.com/ambroslabs/malcom/internal/helpers/served"
	"github.com/ambroslabs/malcom/internal/humanbytes"
	malcomlog "github.com/ambroslabs/malcom/internal/log"
	"github.com/ambroslabs/malcom/internal/logctx"
	localpex "github.com/ambroslabs/malcom/internal/pex"
	"github.com/ambroslabs/malcom/internal/statesync"
)

// fetchSession bundles the long-lived state for one RunFetch call:
// switch, reactors, addrbook, banlist, connect.Manager, mux, peerWatch.
// Built once by newFetchSession; phase methods (walk, download, writeMeta)
// operate on this state.
type fetchSession struct {
	cfg       Config
	log       *slog.Logger
	sw        *p2p.Switch
	ssR       *statesync.Reactor
	book      pexcb.AddrBook
	bans      *banlist.Set
	srv       *served.Set
	mgr       *connect.Manager
	mux       *eventMux
	watch     *peerWatch
	peerAddrs []addrbook.PeerAddr
	nodeID    p2p.ID
}

// newFetchSession constructs every long-lived piece RunFetch needs:
// loads node key + peer addrs + addrbook + banlist, builds the switch
// + reactors, starts the switch, constructs the connect.Manager, sets
// up the event mux + peerWatch.
//
// Returns the session, a cleanup function (to be deferred), and any
// error. cleanup is safe to call even if a later phase fails — it
// runs the same shutdown sequence as a successful run.
func newFetchSession(ctx context.Context, c Config) (*fetchSession, func(), error) {
	c.applyDefaults()
	log := logctx.From(ctx)

	nodeKey, err := nodekey.LoadOrGen(c.NodeKeyPath)
	if err != nil {
		return nil, nil, fmt.Errorf("node key: %w", err)
	}

	peerAddrs, err := buildPeerAddrs(ctx, c)
	if err != nil {
		return nil, nil, err
	}
	addrbookCount := 0
	for _, p := range peerAddrs {
		if p.Source == addrbook.SourceAddrbook {
			addrbookCount++
		}
	}
	log.Info("starting",
		"node_id", string(nodeKey.ID()), "peer_addrs", len(peerAddrs))

	listenAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), c.Listen))
	if err != nil {
		return nil, nil, fmt.Errorf("listen addr: %w", err)
	}
	// Channels: PEX (0x00) lets us harvest addresses from peers via
	// cometbft's peer-exchange; state-sync (0x60/0x61) is what we're
	// here for. Advertising 0x00 is what makes well-behaved peers
	// reply to our PexRequest.
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      listenAddr.DialString(),
		Network:         c.ChainID,
		Version:         version.TMCoreSemVer,
		Channels:        []byte{localpex.Channel, statesync.SnapshotChannel, statesync.ChunkChannel},
		Moniker:         c.Moniker,
		Other:           p2p.DefaultNodeInfoOther{TxIndex: "off"},
	}
	if err := nodeInfo.Validate(); err != nil {
		return nil, nil, fmt.Errorf("nodeInfo invalid: %w", err)
	}

	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, buildMConnConfig())
	if err := transport.Listen(*listenAddr); err != nil {
		return nil, nil, fmt.Errorf("transport.Listen: %w", err)
	}
	ssR := statesync.NewReactor(malcomlog.CmtShim(log.With("module", "statesync")))

	// AddrBook holds peer addresses learned via PEX (and seeded with
	// our bootstrap_peers list at startup). cometbft's implementation —
	// JSON-persistent, bucket-balanced, freshness-tracked.
	book, err := addrbook.NewAddrBook(c.AddrBook, malcomlog.CmtShim(log.With("module", "addrbook")))
	if err != nil {
		return nil, nil, fmt.Errorf("addrbook: %w", err)
	}

	// Banlist: persistent cross-run record of addresses that repeatedly
	// fail to dial. Without this, PEX gossip would re-introduce banned
	// peers on every run, costing a fresh MaxDialFailures cycle per
	// stale gossip.
	bans, err := banlist.New(c.Banlist)
	if err != nil {
		return nil, nil, fmt.Errorf("banlist: %w", err)
	}
	// Served-peers list: peers that have served verified chunks across
	// prior runs. Loaded into the dial pool below + Pinned with the
	// manager so they get redial priority from t=0.
	srv, err := served.New(c.Served)
	if err != nil {
		return nil, nil, fmt.Errorf("served: %w", err)
	}
	log.Info("addrbook loaded", "path", c.AddrBook, "size", addrbookCount)
	log.Info("banlist loaded", "path", c.Banlist, "size", bans.Len())
	log.Info("served loaded", "path", c.Served, "size", srv.Len())

	// Don't dial ourselves.
	if selfAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), listenAddr.DialString())); err == nil {
		book.AddOurAddress(selfAddr)
	}

	// Populate is our addrbook load step. We use it instead of cometbft's
	// auto-load (loadFromFile, which would fire if we called book.Start)
	// because loadFromFile doesn't apply our banlist:
	//   - addrbook-sourced addrs that are in the banlist must be skipped.
	//   - bootstrap addrs clear any existing banlist entry (explicit
	//     user re-allow).
	// loadFromFile would re-add banned entries indiscriminately. Cost we
	// accept: AddAddress creates fresh knownAddress records, so we lose
	// per-entry history (LastSuccess, Attempts, bucket placement) that
	// loadFromFile would have restored.
	res := addrbook.Populate(book, bans, peerAddrs)
	log.Info("addrbook ready", "path", c.AddrBook,
		"added", res.Added, "skipped_banned", res.SkippedBanned)

	sw := p2p.NewSwitch(buildP2PConfig(c.MaxOutboundPeers, c.AllowDuplicateIP), transport)
	sw.SetLogger(malcomlog.CmtShim(log.With("module", "p2p")))
	sw.SetNodeKey(nodeKey)
	sw.SetNodeInfo(nodeInfo)
	sw.SetAddrBook(book)

	// connect.Manager owns all outbound dialing for the run:
	//   - warm-fill from the static pool, then from the cometbft
	//     addrbook via PickAddress (replaces PEX's old dial loop and
	//     the old runKeepWarm goroutine);
	//   - pinned-redial for peers that download/peerWatch care about
	//     (replaces download.tryRedial);
	//   - per-addr dial-failure tracking with book.RemoveAddress +
	//     banlist.Add on threshold (replaces PEX's onDialFail).
	// Constructed before the PEX reactor so we can hand it in as the
	// gossip Kicker — every non-empty PexAddrs triggers an immediate
	// dial wave instead of waiting for the next RefreshTick.
	// Prepend served-peers entries to the dial pool. The pool's cursor
	// walks bootstrap entries first (their head-of-pool position is
	// preserved by buildPool); served entries go even further ahead so
	// the very first dial wave hits known chunk-servers.
	//
	// srv.All() returns entries newest-first by LastSeenAt. We preserve
	// that order so the manager's cursor dials the most recently-useful
	// peers before older ones.
	servedAll := srv.All()
	prefix := make([]addrbook.PeerAddr, 0, len(servedAll))
	for _, e := range servedAll {
		prefix = append(prefix, addrbook.PeerAddr{Addr: e.Addr, Source: addrbook.SourceBootstrap})
	}
	pool := append(prefix, peerAddrs...)

	mgr := connect.New(ctx, connect.Config{
		Switch:          sw,
		Book:            book,
		Banlist:         bans,
		Pool:            pool,
		WarmTarget:      c.PEXTargetPeers,
		DialBatch:       c.PEXMaxPerWave,
		RefreshTick:     c.WarmRefreshInterval,
		BookBias:        50,
		Backoff:         c.PeerRedialBackoff,
		MaxBackoff:      c.MaxRedialBackoff,
		MaxRedials:      c.MaxRedials,
		MaxDialFailures: c.MaxDialFailures,
		BanDuration:     c.AddrBookBanDuration,
		BookDisabled:    c.PEXDisabled,
	})

	// Pin served peers so the manager actively redials them on every
	// disconnect — same priority as walk-discovered "good" peers, but
	// from t=0. New peers learned during the run get pinned via the
	// existing addProvisional path.
	for _, e := range srv.All() {
		mgr.Pin(p2p.ID(e.ID), e.Addr)
	}

	// PEX reactor: sends PexRequest on every AddPeer and writes
	// banlist-filtered PexAddrs into the book. Dialing is owned by
	// connect.Manager — this reactor is gossip-only, but it kicks
	// the manager after each gossip so freshly-learned addrs get
	// dialed before the next 5s tick.
	//
	// In curated-peers mode (PEXDisabled), we skip wiring the PEX
	// reactor entirely: no outbound PexRequest, no PexAddrs gossip
	// processed, no addrbook growth. The manager's BookDisabled flag
	// also blocks warm-fill from drawing book entries, so the only
	// dial source is the static bootstrap_peers pool.
	if !c.PEXDisabled {
		pexR := localpex.NewAutoReactor(book, localpex.AutoConfig{
			Banlist: bans,
			Kicker:  mgr,
		}, malcomlog.CmtShim(log.With("module", "pex")))
		sw.AddReactor("PEX", pexR)
	} else {
		log.Info("pex disabled (curated-peers mode): warm-fill draws only from bootstrap_peers")
	}
	sw.AddReactor("STATESYNC", ssR)

	if err := sw.Start(); err != nil {
		return nil, nil, fmt.Errorf("switch.Start: %w", err)
	}

	mux := newEventMux(ctx, ssR.Out, ssR.OutChunks)

	// peerWatch: long-lived churn loop. Spans walk + download — drops
	// peers that don't advertise anything in our freshness window, and
	// addrbook-bans them so PEX picks fresher candidates. Started here
	// so churn pressure is on the peer set from the moment we start
	// collecting offers.
	watchCtx, watchCancel := context.WithCancel(ctx)
	watch := newPeerWatch(watchCtx, sw, book, mgr,
		c.MinHeight, c.ChurnGrace, c.AddrBookBanDuration, c.RequireStateSyncChannel)
	go watch.run(watchCtx, mux.subscribeCtrl())

	s := &fetchSession{
		cfg:       c,
		log:       log,
		sw:        sw,
		ssR:       ssR,
		book:      book,
		bans:      bans,
		srv:       srv,
		mgr:       mgr,
		mux:       mux,
		watch:     watch,
		peerAddrs: peerAddrs,
		nodeID:    nodeKey.ID(),
	}

	// Cleanup matches the shutdown order we used to express via stacked
	// defers (LIFO). Order matters: watchCancel first so the watch
	// goroutine settles before the mux closes; book/bans saves before
	// switch stop so persistence wins regardless of how long sw.Stop
	// takes; book before bans because the addrbook is the primary
	// source of dial candidates — losing the banlist on a save error
	// is a soft regression (PEX gossip will re-introduce stale peers
	// for one MaxDialFailures cycle), losing the addrbook is a cold
	// restart; sw.Stop bounded by a 1s deadline because cometbft's
	// clean per-peer disconnect can take 5-10s and the OS reaps
	// sockets on process exit anyway — without the bound, fail-fast
	// paths (walk finds nothing, ctx cancelled mid-run) look like a
	// hang; manager last because it polls the switch and we want it
	// quiet during shutdown.
	cleanup := func() {
		watchCancel()
		mux.stop()
		book.Save()
		if err := bans.Save(); err != nil {
			log.Error("save banlist failed", "path", c.Banlist, "err", err)
		} else {
			log.Info("banlist saved", "path", c.Banlist, "size", bans.Len())
		}
		if err := srv.Save(); err != nil {
			log.Error("save served failed", "path", c.Served, "err", err)
		} else {
			log.Info("served saved", "path", c.Served, "size", srv.Len())
		}
		stopped := make(chan struct{})
		go func() { _ = sw.Stop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(1 * time.Second):
			log.Debug("sw.Stop slow; skipping wait")
		}
		mgr.Stop()
	}
	return s, cleanup, nil
}

// buildPeerAddrs loads addrs from a previous addrbook.json and prepends
// any user-supplied bootstrap_peers CSV entries. Errors if the
// combined list is empty.
//
// In curated-peers mode (PEXDisabled), the addrbook is intentionally
// excluded so the manager's static dial pool is exactly bootstrap_peers
// — nothing more.
func buildPeerAddrs(ctx context.Context, c Config) ([]addrbook.PeerAddr, error) {
	var peerAddrs []addrbook.PeerAddr
	if !c.PEXDisabled {
		peerAddrs = loadAddrbookPeers(ctx, c.AddrBook)
	}
	for _, s := range c.BootstrapPeers {
		s = strings.TrimSpace(s)
		if s != "" {
			peerAddrs = append([]addrbook.PeerAddr{{Addr: s, Source: addrbook.SourceBootstrap}}, peerAddrs...)
		}
	}
	if len(peerAddrs) == 0 {
		return nil, fmt.Errorf("%w: no peer addrs available — %s", ErrNoPeers, hintNoPeerAddrs(c.ChainID))
	}
	return peerAddrs, nil
}

// walk runs the height-discovery walk against the peer set the manager
// has been warming up. Returns the chosen offer + a starter "good
// peers" list (chunk-0 responder + everyone the offer was advertised
// by) + the verified chunk-0 bytes (so the download phase can seed
// chunk_00000.bin instead of refetching).
func (s *fetchSession) walk(ctx context.Context) (*snapshotOffer, []p2p.ID, []byte, error) {
	return walkBackward(ctx, s.sw, s.ssR, s.mux, s.watch, s.peerAddrs, s.cfg)
}

// prepareSnapshotDir parses chunk hashes from the chosen offer's
// metadata, validates the count matches Chunks, creates the output
// directory, and writes metadata.bin (chunk goroutines write into the
// same dir concurrently and rely on it existing).
func (s *fetchSession) prepareSnapshotDir(outRoot string, offer *snapshotOffer) (string, [][]byte, error) {
	chunkHashes, err := parseChunkHashes(offer.Metadata)
	if err != nil {
		return "", nil, fmt.Errorf("parse chunk_hashes: %w", err)
	}
	if uint32(len(chunkHashes)) != offer.Chunks {
		return "", nil, fmt.Errorf("metadata mismatch: chunk_hashes=%d expected=%d",
			len(chunkHashes), offer.Chunks)
	}
	snapDir := filepath.Join(outRoot, fmt.Sprintf("snapshot_%s_%d", s.cfg.ChainID, offer.Height))
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return "", nil, fmt.Errorf("%w: mkdir snapshot dir: %w", ErrDiskFailed, err)
	}
	// Make outRoot's directory entry for snapDir durable, so post-reboot
	// `ls outRoot` agrees with the durable contents inside snapDir.
	if err := durable.FsyncDir(outRoot); err != nil {
		return "", nil, fmt.Errorf("%w: fsync outRoot: %w", ErrDiskFailed, err)
	}
	// Reuse a matching metadata.bin from a prior partial fetch: if the
	// on-disk content already equals offer.Metadata byte-for-byte, the
	// rewrite would be a no-op (and would needlessly rotate the file's
	// mtime and inode). A mismatch — or a missing file — falls back to
	// the atomic rewrite path.
	mdPath := filepath.Join(snapDir, "metadata.bin")
	if existing, err := os.ReadFile(mdPath); err != nil || !bytes.Equal(existing, offer.Metadata) {
		if err := durable.WriteFile(mdPath, offer.Metadata, 0o644); err != nil {
			return "", nil, fmt.Errorf("%w: write metadata.bin: %w", ErrDiskFailed, err)
		}
	}
	return snapDir, chunkHashes, nil
}

// download runs the chunk-fetch phase. The session-owned mux gives
// download its own subscription so it doesn't share an event channel
// with peerWatch / walk.
func (s *fetchSession) download(ctx context.Context, offer *snapshotOffer, good []p2p.ID, chunkHashes [][]byte, snapDir string) (uint64, error) {
	return download(ctx, s.sw, s.ssR, s.mux.subscribe(), s.cfg.ChainID,
		offer, chunkHashes, good, snapDir,
		s.cfg.PerPeerLimit, s.cfg.ChunkTimeout, s.cfg.PeerFailLimit,
		s.cfg.ProvisionalProbeStrikes, s.cfg.ProvisionalProbeInflight,
		s.cfg.MaxDiskWriteFailures,
		s.watch, s.mgr, s.srv, s.cfg.OnChunkReady)
}

// verifySnapshotHash recomputes the wire-level snapshot hash from the
// chunk files on disk and compares it to offer.Hash. cosmos-sdk's
// snapshotter sets Snapshot.Hash = SHA256(chunk_0 || ... || chunk_{N-1})
// during snapshot creation in store/snapshots/store.go's Save.
//
// Complements the per-chunk verify in download.onChunk, which proves
// chunks match metadata.chunk_hashes. Per-chunk alone leaves a gap:
// offerSet.add caches the first peer's metadata for each
// (height, format, hash) key, so a peer racing first with forged
// metadata + matching forged chunks passes per-chunk and only fails
// the aggregate check here.
//
// ctx is checked between chunks so Ctrl+C interrupts the trailing
// pass instead of waiting for it to finish; per-chunk reads are
// non-cancellable but bounded (~100 ms each on the cold path).
func verifySnapshotHash(ctx context.Context, snapDir string, offer *snapshotOffer) error {
	h := sha256.New()
	buf := make([]byte, 1<<20)
	for i := uint32(0); i < offer.Chunks; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := filepath.Join(snapDir, fmt.Sprintf("chunk_%05d.bin", i))
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("%w: open chunk %d: %w", ErrDiskFailed, i, err)
		}
		if _, err := io.CopyBuffer(h, f, buf); err != nil {
			f.Close()
			return fmt.Errorf("%w: read chunk %d: %w", ErrDiskFailed, i, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("%w: close chunk %d: %w", ErrDiskFailed, i, err)
		}
	}
	got := h.Sum(nil)
	if !bytes.Equal(got, offer.Hash) {
		return fmt.Errorf("snapshot hash mismatch: got %s, want %s — %s",
			hex.EncodeToString(got), hex.EncodeToString(offer.Hash),
			hintHashMismatch)
	}
	return nil
}

// writeMeta writes meta.json and the .complete sentinel in snapDir.
// Logs the final summary on success.
func (s *fetchSession) writeMeta(snapDir string, offer *snapshotOffer, good []p2p.ID, bytesTotal uint64) error {
	offered := make([]string, 0, len(offer.Peers))
	for p := range offer.Peers {
		offered = append(offered, p)
	}
	sort.Strings(offered)
	goodStr := make([]string, 0, len(good))
	for _, p := range good {
		goodStr = append(goodStr, string(p))
	}
	sort.Strings(goodStr)

	meta := savedMeta{
		ChainID:         s.cfg.ChainID,
		Height:          offer.Height,
		Format:          offer.Format,
		Chunks:          offer.Chunks,
		HashHex:         hex.EncodeToString(offer.Hash),
		MetadataLen:     len(offer.Metadata),
		GoodPeers:       goodStr,
		OfferedBy:       offered,
		DownloadedAt:    time.Now().UTC(),
		BytesTotal:      bytesTotal,
		BytesTotalHuman: humanbytes.Format(bytesTotal),
	}
	if err := writeJSONAtomic(filepath.Join(snapDir, "meta.json"), meta); err != nil {
		return fmt.Errorf("%w: write meta.json: %w", ErrDiskFailed, err)
	}
	// Make every prior rename in snapDir (chunks, metadata.bin, meta.json)
	// durable before .complete lands, so a crash can never leave the
	// sentinel present alongside a torn predecessor.
	if err := durable.FsyncDir(snapDir); err != nil {
		return fmt.Errorf("%w: fsync snapshot dir: %w", ErrDiskFailed, err)
	}
	if err := durable.WriteFile(filepath.Join(snapDir, ".complete"), nil, 0o644); err != nil {
		return fmt.Errorf("%w: mark complete: %w", ErrDiskFailed, err)
	}
	if err := durable.FsyncDir(snapDir); err != nil {
		return fmt.Errorf("%w: fsync snapshot dir after .complete: %w", ErrDiskFailed, err)
	}
	s.log.Info("snapshot saved", "dir", snapDir)
	s.log.Info("download complete",
		"height", offer.Height, "format", offer.Format,
		"chunks", offer.Chunks, "bytes", humanbytes.Format(bytesTotal))
	return nil
}
