package snapfetch

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	pexcb "github.com/cometbft/cometbft/p2p/pex"
	"github.com/cometbft/cometbft/version"

	"github.com/zrbecker/cosmos-p2p/internal/connect"
	"github.com/zrbecker/cosmos-p2p/internal/helpers/addrbook"
	"github.com/zrbecker/cosmos-p2p/internal/helpers/banlist"
	"github.com/zrbecker/cosmos-p2p/internal/helpers/nodekey"
	"github.com/zrbecker/cosmos-p2p/internal/logctx"
	localpex "github.com/zrbecker/cosmos-p2p/internal/pex"
	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

// fetchSession bundles the long-lived state for one RunFetch call:
// switch, reactors, addrbook, banlist, connect.Manager, mux, peerWatch.
// Built once by newFetchSession; phase methods (walk, download, writeMeta)
// operate on this state.
type fetchSession struct {
	cfg       Config
	log       cmtlog.Logger
	sw        *p2p.Switch
	ssR       *statesync.Reactor
	book      pexcb.AddrBook
	bans      *banlist.Set
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
	ssR := statesync.NewReactor(log.With("module", "statesync"))

	// AddrBook holds peer addresses learned via PEX (and seeded with
	// our bootstrap_peers list at startup). cometbft's implementation —
	// JSON-persistent, bucket-balanced, freshness-tracked.
	book, err := addrbook.NewAddrBook(c.AddrBook, log.With("module", "addrbook"))
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
	log.Info("addrbook loaded", "path", c.AddrBook, "size", addrbookCount)
	log.Info("banlist loaded", "path", c.Banlist, "size", bans.Len())

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

	sw := p2p.NewSwitch(buildP2PConfig(c.MaxOutboundPeers), transport)
	sw.SetLogger(log.With("module", "p2p"))
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
	mgr := connect.New(ctx, connect.Config{
		Switch:          sw,
		Book:            book,
		Banlist:         bans,
		Pool:            peerAddrs,
		WarmTarget:      c.PEXTargetPeers,
		DialBatch:       c.PEXMaxPerWave,
		RefreshTick:     c.WarmRefreshInterval,
		BookBias:        50,
		Backoff:         c.PeerRedialBackoff,
		MaxBackoff:      c.MaxRedialBackoff,
		MaxRedials:      c.MaxRedials,
		MaxDialFailures: c.MaxDialFailures,
		BanDuration:     c.AddrBookBanDuration,
	})

	// PEX reactor: sends PexRequest on every AddPeer and writes
	// banlist-filtered PexAddrs into the book. Dialing is owned by
	// connect.Manager — this reactor is gossip-only, but it kicks
	// the manager after each gossip so freshly-learned addrs get
	// dialed before the next 5s tick.
	pexR := localpex.NewAutoReactor(book, localpex.AutoConfig{
		Banlist: bans,
		Kicker:  mgr,
	}, log.With("module", "pex"))

	sw.AddReactor("PEX", pexR)
	sw.AddReactor("STATESYNC", ssR)

	if err := sw.Start(); err != nil {
		return nil, nil, fmt.Errorf("switch.Start: %w", err)
	}

	mux := newEventMux(ctx, ssR.Out)

	// peerWatch: long-lived churn loop. Spans walk + download — drops
	// peers that don't advertise anything in our freshness window, and
	// addrbook-bans them so PEX picks fresher candidates. Started here
	// so churn pressure is on the peer set from the moment we start
	// collecting offers.
	watchCtx, watchCancel := context.WithCancel(ctx)
	watch := newPeerWatch(watchCtx, sw, book, mgr,
		c.MinHeight, c.ChurnGrace, c.AddrBookBanDuration, c.RequireStateSyncChannel)
	go watch.run(watchCtx, mux.subscribe())

	s := &fetchSession{
		cfg:       c,
		log:       log,
		sw:        sw,
		ssR:       ssR,
		book:      book,
		bans:      bans,
		mgr:       mgr,
		mux:       mux,
		watch:     watch,
		peerAddrs: peerAddrs,
		nodeID:    nodeKey.ID(),
	}

	// Cleanup matches the shutdown order we used to express via stacked
	// defers (LIFO). Order matters: watchCancel first so the watch
	// goroutine settles before the mux closes; bans/book saves before
	// switch stop so persistence wins regardless of how long sw.Stop
	// takes; sw.Stop skipped on ctx.Err to give Ctrl-C a fast exit
	// (cometbft's clean per-peer disconnect can take 5-10s, OS reaps
	// sockets on process exit anyway); manager last because it polls
	// the switch and we want it quiet during shutdown.
	cleanup := func() {
		watchCancel()
		mux.stop()
		if err := bans.Save(); err != nil {
			log.Error("save banlist failed", "path", c.Banlist, "err", err)
		} else {
			log.Info("banlist saved", "path", c.Banlist, "size", bans.Len())
		}
		book.Save()
		if ctx.Err() == nil {
			_ = sw.Stop()
		}
		mgr.Stop()
	}
	return s, cleanup, nil
}

// buildPeerAddrs loads addrs from a previous addrbook.json and prepends
// any user-supplied bootstrap_peers CSV entries. Errors if the
// combined list is empty.
func buildPeerAddrs(ctx context.Context, c Config) ([]addrbook.PeerAddr, error) {
	peerAddrs := loadAddrbookPeers(ctx, c.AddrBook)
	for _, s := range c.BootstrapPeers {
		s = strings.TrimSpace(s)
		if s != "" {
			peerAddrs = append([]addrbook.PeerAddr{{Addr: s, Source: addrbook.SourceBootstrap}}, peerAddrs...)
		}
	}
	if len(peerAddrs) == 0 {
		return nil, fmt.Errorf("no peer addrs available")
	}
	return peerAddrs, nil
}

// walk runs the height-discovery walk against the peer set the manager
// has been warming up. Returns the chosen offer + a starter "good
// peers" list (chunk-0 responder + everyone the offer was advertised by).
func (s *fetchSession) walk(ctx context.Context) (*snapshotOffer, []p2p.ID, error) {
	return walkBackward(ctx, s.sw, s.ssR, s.mux, s.peerAddrs, s.cfg)
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
		return "", nil, fmt.Errorf("mkdir snapshot dir: %w", err)
	}
	// Make outRoot's directory entry for snapDir durable, so post-reboot
	// `ls outRoot` agrees with the durable contents inside snapDir.
	if err := fsyncDir(outRoot); err != nil {
		return "", nil, fmt.Errorf("fsync outRoot: %w", err)
	}
	if err := writeFileAtomic(filepath.Join(snapDir, "metadata.bin"), offer.Metadata, 0o644); err != nil {
		return "", nil, fmt.Errorf("write metadata.bin: %w", err)
	}
	return snapDir, chunkHashes, nil
}

// download runs the chunk-fetch phase. The session-owned mux gives
// download its own subscription so it doesn't share an event channel
// with peerWatch / walk.
func (s *fetchSession) download(ctx context.Context, offer *snapshotOffer, good []p2p.ID, chunkHashes [][]byte, snapDir string) (uint64, error) {
	return download(ctx, s.sw, s.ssR, s.mux.subscribe(),
		offer, chunkHashes, good, snapDir,
		s.cfg.PerPeerLimit, s.cfg.ChunkTimeout, s.cfg.PeerFailLimit,
		s.cfg.ProvisionalProbeStrikes, s.cfg.ProvisionalProbeInflight,
		s.watch, s.mgr)
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
		Height:          offer.Height,
		Format:          offer.Format,
		Chunks:          offer.Chunks,
		HashHex:         hex.EncodeToString(offer.Hash),
		MetadataLen:     len(offer.Metadata),
		GoodPeers:       goodStr,
		OfferedBy:       offered,
		DownloadedAt:    time.Now().UTC(),
		BytesTotal:      bytesTotal,
		BytesTotalHuman: humanBytes(bytesTotal),
	}
	if err := writeJSONAtomic(filepath.Join(snapDir, "meta.json"), meta); err != nil {
		return fmt.Errorf("write meta.json: %w", err)
	}
	// Make every prior rename in snapDir (chunks, metadata.bin, meta.json)
	// durable before .complete lands, so a crash can never leave the
	// sentinel present alongside a torn predecessor.
	if err := fsyncDir(snapDir); err != nil {
		return fmt.Errorf("fsync snapshot dir: %w", err)
	}
	if err := writeFileAtomic(filepath.Join(snapDir, ".complete"), nil, 0o644); err != nil {
		return fmt.Errorf("mark complete: %w", err)
	}
	if err := fsyncDir(snapDir); err != nil {
		return fmt.Errorf("fsync snapshot dir after .complete: %w", err)
	}
	s.log.Info("snapshot saved", "dir", snapDir)
	s.log.Info("download complete",
		"height", offer.Height, "format", offer.Format,
		"chunks", offer.Chunks, "bytes", humanBytes(bytesTotal))
	return nil
}
