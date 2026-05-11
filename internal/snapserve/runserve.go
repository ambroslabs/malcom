package snapserve

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	"github.com/cometbft/cometbft/version"

	"github.com/zrbecker/cosmos-p2p/internal/connect"
	"github.com/zrbecker/cosmos-p2p/internal/helpers/addrbook"
	"github.com/zrbecker/cosmos-p2p/internal/helpers/banlist"
	"github.com/zrbecker/cosmos-p2p/internal/helpers/nodekey"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/logctx"
	localpex "github.com/zrbecker/cosmos-p2p/internal/pex"
	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

// ErrNoPeers signals that BootstrapPeers was empty AND the addrbook
// loaded zero entries. Without anyone to dial, the node can still
// accept inbound connections but won't participate in PEX.
var ErrNoPeers = errors.New("no peers configured")

// ErrSnapshotSourceMissing means neither SnapshotDirs nor SnapshotsRoot
// is set; we have nothing to serve.
var ErrSnapshotSourceMissing = errors.New("set either SnapshotDirs (static list) or SnapshotsRoot (dir-watch); not neither")

// ErrSnapshotSourceConflict means the caller set both SnapshotDirs and
// SnapshotsRoot. The two modes have distinct semantics (strict-load vs
// scan-and-skip, no-rescan vs periodic-rescan) — picking one keeps
// behavior unambiguous.
var ErrSnapshotSourceConflict = errors.New("SnapshotDirs and SnapshotsRoot are mutually exclusive; set exactly one")

func validateConfig(c *Config) error {
	hasList := len(c.SnapshotDirs) > 0
	hasRoot := c.SnapshotsRoot != ""
	if hasList && hasRoot {
		return ErrSnapshotSourceConflict
	}
	if !hasList && !hasRoot {
		return ErrSnapshotSourceMissing
	}
	return nil
}

// RunServe is the library entry point. Loads + verifies the snapshot
// catalogue, builds the same p2p/PEX stack snapfetch uses, plugs the
// catalogue into the statesync reactor in serve mode, and blocks until
// ctx is cancelled (typically by a SIGINT/SIGTERM in the CLI wrapper).
//
// One of SnapshotDirs (static list) or SnapshotsRoot (auto-discovery
// + periodic rescan) must be set, not both. In SnapshotsRoot mode the
// reactor's catalogue is swapped lock-free on every successful rescan,
// without disconnecting peers — call ReloadSnapshots to trigger an
// immediate rescan from outside (the CLI hooks this to SIGHUP).
//
// On clean shutdown, the addrbook and banlist are persisted before
// returning. Nothing on disk under SnapshotDirs / SnapshotsRoot is
// mutated by serving.
func RunServe(ctx context.Context, c Config) error {
	c.applyDefaults()
	ctx = logctx.WithFields(ctx, "module", "serve")
	log := logctx.From(ctx)

	if err := validateConfig(&c); err != nil {
		return err
	}

	// One-shot warning at startup if integrity verification is off.
	// Lives here (not inside loadOne) so it doesn't fire per-snapshot
	// per-rescan in dir-watch mode.
	if c.VerifyMode == VerifyMetadataOnly {
		log.Warn("metadata-only verify; chunk integrity not checked",
			"hint", "set -verify=aggregate or -verify=per-chunk to re-hash on startup")
	}

	var (
		initialStore *Store
		catalog      *Catalog
	)
	switch {
	case c.SnapshotsRoot != "":
		// Build the catalog but don't start its rescan goroutine
		// until after sw.Start, so the initial reactor handoff and
		// the background rescan don't race during setup.
		catalog = NewCatalog(CatalogConfig{
			RootDir:        c.SnapshotsRoot,
			ChainID:        c.ChainID,
			VerifyMode:     c.VerifyMode,
			RescanInterval: c.RescanInterval,
			Logger:         log,
		})
		if err := catalog.Rescan(ctx); err != nil {
			return fmt.Errorf("initial scan of %s: %w", c.SnapshotsRoot, err)
		}
		initialStore = catalog.Current()
		log.Info("snapshot catalog ready",
			"mode", "dir-watch",
			"root", c.SnapshotsRoot,
			"rescan", c.RescanInterval,
			"snapshots", initialStore.Len(),
			"summary", initialStore.Describe(),
			"verify", c.VerifyMode)
		if initialStore.Len() == 0 {
			log.Warn("catalog is empty at startup — server is running but advertising nothing until snapshots are dropped under root",
				"root", c.SnapshotsRoot)
		}
	default:
		s, err := LoadStore(c.SnapshotDirs, c.ChainID, c.VerifyMode, log)
		if err != nil {
			return fmt.Errorf("load store: %w", err)
		}
		initialStore = s
		log.Info("snapshot catalog ready",
			"mode", "static",
			"snapshots", initialStore.Len(),
			"summary", initialStore.Describe(),
			"verify", c.VerifyMode)
	}

	nodeKey, err := nodekey.LoadOrGen(c.NodeKeyPath)
	if err != nil {
		return fmt.Errorf("node key: %w", err)
	}

	peerAddrs, err := buildServePeerAddrs(ctx, c)
	if err != nil {
		return err
	}

	listenAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), c.Listen))
	if err != nil {
		return fmt.Errorf("listen addr: %w", err)
	}
	log.Info("starting",
		"node_id", string(nodeKey.ID()),
		"listen", listenAddr.DialString(),
		"chain", c.ChainID,
		"peer_addrs", len(peerAddrs))

	// Same channel set as fetch (PEX + state-sync). Advertising the
	// state-sync channels is what tells well-behaved peers we'll
	// answer their SnapshotsRequest.
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
		return fmt.Errorf("nodeInfo invalid: %w", err)
	}

	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, buildMConnConfig())
	if err := transport.Listen(*listenAddr); err != nil {
		return fmt.Errorf("transport.Listen: %w", err)
	}

	ssR := statesync.NewReactor(malcomlog.CmtShim(log.With("module", "statesync")))
	ssR.SetProvider(initialStore)
	ssR.SetProbe(false) // serve mode: don't pester peers for their snapshots

	book, err := addrbook.NewAddrBook(c.AddrBook, malcomlog.CmtShim(log.With("module", "addrbook")))
	if err != nil {
		return fmt.Errorf("addrbook: %w", err)
	}
	bans, err := banlist.New(c.Banlist)
	if err != nil {
		return fmt.Errorf("banlist: %w", err)
	}
	log.Info("addrbook loaded", "path", c.AddrBook)
	log.Info("banlist loaded", "path", c.Banlist, "size", bans.Len())

	if selfAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), listenAddr.DialString())); err == nil {
		book.AddOurAddress(selfAddr)
	}

	res := addrbook.Populate(book, bans, peerAddrs)
	log.Info("addrbook ready",
		"path", c.AddrBook,
		"added", res.Added,
		"skipped_banned", res.SkippedBanned)

	sw := p2p.NewSwitch(buildP2PConfig(c.MaxOutboundPeers, c.AllowDuplicateIP), transport)
	sw.SetLogger(malcomlog.CmtShim(log.With("module", "p2p")))
	sw.SetNodeKey(nodeKey)
	sw.SetNodeInfo(nodeInfo)
	sw.SetAddrBook(book)

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
		BookDisabled:    c.PEXDisabled,
	})

	if !c.PEXDisabled {
		pexR := localpex.NewAutoReactor(book, localpex.AutoConfig{
			Banlist: bans,
			Kicker:  mgr,
		}, malcomlog.CmtShim(log.With("module", "pex")))
		sw.AddReactor("PEX", pexR)
	} else {
		log.Info("pex disabled: gossip dial pool restricted to bootstrap_peers")
	}
	sw.AddReactor("STATESYNC", ssR)

	if err := sw.Start(); err != nil {
		return fmt.Errorf("switch.Start: %w", err)
	}

	// Periodic persistence (#81): a crash/OOM/SIGKILL between clean
	// shutdowns loses every PEX-learned addr. Goroutine ticks at
	// c.PersistInterval, saves the addrbook + banlist. The clean-
	// shutdown defer below still runs a final save; this is the
	// crash-survivor.
	persistCtx, persistCancel := context.WithCancel(ctx)
	defer persistCancel()
	go runPersistLoop(persistCtx, c.PersistInterval,
		book.Save,
		bans.Save,
		log.With("module", "persist"))

	// In dir-watch mode, start the rescan goroutine now that the
	// reactor is running. OnStore is wired here (not at Catalog
	// construction) so we can capture ssR after sw.Start without
	// hoisting Catalog handling above the p2p setup.
	if catalog != nil {
		// Wire the reactor hand-off via SetOnStore (atomic.Pointer
		// underneath), so the loop goroutine and this setup goroutine
		// never share a plain field. We've already done an initial
		// Rescan synchronously above (so the reactor's initialStore
		// is non-nil before sw.Start); the loop just runs the
		// periodic + trigger cycle.
		catalog.SetOnStore(func(s *Store) {
			ssR.SetProvider(s)
			log.Info("reactor catalogue swapped",
				"snapshots", s.Len(), "summary", s.Describe())
		})
		go catalog.loop(ctx)
		defer catalog.Stop()
		if c.OnReloader != nil {
			c.OnReloader(catalog.Trigger)
		}
	}
	defer func() {
		// Graceful drain (#82): tell the reactor to fast-fail every
		// new ChunkRequest with Missing=true, then give in-flight
		// ChunkResponse sends c.ShutdownDrain to flush through their
		// MConnection send queues. Without this, sw.Stop closing
		// sockets mid-send leaves peers waiting on their per-chunk
		// timeout — they'd refetch eventually, but slowly, and the
		// partial bytes they received are wasted.
		//
		// We don't have a hook for "MConnection drained"; the budget
		// is just a fixed window. cometbft's send rate cap is 10 MiB/s
		// per peer, so 30s drains ~300 MiB per peer — comfortably
		// above one chunk (10 MiB).
		if c.ShutdownDrain > 0 {
			ssR.BeginShutdown()
			log.Info("draining in-flight chunks", "budget", c.ShutdownDrain)
			time.Sleep(c.ShutdownDrain)
			log.Info("drain window elapsed",
				"chunks_drained", ssR.Drained())
		}

		// Mirror snapfetch's shutdown ordering: addrbook + banlist
		// saves first (so a slow sw.Stop can't lose them), then
		// switch stop bounded by 1s, then manager.
		book.Save()
		if err := bans.Save(); err != nil {
			log.Error("save banlist failed", "path", c.Banlist, "err", err)
		} else {
			log.Info("banlist saved", "path", c.Banlist, "size", bans.Len())
		}
		stopped := make(chan struct{})
		go func() { _ = sw.Stop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(1 * time.Second):
			log.Debug("sw.Stop slow; skipping wait")
		}
		mgr.Stop()
	}()

	log.Info("serving",
		"listen", listenAddr.DialString(),
		"node_id", string(nodeKey.ID()),
		"snapshots", initialStore.Len())

	// Periodic served-counter logs so the operator has something to
	// look at without enabling debug. Cheap (atomic loads). Cadence
	// matches fetch's stat lines.
	stats := time.NewTicker(30 * time.Second)
	defer stats.Stop()

	for {
		select {
		case <-ctx.Done():
			snapshots, chunks, missing := ssR.Served()
			recv, sent := ssR.Bytes()
			log.Info("shutting down",
				"snapshots_served", snapshots,
				"chunks_served", chunks,
				"chunks_missing", missing,
				"bytes_recv", recv,
				"bytes_sent", sent)
			return nil
		case <-stats.C:
			snapshots, chunks, missing := ssR.Served()
			recv, sent := ssR.Bytes()
			out, in, dialing := sw.NumPeers()
			log.Info("stats",
				"peers_out", out, "peers_in", in, "dialing", dialing,
				"snapshots_served", snapshots,
				"chunks_served", chunks,
				"chunks_missing", missing,
				"chunks_drained", ssR.Drained(),
				"bytes_recv", recv,
				"bytes_sent", sent)
		}
	}
}

func buildServePeerAddrs(ctx context.Context, c Config) ([]addrbook.PeerAddr, error) {
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
		// In serve mode an empty pool isn't fatal — a node with a
		// public listen addr can still accept inbound connections,
		// it just won't participate in PEX outbound. Surface as a
		// warning and proceed.
		logctx.From(ctx).Warn("no bootstrap_peers and addrbook empty — serving inbound-only (no PEX gossip out)")
	}
	return peerAddrs, nil
}

func loadAddrbookPeers(ctx context.Context, addrBookPath string) []addrbook.PeerAddr {
	if addrBookPath == "" {
		return nil
	}
	items, err := addrbook.Load(addrBookPath)
	if err != nil {
		logctx.From(ctx).Error("load addrbook failed", "err", err)
		return nil
	}
	out := make([]addrbook.PeerAddr, 0, len(items))
	seen := map[string]bool{}
	for _, it := range items {
		s := it.String()
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, addrbook.PeerAddr{Addr: s, Source: addrbook.SourceAddrbook})
	}
	return out
}

// buildP2PConfig duplicates snapfetch.buildP2PConfig — the values are
// the same (we want fast handshake/dial timeouts and the same outbound
// peer cap behavior). Keeping it local here avoids a snapfetch import
// cycle.
func buildP2PConfig(maxOutbound int, allowDuplicateIP bool) *cfg.P2PConfig {
	p := cfg.DefaultP2PConfig()
	p.AllowDuplicateIP = allowDuplicateIP
	p.HandshakeTimeout = 5 * time.Second
	p.DialTimeout = 5 * time.Second
	p.MaxNumOutboundPeers = maxOutbound
	return p
}

// buildMConnConfig matches snapfetch's MConn tuning. The serve side
// also benefits from the larger packet payload (the chunks we ship
// are 10 MiB) and the higher Send/RecvRate (cometbft's defaults
// would throttle a 10 MiB chunk well below typical peer-side caps).
func buildMConnConfig() conn.MConnConfig {
	mConfig := conn.DefaultMConnConfig()
	mConfig.MaxPacketMsgPayloadSize = 256 * 1024
	mConfig.SendRate = 10 * 1024 * 1024
	mConfig.RecvRate = 10 * 1024 * 1024
	return mConfig
}
