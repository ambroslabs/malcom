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

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	pexcb "github.com/cometbft/cometbft/p2p/pex"
	"github.com/cometbft/cometbft/version"

	"github.com/zrbecker/cosmos-p2p/internal/helpers/nodekey"
	"github.com/zrbecker/cosmos-p2p/internal/logctx"
	"github.com/zrbecker/cosmos-p2p/internal/peers/dead"
	localpex "github.com/zrbecker/cosmos-p2p/internal/pex"
	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

// RunFetch is the library entry point. It builds a p2p.Switch, walks
// candidate heights, downloads chunks, and writes everything under
// <outRoot>/snapshot_<chain>_<height>/ (chunks + metadata.bin +
// meta.json + .complete marker).
//
// On error any partial output under outRoot is left in place — no
// .complete marker is written, so the cli can detect incomplete dirs
// and the user can inspect / remove them.
func RunFetch(ctx context.Context, c Config, outRoot string) error {
	c.applyDefaults()
	ctx = logctx.WithFields(ctx, "module", "fetch")
	log := logctx.From(ctx)

	nodeKey, err := nodekey.LoadOrGen(c.NodeKeyPath)
	if err != nil {
		return fmt.Errorf("node key: %w", err)
	}

	peerAddrs := loadAddrbookPeers(ctx, c.AddrBook)
	addrbookCount := len(peerAddrs)
	for _, s := range strings.Split(c.BootstrapPeersCSV, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			peerAddrs = append([]peerAddr{{addr: s, source: "bootstrap"}}, peerAddrs...)
		}
	}
	if len(peerAddrs) == 0 {
		return fmt.Errorf("no peer addrs available")
	}
	addrByNodeID := map[string]string{}
	for _, s := range peerAddrs {
		parts := strings.SplitN(s.addr, "@", 2)
		if len(parts) == 2 {
			addrByNodeID[parts[0]] = s.addr
		}
	}
	log.Info("starting",
		"node_id", string(nodeKey.ID()), "peer_addrs", len(peerAddrs))

	listenAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), c.Listen))
	if err != nil {
		return fmt.Errorf("listen addr: %w", err)
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
		return fmt.Errorf("nodeInfo invalid: %w", err)
	}
	p2pConfig := buildP2PConfig()
	p2pConfig.MaxNumOutboundPeers = c.MaxOutboundPeers
	mConfig := buildMConnConfig()

	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, mConfig)
	if err := transport.Listen(*listenAddr); err != nil {
		return fmt.Errorf("transport.Listen: %w", err)
	}
	ssR := statesync.NewReactor(log.With("module", "statesync"))
	ssR.KeepBytes = true

	// AddrBook holds peer addresses learned via PEX (and seeded with
	// our bootstrap_peers list at startup). cometbft's implementation —
	// JSON-persistent, bucket-balanced, freshness-tracked.
	bookPath := c.AddrBook
	if bookPath == "" {
		bookPath = filepath.Join(filepath.Dir(c.NodeKeyPath), "addrbook.json")
	}
	if err := os.MkdirAll(filepath.Dir(bookPath), 0o755); err != nil {
		return fmt.Errorf("mkdir addrbook dir: %w", err)
	}
	book := pexcb.NewAddrBook(bookPath, false /* routabilityStrict */)
	book.SetLogger(log.With("module", "addrbook"))

	// Dead-peer set: persistent cross-run tombstone for addresses that
	// repeatedly fail to dial. Lives next to addrbook.json. Without
	// this, PEX gossip would re-introduce known-dead peers on every
	// run, costing a fresh MaxDialFailures cycle per stale gossip.
	deadPath := filepath.Join(filepath.Dir(bookPath), "deadpeers.json")
	deadSet := dead.New(deadPath)
	if err := deadSet.Load(); err != nil {
		log.Error("load dead-peers failed", "path", deadPath, "err", err)
	}
	log.Info("addrbook loaded", "path", bookPath, "size", addrbookCount)
	log.Info("dead-peers loaded", "path", deadPath, "size", deadSet.Len())

	// PEX reactor: sends PexRequest on every AddPeer, writes received
	// PexAddrs to the book, and runs a dial loop that grows the
	// connected-peer set toward TargetPeers in parallel waves. ~30×
	// more aggressive than cometbft's ensurePeers default — we're a
	// one-shot fetcher, not a long-running node.
	pexR := localpex.NewAutoReactor(book, localpex.AutoConfig{
		TargetPeers:        c.PEXTargetPeers,
		MaxPerWave:         c.PEXMaxPerWave,
		DialInterval:       2 * time.Second,
		BookBias:           50,
		MaxDialFailures:    c.MaxDialFailures,
		FailureBanDuration: 5 * time.Minute,
		DeadPeers:          deadSet,
	}, log.With("module", "pex"))

	sw := p2p.NewSwitch(p2pConfig, transport)
	sw.SetLogger(log.With("module", "p2p"))
	sw.SetNodeKey(nodeKey)
	sw.SetNodeInfo(nodeInfo)
	sw.SetAddrBook(book)
	sw.AddReactor("PEX", pexR)
	sw.AddReactor("STATESYNC", ssR)

	// Don't dial ourselves.
	if selfAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), listenAddr.DialString())); err == nil {
		book.AddOurAddress(selfAddr)
	}

	// Pre-populate the book with our peer addrs (bootstrap CSV entries
	// from chain-registry, plus any addrs loaded from a previous
	// addrbook.json). Source = peer's own address (it "told us about
	// itself"). Addrbook-sourced addrs are filtered against the dead
	// set; user-supplied bootstrap addrs are not (the user explicitly
	// named them, so they get a fresh chance and any prior dead-set
	// entry is cleared).
	added := 0
	skippedDead := 0
	for _, s := range peerAddrs {
		if s.source == "addrbook" && deadSet.Has(s.addr) {
			skippedDead++
			continue
		}
		if s.source == "bootstrap" {
			deadSet.Remove(s.addr)
		}
		na, err := p2p.NewNetAddressString(s.addr)
		if err != nil {
			continue
		}
		if err := book.AddAddress(na, na); err == nil {
			added++
		}
	}
	log.Info("addrbook ready", "path", bookPath, "added", added, "skipped_dead", skippedDead)

	if err := sw.Start(); err != nil {
		return fmt.Errorf("switch.Start: %w", err)
	}
	// Defers run LIFO. book.Save first (fast, JSON dump), then sw.Stop
	// — but skip sw.Stop on ctx cancel: cometbft's clean peer-disconnect
	// can take 5-10s with many peers, and the OS reaps the TCP sockets
	// regardless when the process exits.
	defer func() {
		if ctx.Err() != nil {
			return
		}
		_ = sw.Stop()
	}()
	defer book.Save()
	defer func() {
		if err := deadSet.Save(); err != nil {
			log.Error("save dead-peers failed", "path", deadPath, "err", err)
			return
		}
		log.Info("dead-peers saved", "path", deadPath, "size", deadSet.Len())
	}()

	mux := newEventMux(ctx, ssR.Out)
	defer mux.stop()

	// peerWatch: long-lived churn loop. Spans both walking and
	// downloading phases — drops peers that don't advertise anything
	// in our freshness window, and addrbook-bans them so PEX picks
	// fresher candidates. Started here so churn pressure is on the
	// peer set from the moment we start collecting offers.
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := newPeerWatch(watchCtx, sw, book, c.MinHeight, c.ChurnGrace, c.AddrBookBanDuration, c.RequireStateSyncChannel)
	go watch.run(watchCtx, mux.subscribe())

	var (
		chosen      *snapshotOffer
		goodPeers   []p2p.ID
		bytesTotal  uint64
		chunkHashes [][]byte
	)

	// Walk: dial peer addrs, warm up, then probe target heights in
	// descending order until one peer serves chunk-0. No rescan
	// loop — if the walk exhausts the freshness window, error out.
	chosen, goodPeers, err = walkBackward(ctx, sw, ssR, mux, peerAddrs,
		c, addrByNodeID)
	if err != nil {
		return err
	}

	chunkHashes, err = parseChunkHashes(chosen.Metadata)
	if err != nil {
		return fmt.Errorf("parse chunk_hashes: %w", err)
	}
	if uint32(len(chunkHashes)) != chosen.Chunks {
		return fmt.Errorf("metadata mismatch: chunk_hashes=%d expected=%d",
			len(chunkHashes), chosen.Chunks)
	}

	// Create the output dir and write metadata.bin before any chunk
	// arrives — chunk goroutines write into snapDir concurrently and
	// rely on it existing.
	snapDir := filepath.Join(outRoot, fmt.Sprintf("snapshot_%s_%d", c.ChainID, chosen.Height))
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return fmt.Errorf("mkdir snapshot dir: %w", err)
	}
	if err := os.WriteFile(filepath.Join(snapDir, "metadata.bin"), chosen.Metadata, 0o644); err != nil {
		return fmt.Errorf("write metadata.bin: %w", err)
	}

	// ─── Download all chunks ──────────────────────────────────────────
	fetchCtx, fetchCancel := context.WithTimeout(ctx, c.MaxFetchTime)
	bt, derr := download(fetchCtx, sw, ssR, mux.subscribe(),
		chosen, chunkHashes, goodPeers, addrByNodeID, snapDir, peerAddrs,
		c.PerPeerLimit, c.ChunkTimeout, c.PeerFailLimit,
		c.MaxRedials, c.PeerRedialBackoff, c.MaxRedialBackoff,
		c.ProvisionalProbeStrikes, c.ProvisionalProbeInflight,
		c.WarmPeerTarget, c.WarmRefreshInterval, watch)
	fetchCancel()
	if derr != nil {
		return fmt.Errorf("download failed: %w", derr)
	}
	bytesTotal = bt

	offered := make([]string, 0, len(chosen.Peers))
	for p := range chosen.Peers {
		offered = append(offered, p)
	}
	sort.Strings(offered)
	good := make([]string, 0, len(goodPeers))
	for _, p := range goodPeers {
		good = append(good, string(p))
	}
	sort.Strings(good)

	meta := savedMeta{
		Height:          chosen.Height,
		Format:          chosen.Format,
		Chunks:          chosen.Chunks,
		HashHex:         hex.EncodeToString(chosen.Hash),
		MetadataLen:     len(chosen.Metadata),
		GoodPeers:       good,
		OfferedBy:       offered,
		DownloadedAt:    time.Now().UTC(),
		BytesTotal:      bytesTotal,
		BytesTotalHuman: humanBytes(bytesTotal),
	}
	if err := writeJSON(filepath.Join(snapDir, "meta.json"), meta); err != nil {
		return fmt.Errorf("write meta.json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(snapDir, ".complete"), nil, 0o644); err != nil {
		return fmt.Errorf("mark complete: %w", err)
	}
	log.Info("snapshot saved", "dir", snapDir)
	log.Info("download complete",
		"height", chosen.Height, "format", chosen.Format,
		"chunks", chosen.Chunks, "bytes", humanBytes(bytesTotal))

	return nil
}

func buildP2PConfig() *cfg.P2PConfig {
	p := cfg.DefaultP2PConfig()
	p.AllowDuplicateIP = true
	p.HandshakeTimeout = 5 * time.Second
	p.DialTimeout = 5 * time.Second
	// Cap outbound peers below cometbft's 256 hard ceiling. Caller
	// can override via Config.MaxOutboundPeers; 64 is the polite-low
	// default that still absorbs PEX-harvested addrbooks dominated
	// by non-snapshot peers.
	p.MaxNumOutboundPeers = 64 // overridden in NewSwitch by Config below
	return p
}

func buildMConnConfig() conn.MConnConfig {
	mConfig := conn.DefaultMConnConfig()
	// State-sync peers commonly bump MaxPacketMsgPayloadSize past the
	// default 1024 bytes (e.g. some send single PacketMsgs of 10–100KB).
	// 256KB accommodates any reasonable peer config; we only send tiny
	// requests so this doesn't break anyone we connect to.
	mConfig.MaxPacketMsgPayloadSize = 256 * 1024
	// cometbft defaults to 500 KB/s per connection — at that rate one
	// 10MB chunk takes ~20s and a 3GB snapshot from 5 peers takes ~17min.
	// Bump to 10 MB/s so we're peer-side-limited, not us-limited.
	mConfig.SendRate = 10 * 1024 * 1024
	mConfig.RecvRate = 10 * 1024 * 1024
	return mConfig
}
