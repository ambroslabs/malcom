// Package snapfetch is the reusable core behind `malcom snapshot fetch`.
// It walks candidate snapshot heights against state-sync peers, picks
// the freshest offer that any peer will serve, downloads every chunk
// in parallel, verifies each against the per-chunk SHA256 in the
// snapshot Metadata blob, and writes everything under a per-snapshot
// directory ready for `malcom snapshot import` to consume.
package snapfetch

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	pexcb "github.com/cometbft/cometbft/p2p/pex"
	"github.com/cometbft/cometbft/version"

	"github.com/zrbecker/cosmos-p2p/internal/crawler"
	localpex "github.com/zrbecker/cosmos-p2p/internal/pex"
	"github.com/zrbecker/cosmos-p2p/internal/peers"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotinspect"
	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

// Config holds all knobs for RunFetch. Field defaults are documented in
// the comments — pass zero values to opt into the defaults via Defaults().
type Config struct {
	ChainID     string
	NodeKeyPath string
	Listen      string // default "tcp://0.0.0.0:0"
	Moniker     string // default "cosmos-p2p-snapfetch"
	Cumulative  string // path to peers DB
	AddrBook    string // optional path to polkachu addrbook
	ExtraSeedsCSV string

	DiscoverFor       time.Duration // default 25s
	DialParallel      int           // default 32
	MaxCandidates     int           // default 5
	ProbeTimeout      time.Duration // default 12s
	MinGoodPeers      int           // default 1
	PerPeerLimit      int           // default 2
	ChunkTimeout      time.Duration // default 45s
	MaxFetchTime      time.Duration // default 30m
	PeerFailLimit     int           // default 3 (hash-mismatch / missing-chunk strikes before ban)
	PeerRedialMax     int           // deprecated; retained for back-compat (no longer caps redials)
	PeerRedialBackoff time.Duration // default 5s — base backoff between redial attempts; doubles on each retry up to MaxRedialBackoff

	// MaxRedialBackoff caps the exponential backoff between redial
	// attempts to a connected peer. We never permanently ban peers for
	// being temporarily disconnected; only hash-mismatch / missing-chunk
	// strikes (PeerFailLimit) ban a peer.
	MaxRedialBackoff time.Duration // default 5m

	// WarmPeerTarget is the connected-peer count below which the
	// background refresher keeps dialing seeds during phase 3. Without
	// this, peer attrition during a long bank-store import can starve
	// snapfetch even though plenty of seeds are still reachable.
	WarmPeerTarget int // default 16

	// WarmRefreshInterval is how often the background refresher checks
	// the connected count and dials more seeds if needed.
	WarmRefreshInterval time.Duration // default 5s

	TargetHeight      uint64

	// MaxHeight is the upper bound for snapshot selection. The cli
	// resolves it to the chain's current height via RPC by default.
	// Walking starts from floor(MaxHeight, SnapshotInterval).
	// Required (the walking algorithm has no useful behavior without it).
	MaxHeight uint64

	// MinHeight is the freshness floor: walking stops once the
	// candidate target height drops below this. Typically
	// MaxHeight - MaxAgeBlocks. Zero disables the floor (walks
	// all the way to height 1 — usually undesirable).
	MinHeight uint64

	// SnapshotInterval is the chain's snapshot stride (cosmoshub
	// mints every 1000 blocks). Walking decrements target by this on
	// each per-height failure.
	SnapshotInterval uint64

	// PerHeightTimeout is how long to wait for a peer to serve
	// chunk-0 at the current target height before walking back.
	PerHeightTimeout time.Duration

	// MaxOutboundPeers is the hard cap on the cometbft Switch's
	// outbound connection count.
	MaxOutboundPeers int

	// PEXTargetPeers / PEXMaxPerWave control our PEX auto-dial
	// reactor's pace. TargetPeers should be < MaxOutboundPeers.
	PEXTargetPeers int
	PEXMaxPerWave  int

	// ChurnGrace is how long a connected peer has to advertise a
	// useful snapshot before being dropped. See walkBackward.
	ChurnGrace time.Duration
	MaxRescans        int           // default 3
	RescanDiscoverFor time.Duration // default 15s

	// Logger is optional; if nil, a no-op logger is used. The CLI passes
	// a pre-filtered cmtlog.Logger here so debug/info filtering is the
	// caller's concern.
	Logger cmtlog.Logger
}

// applyDefaults fills in zero-valued fields with defaults. Mutates cfg.
func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = "tcp://0.0.0.0:0"
	}
	if c.Moniker == "" {
		c.Moniker = "cosmos-p2p-snapfetch"
	}
	if c.DiscoverFor == 0 {
		c.DiscoverFor = 25 * time.Second
	}
	if c.DialParallel == 0 {
		c.DialParallel = 32
	}
	if c.MaxCandidates == 0 {
		c.MaxCandidates = 5
	}
	if c.ProbeTimeout == 0 {
		c.ProbeTimeout = 12 * time.Second
	}
	if c.MinGoodPeers == 0 {
		c.MinGoodPeers = 1
	}
	if c.PerPeerLimit == 0 {
		c.PerPeerLimit = 2
	}
	if c.ChunkTimeout == 0 {
		c.ChunkTimeout = 45 * time.Second
	}
	if c.MaxFetchTime == 0 {
		c.MaxFetchTime = 30 * time.Minute
	}
	if c.PeerFailLimit == 0 {
		c.PeerFailLimit = 3
	}
	if c.PeerRedialMax == 0 {
		c.PeerRedialMax = 3
	}
	if c.PeerRedialBackoff == 0 {
		c.PeerRedialBackoff = 5 * time.Second
	}
	if c.MaxRedialBackoff == 0 {
		c.MaxRedialBackoff = 5 * time.Minute
	}
	if c.WarmPeerTarget == 0 {
		c.WarmPeerTarget = 16
	}
	if c.WarmRefreshInterval == 0 {
		c.WarmRefreshInterval = 5 * time.Second
	}
	if c.MaxRescans == 0 {
		c.MaxRescans = 3
	}
	if c.RescanDiscoverFor == 0 {
		c.RescanDiscoverFor = 15 * time.Second
	}
	if c.SnapshotInterval == 0 {
		c.SnapshotInterval = 1000
	}
	if c.PerHeightTimeout == 0 {
		c.PerHeightTimeout = 10 * time.Second
	}
	if c.MaxOutboundPeers == 0 {
		c.MaxOutboundPeers = 64
	}
	if c.PEXTargetPeers == 0 {
		c.PEXTargetPeers = 48
	}
	if c.PEXMaxPerWave == 0 {
		c.PEXMaxPerWave = 8
	}
	if c.ChurnGrace == 0 {
		c.ChurnGrace = 3 * time.Second
	}
	if c.Logger == nil {
		c.Logger = cmtlog.NewNopLogger()
	}
}

// Result is what RunFetch returns to the caller. Mirrors the contents
// of the on-disk meta.json so consumers (the cli, tests) don't need to
// re-parse the file.
type Result struct {
	Height      uint64
	Format      uint32
	Chunks      uint32
	Hash        []byte
	Metadata    []byte
	ChunkHashes [][]byte
	BytesTotal  uint64
	GoodPeers   []string
	OfferedBy   []string
}

// peerSeed is one address (with provenance) pulled from the peers DB or
// addrbook. Internal.
type peerSeed struct{ addr, source string }

// snapshotOffer is one (height, format, hash) tuple advertised by ≥1
// peer. Internal.
type snapshotOffer struct {
	Height   uint64
	Format   uint32
	Chunks   uint32
	Hash     []byte
	Metadata []byte
	Peers    map[string]bool
}

// savedMeta is the JSON shape written to <dir>/meta.json after a
// successful fetch.
type savedMeta struct {
	Height          uint64    `json:"height"`
	Format          uint32    `json:"format"`
	Chunks          uint32    `json:"chunks"`
	HashHex         string    `json:"hash_hex"`
	MetadataLen     int       `json:"metadata_len"`
	GoodPeers       []string  `json:"good_peers"`
	OfferedBy       []string  `json:"offered_by"`
	DownloadedAt    time.Time `json:"downloaded_at"`
	BytesTotal      uint64    `json:"bytes_total"`
	BytesTotalHuman string    `json:"bytes_total_human"`
}

func snapKey(s *statesync.Snapshot) string {
	return fmt.Sprintf("%d_%d_%s", s.Height, s.Format, hex.EncodeToString(s.Hash))
}

func snapKeyOffer(o *snapshotOffer) string {
	return fmt.Sprintf("%d_%d_%s", o.Height, o.Format, hex.EncodeToString(o.Hash))
}

// RunFetch is the library entry point. It builds a p2p.Switch, walks
// candidate heights, downloads chunks, and writes everything under
// <outRoot>/snapshot_<chain>_<height>/ (chunks + metadata.bin +
// meta.json + .complete marker).
//
// On error any partial output under outRoot is left in place — no
// .complete marker is written, so the cli can detect incomplete dirs
// and the user can inspect / remove them.
func RunFetch(ctx context.Context, cfg Config, outRoot string) (*Result, error) {
	cfg.applyDefaults()
	logger := cfg.Logger

	if cfg.NodeKeyPath != "" {
		if err := os.MkdirAll(filepath.Dir(cfg.NodeKeyPath), 0o700); err != nil {
			return nil, fmt.Errorf("mkdir node-key dir: %w", err)
		}
	}
	nodeKey, err := p2p.LoadOrGenNodeKey(cfg.NodeKeyPath)
	if err != nil {
		return nil, fmt.Errorf("node key: %w", err)
	}

	seeds := loadSeeds(cfg.Cumulative, cfg.AddrBook, logger)
	for _, s := range strings.Split(cfg.ExtraSeedsCSV, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			seeds = append([]peerSeed{{addr: s, source: "flag"}}, seeds...)
		}
	}
	if len(seeds) == 0 {
		return nil, fmt.Errorf("no seeds available")
	}
	addrByNodeID := map[string]string{}
	for _, s := range seeds {
		parts := strings.SplitN(s.addr, "@", 2)
		if len(parts) == 2 {
			addrByNodeID[parts[0]] = s.addr
		}
	}
	logger.With("module", "snapfetch").Info("starting",
		"node_id", string(nodeKey.ID()), "seeds", len(seeds))

	listenAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), cfg.Listen))
	if err != nil {
		return nil, fmt.Errorf("listen addr: %w", err)
	}
	// Channels: PEX (0x00) lets us harvest addresses from peers via
	// cometbft's peer-exchange; state-sync (0x60/0x61) is what we're
	// here for. Advertising 0x00 is what makes well-behaved peers
	// reply to our PexRequest.
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      listenAddr.DialString(),
		Network:         cfg.ChainID,
		Version:         version.TMCoreSemVer,
		Channels:        []byte{localpex.Channel, statesync.SnapshotChannel, statesync.ChunkChannel},
		Moniker:         cfg.Moniker,
		Other:           p2p.DefaultNodeInfoOther{TxIndex: "off"},
	}
	if err := nodeInfo.Validate(); err != nil {
		return nil, fmt.Errorf("nodeInfo invalid: %w", err)
	}
	p2pConfig := buildP2PConfig()
	p2pConfig.MaxNumOutboundPeers = cfg.MaxOutboundPeers
	mConfig := buildMConnConfig()

	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, mConfig)
	if err := transport.Listen(*listenAddr); err != nil {
		return nil, fmt.Errorf("transport.Listen: %w", err)
	}
	ssR := statesync.NewReactor(logger.With("module", "statesync"))
	ssR.KeepBytes = true

	// AddrBook holds peer addresses learned via PEX (and seeded with
	// our extra_seeds list at startup). cometbft's implementation —
	// JSON-persistent, bucket-balanced, freshness-tracked.
	bookPath := cfg.AddrBook
	if bookPath == "" {
		bookPath = filepath.Join(filepath.Dir(cfg.NodeKeyPath), "addrbook.json")
	}
	if err := os.MkdirAll(filepath.Dir(bookPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir addrbook dir: %w", err)
	}
	book := pexcb.NewAddrBook(bookPath, false /* routabilityStrict */)
	book.SetLogger(logger.With("module", "addrbook"))

	// PEX reactor: sends PexRequest on every AddPeer, writes received
	// PexAddrs to the book, and runs a dial loop that grows the
	// connected-peer set toward TargetPeers in parallel waves. ~30×
	// more aggressive than cometbft's ensurePeers default — we're a
	// one-shot fetcher, not a long-running node.
	pexR := localpex.NewAutoReactor(book, localpex.AutoConfig{
		TargetPeers:  cfg.PEXTargetPeers,
		MaxPerWave:   cfg.PEXMaxPerWave,
		DialInterval: 2 * time.Second,
		BookBias:     50,
	}, logger.With("module", "pex"))

	sw := p2p.NewSwitch(p2pConfig, transport)
	sw.SetLogger(logger.With("module", "p2p"))
	sw.SetNodeKey(nodeKey)
	sw.SetNodeInfo(nodeInfo)
	sw.SetAddrBook(book)
	sw.AddReactor("PEX", pexR)
	sw.AddReactor("STATESYNC", ssR)

	// Don't dial ourselves.
	if selfAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), listenAddr.DialString())); err == nil {
		book.AddOurAddress(selfAddr)
	}

	// Seed the book with our extra_seeds (chain-registry entries).
	// Source = seed's own address (it "told us about itself").
	seeded := 0
	for _, s := range seeds {
		na, err := p2p.NewNetAddressString(s.addr)
		if err != nil {
			continue
		}
		if err := book.AddAddress(na, na); err == nil {
			seeded++
		}
	}
	logger.With("module", "snapfetch").Info("addrbook ready",
		"path", bookPath, "seeded", seeded)

	if err := sw.Start(); err != nil {
		return nil, fmt.Errorf("switch.Start: %w", err)
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

	flog := logger.With("module", "snapfetch")
	mux := newEventMux(ctx, ssR.Out)
	defer mux.stop()

	// peerWatch: long-lived churn loop. Spans both walking and
	// downloading phases — drops peers that don't advertise anything
	// in our freshness window, and addrbook-bans them so PEX picks
	// fresher candidates. Started here so churn pressure is on the
	// peer set from the moment we start collecting offers.
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watch := newPeerWatch(sw, book, cfg.MinHeight, cfg.ChurnGrace, flog)
	go watch.run(watchCtx, mux.subscribe())

	var (
		chosen      *snapshotOffer
		goodPeers   []p2p.ID
		bytesTotal  uint64
		chunkHashes [][]byte
	)

	// Walk: dial seeds, warm up, then probe target heights in
	// descending order until one peer serves chunk-0. No rescan
	// loop — if the walk exhausts the freshness window, error out.
	chosen, goodPeers, err = walkBackward(ctx, sw, ssR, mux, seeds,
		cfg, addrByNodeID, flog)
	if err != nil {
		return nil, err
	}

	chunkHashes, err = parseChunkHashes(chosen.Metadata)
	if err != nil {
		return nil, fmt.Errorf("parse chunk_hashes: %w", err)
	}
	if uint32(len(chunkHashes)) != chosen.Chunks {
		return nil, fmt.Errorf("metadata mismatch: chunk_hashes=%d expected=%d",
			len(chunkHashes), chosen.Chunks)
	}

	// Create the output dir and write metadata.bin before any chunk
	// arrives — chunk goroutines write into snapDir concurrently and
	// rely on it existing.
	snapDir := filepath.Join(outRoot, fmt.Sprintf("snapshot_%s_%d", cfg.ChainID, chosen.Height))
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir snapshot dir: %w", err)
	}
	if err := os.WriteFile(filepath.Join(snapDir, "metadata.bin"), chosen.Metadata, 0o644); err != nil {
		return nil, fmt.Errorf("write metadata.bin: %w", err)
	}

	// ─── Download all chunks ──────────────────────────────────────────
	fetchCtx, fetchCancel := context.WithTimeout(ctx, cfg.MaxFetchTime)
	bt, derr := download(fetchCtx, sw, ssR, mux.subscribe(),
		chosen, chunkHashes, goodPeers, addrByNodeID, snapDir, seeds,
		cfg.PerPeerLimit, cfg.ChunkTimeout, cfg.PeerFailLimit,
		cfg.PeerRedialBackoff, cfg.MaxRedialBackoff,
		cfg.WarmPeerTarget, cfg.WarmRefreshInterval, watch, flog)
	fetchCancel()
	if derr != nil {
		return nil, fmt.Errorf("download failed: %w", derr)
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
		return nil, fmt.Errorf("write meta.json: %w", err)
	}
	if err := os.WriteFile(filepath.Join(snapDir, ".complete"), nil, 0o644); err != nil {
		return nil, fmt.Errorf("mark complete: %w", err)
	}
	flog.Info("snapshot saved", "dir", snapDir)
	flog.Info("download complete",
		"height", chosen.Height, "format", chosen.Format,
		"chunks", chosen.Chunks, "bytes", humanBytes(bytesTotal))

	return &Result{
		Height:      chosen.Height,
		Format:      chosen.Format,
		Chunks:      chosen.Chunks,
		Hash:        chosen.Hash,
		Metadata:    chosen.Metadata,
		ChunkHashes: chunkHashes,
		BytesTotal:  bytesTotal,
		GoodPeers:   good,
		OfferedBy:   offered,
	}, nil
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

// InspectAndEnrich runs the snapshotinspect package on a completed
// snapshot directory and rewrites meta.json with structural details.
// Optional post-process — RunFetch doesn't call it (the next pipeline
// step parses the snapshot anyway). logger may be nil for silent
// operation.
func InspectAndEnrich(dir string, logger cmtlog.Logger) error {
	if logger != nil {
		logger.Info("inspecting", "dir", dir)
	}
	t0 := time.Now()
	res, err := snapshotinspect.Inspect(dir)
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}
	mdBytes, err := os.ReadFile(filepath.Join(dir, "metadata.bin"))
	if err != nil {
		return fmt.Errorf("read metadata.bin: %w", err)
	}
	hashes, err := snapshotinspect.ParseChunkHashes(mdBytes)
	if err != nil {
		return fmt.Errorf("parse chunk_hashes: %w", err)
	}

	mp := filepath.Join(dir, "meta.json")
	raw, err := os.ReadFile(mp)
	if err != nil {
		return fmt.Errorf("read meta.json: %w", err)
	}
	var current map[string]interface{}
	if err := json.Unmarshal(raw, &current); err != nil {
		return fmt.Errorf("parse meta.json: %w", err)
	}
	current["inspected_at"] = time.Now().UTC()
	current["decompressed_bytes"] = res.DecompressedBytes
	current["decompressed_bytes_human"] = snapshotinspect.HumanBytes(res.DecompressedBytes)
	current["total_items"] = res.TotalItems
	current["stores"] = res.Stores
	current["extensions"] = res.Extensions
	if len(res.UnknownItemTags) > 0 {
		current["unknown_item_tags"] = res.UnknownItemTags
	}
	current["chunk_hashes_hex"] = hashes
	enriched, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(mp, append(enriched, '\n'), 0o644); err != nil {
		return err
	}
	if logger != nil {
		logger.Info("inspect complete",
			"items", res.TotalItems,
			"decompressed", snapshotinspect.HumanBytes(res.DecompressedBytes),
			"stores", len(res.Stores),
			"extensions", len(res.Extensions),
			"elapsed", time.Since(t0).Truncate(time.Millisecond))
	}
	return nil
}

// ─── Event multiplexer ───────────────────────────────────────────────────
//
// statesync.Reactor has one Out channel; we want multiple consumers
// across phases. eventMux fans out events to all current subscribers.
// Subscribers drop events if their inbox is full.

type eventMux struct {
	in    <-chan statesync.Event
	mu    sync.Mutex
	subs  []chan statesync.Event
	close chan struct{}
}

func newEventMux(ctx context.Context, in <-chan statesync.Event) *eventMux {
	m := &eventMux{in: in, close: make(chan struct{})}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.close:
				return
			case ev := <-in:
				m.mu.Lock()
				subs := append([]chan statesync.Event(nil), m.subs...)
				m.mu.Unlock()
				for _, s := range subs {
					select {
					case s <- ev:
					default:
					}
				}
			}
		}
	}()
	return m
}

func (m *eventMux) subscribe() chan statesync.Event {
	c := make(chan statesync.Event, 256)
	m.mu.Lock()
	m.subs = append(m.subs, c)
	m.mu.Unlock()
	return c
}

func (m *eventMux) stop() { close(m.close) }

// ─── peerWatch: long-lived churn loop ──────────────────────────────────
//
// Spans walk + download. Listens for SnapshotsResponse events; marks
// each peer "useful" if it ever advertised a snapshot at height
// >= minHeight. On a 1s tick, peers connected for ≥ grace with no
// useful flag get StopPeerGracefully'd AND MarkBad'd in the addrbook
// (1h ban) so PEX won't re-dial them this run.
//
// minHeight = 0 disables churning (peerWatch still subscribes; just
// never drops anyone).
type peerWatch struct {
	sw        *p2p.Switch
	book      pexcb.AddrBook
	minHeight uint64
	grace     time.Duration
	log       cmtlog.Logger

	mu        sync.Mutex
	firstSeen map[p2p.ID]time.Time
	useful    map[p2p.ID]bool
}

func newPeerWatch(sw *p2p.Switch, book pexcb.AddrBook, minHeight uint64, grace time.Duration, log cmtlog.Logger) *peerWatch {
	return &peerWatch{
		sw:        sw,
		book:      book,
		minHeight: minHeight,
		grace:     grace,
		log:       log,
		firstSeen: map[p2p.ID]time.Time{},
		useful:    map[p2p.ID]bool{},
	}
}

// markUseful records that the named peer offered something inside
// our freshness window. Safe to call from any goroutine.
func (w *peerWatch) markUseful(peerID p2p.ID) {
	w.mu.Lock()
	w.useful[peerID] = true
	w.mu.Unlock()
}

// banPeer is the one-stop shop for "this peer is useless; evict it":
// disconnect + addrbook-ban. Used by both the periodic churn tick
// and download()'s misbehavior path.
func (w *peerWatch) banPeer(peer p2p.Peer, reason string) {
	addr := peer.SocketAddr()
	w.log.Debug("evicting peer", "peer", string(peer.ID()), "reason", reason)
	w.sw.StopPeerGracefully(peer)
	if w.book != nil {
		w.book.MarkBad(addr, time.Hour)
	}
	w.mu.Lock()
	delete(w.firstSeen, peer.ID())
	w.mu.Unlock()
}

func (w *peerWatch) tick() {
	if w.minHeight == 0 {
		return
	}
	now := time.Now()
	for _, peer := range w.sw.Peers().List() {
		id := peer.ID()
		w.mu.Lock()
		if _, ok := w.firstSeen[id]; !ok {
			w.firstSeen[id] = now
			w.mu.Unlock()
			continue
		}
		if w.useful[id] {
			w.mu.Unlock()
			continue
		}
		first := w.firstSeen[id]
		w.mu.Unlock()
		if now.Sub(first) < w.grace {
			continue
		}
		w.banPeer(peer, "no useful offer in window")
	}
}

// run is the watcher's main loop. Subscribes to evs (the caller
// supplies the mux subscription so subscriber lifecycle matches
// peerWatch's). Returns when ctx is cancelled.
func (w *peerWatch) run(ctx context.Context, evs chan statesync.Event) {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.tick()
		case ev, ok := <-evs:
			if !ok {
				return
			}
			if ev.Snapshot == nil {
				continue
			}
			if w.minHeight == 0 || ev.Snapshot.Height >= w.minHeight {
				w.markUseful(p2p.ID(ev.PeerID))
			}
		}
	}
}

// ─── Walking algorithm: deterministic newest-first picker ───────────────

// walkBackward replaces the old discover→rank→race-probe pipeline with
// a direct walk: it dials seeds, lets PEX warm the peer set for ~3s,
// then iterates target heights in descending order (interval-aligned)
// from floor(MaxHeight, interval) down to MinHeight. For each target
// it asks every offering peer for chunk-0 and accepts the first valid
// response. On timeout it walks back by SnapshotInterval.
//
// If TargetHeight is set, walks exactly that one height (no fallback).
//
// Returns the chosen offer + a starter good-peers list (the chunk-0
// responder, plus any peer in the offer's Peers map; phase 3
// dispatches to all of them).
func walkBackward(
	ctx context.Context,
	sw *p2p.Switch,
	ssR *statesync.Reactor,
	mux *eventMux,
	seeds []peerSeed,
	cfg Config,
	addrByNodeID map[string]string,
	logger cmtlog.Logger,
) (*snapshotOffer, []p2p.ID, error) {
	_ = addrByNodeID

	// Subscribe to events BEFORE dialing so any SnapshotsResponse
	// arriving during the seed-dial wave + warmup is captured (the
	// mux drops events when there are no subscribers).
	evs := mux.subscribe()

	// Kickstart: fire-and-forget dials to a capped subset of seeds.
	// We don't wait for results — each unreachable peer can take 30s+
	// to time out, and with PEX-accumulated addrbooks of thousands of
	// peers a synchronous wait would stall fetch for tens of minutes.
	// PEX's auto-dial loop (running on a 2s tick against the addrbook)
	// handles the rest.
	const kickstartCap = 64
	{
		addrs := make([]string, 0, kickstartCap)
		for i, s := range seeds {
			if i >= kickstartCap {
				break
			}
			addrs = append(addrs, s.addr)
		}
		if err := sw.DialPeersAsync(addrs); err != nil {
			logger.Error("kickstart dial", "err", err)
		}
	}

	// Target list.
	var targets []uint64
	switch {
	case cfg.TargetHeight != 0:
		targets = []uint64{cfg.TargetHeight}
	case cfg.MaxHeight == 0:
		return nil, nil, fmt.Errorf("walking requires MaxHeight > 0 or an explicit TargetHeight")
	default:
		targets = walkTargets(cfg.MaxHeight, cfg.MinHeight, cfg.SnapshotInterval)
		if len(targets) == 0 {
			return nil, nil, fmt.Errorf("no target heights in [%d, %d] with stride %d",
				cfg.MinHeight, cfg.MaxHeight, cfg.SnapshotInterval)
		}
	}

	// Single subscription drives both offer collection and chunk-0
	// reception. New SnapshotsResponse events update the offers map;
	// new ChunkResponse events at the current target trigger acceptance.
	// (`evs` was subscribed earlier, before the dial wave.)
	offers := map[string]*snapshotOffer{}
	offerByHeight := map[uint64][]string{}

	// Churn lives in peerWatch (started by RunFetch). We just collect
	// offers here; peerWatch sees them via its own subscription.

	addOffer := func(s *statesync.Snapshot, peerID string) {
		k := snapKey(s)
		rec, ok := offers[k]
		if !ok {
			rec = &snapshotOffer{
				Height:   s.Height,
				Format:   s.Format,
				Chunks:   s.Chunks,
				Hash:     s.Hash,
				Metadata: s.Metadata,
				Peers:    map[string]bool{},
			}
			offers[k] = rec
			offerByHeight[s.Height] = append(offerByHeight[s.Height], k)
		}
		rec.Peers[peerID] = true
	}

	// Drain any events that arrived during the seed-dial wave into
	// the offers map BEFORE we start the 3s warmup. Otherwise the
	// warmup `time.After` blocks the receive loop and offers
	// accumulate in the channel buffer.
	drainEvents := func() {
		for {
			select {
			case ev := <-evs:
				if ev.Snapshot != nil {
					addOffer(ev.Snapshot, ev.PeerID)
				}
			default:
				return
			}
		}
	}
	drainEvents()

	// 3s warmup so PEX-harvested peers can connect and send their
	// SnapshotsResponse. Drain again afterward.
	warmupDeadline := time.NewTimer(3 * time.Second)
	for {
		drainEvents()
		select {
		case <-ctx.Done():
			warmupDeadline.Stop()
			return nil, nil, ctx.Err()
		case <-warmupDeadline.C:
			drainEvents()
			goto walkLoop
		case ev := <-evs:
			if ev.Snapshot != nil {
				addOffer(ev.Snapshot, ev.PeerID)
			}
		}
	}
walkLoop:
	// Dynamic target queue. Walks the precomputed `targets` (newest
	// first) but allows jumping back UP if a fresher offer arrives
	// mid-iteration. `failed` records heights whose deadline has
	// expired (don't revisit). `asked` tracks (peer, snapKey)
	// pairs across iterations so jumping doesn't re-spam peers we
	// already asked.
	queue := append([]uint64(nil), targets...)
	failed := map[uint64]bool{}
	asked := map[string]bool{}
	askKey := func(peerID, k string) string { return peerID + ":" + k }

	dispatch := func(target uint64) int {
		n := 0
		for _, k := range offerByHeight[target] {
			offer := offers[k]
			for pid := range offer.Peers {
				ak := askKey(pid, k)
				if asked[ak] {
					continue
				}
				peer := sw.Peers().Get(p2p.ID(pid))
				if peer == nil {
					continue
				}
				if ssR.RequestChunk(peer, offer.Height, offer.Format, 0) {
					asked[ak] = true
					n++
				}
			}
		}
		return n
	}

	for len(queue) > 0 {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		target := queue[0]
		queue = queue[1:]
		if failed[target] {
			continue
		}

		initialAsks := dispatch(target)
		out, _, dialing := sw.NumPeers()
		logger.Info("searching for snapshot",
			"height", target,
			"asking_peers", initialAsks,
			"connected", out,
			"dialing", dialing,
			"book_size", offerCount(offers))

		deadline := time.NewTimer(cfg.PerHeightTimeout)
		var accepted *snapshotOffer
		var responder p2p.ID
		jumped := false
	heightLoop:
		for {
			select {
			case <-ctx.Done():
				deadline.Stop()
				return nil, nil, ctx.Err()
			case <-deadline.C:
				break heightLoop
			case ev, ok := <-evs:
				if !ok {
					deadline.Stop()
					return nil, nil, fmt.Errorf("event channel closed")
				}
				if ev.Snapshot != nil {
					addOffer(ev.Snapshot, ev.PeerID)
					// Jump-up: a new offer arrived for a height
					// fresher than our current target. Abort this
					// iteration; the queue gets the new height
					// prioritized (and the current target requeued
					// behind it, since we never gave it the full
					// 10s window).
					if cfg.TargetHeight == 0 &&
						ev.Snapshot.Height > target &&
						(cfg.MinHeight == 0 || ev.Snapshot.Height >= cfg.MinHeight) &&
						!failed[ev.Snapshot.Height] {
						logger.Info("found higher snapshot from new peer; jumping",
							"from_height", target,
							"to_height", ev.Snapshot.Height,
							"peer", ev.PeerID)
						deadline.Stop()
						queue = append([]uint64{ev.Snapshot.Height, target}, queue...)
						jumped = true
						break heightLoop
					}
					if ev.Snapshot.Height == target {
						// New offer at our target — dispatch chunk-0 to this peer.
						dispatch(target)
					}
					continue
				}
				if ev.Chunk == nil || ev.Chunk.Index != 0 {
					continue
				}
				if ev.Chunk.Height != target {
					continue
				}
				if ev.Chunk.Missing || len(ev.Chunk.Bytes) == 0 {
					continue
				}
				// Find the offer whose (height, format) matches.
				for _, k := range offerByHeight[target] {
					o := offers[k]
					if o.Format == ev.Chunk.Format {
						accepted = o
						responder = p2p.ID(ev.PeerID)
						deadline.Stop()
						break heightLoop
					}
				}
			}
		}

		if accepted != nil {
			logger.Info("downloading snapshot",
				"height", accepted.Height, "format", accepted.Format,
				"chunks", accepted.Chunks,
				"hash", hex.EncodeToString(accepted.Hash)[:16],
				"served_by", string(responder))
			good := []p2p.ID{responder}
			seen := map[p2p.ID]bool{responder: true}
			for pid := range accepted.Peers {
				id := p2p.ID(pid)
				if !seen[id] {
					good = append(good, id)
					seen[id] = true
				}
			}
			return accepted, good, nil
		}

		if jumped {
			continue
		}

		if cfg.TargetHeight != 0 {
			return nil, nil, fmt.Errorf("target height %d: no peer served chunk-0 within %s",
				cfg.TargetHeight, cfg.PerHeightTimeout)
		}
		failed[target] = true
		next := uint64(0)
		if len(queue) > 0 {
			next = queue[0]
		}
		logger.Info("no served offer; walking back",
			"height", target, "next", next)
	}

	return nil, nil, fmt.Errorf("no servable snapshot found in window [%d, %d]",
		cfg.MinHeight, cfg.MaxHeight)
}

// offerCount returns the number of distinct snapshot offers we've
// collected so far (sum across heights). Used for diagnostic logs.
func offerCount(offers map[string]*snapshotOffer) int { return len(offers) }

// walkTargets returns a descending list of heights from
// floor(top, interval) down to >= minHeight, stepping by interval.
func walkTargets(top, minHeight, interval uint64) []uint64 {
	if interval == 0 || top < minHeight {
		return nil
	}
	start := (top / interval) * interval
	var out []uint64
	for h := start; h >= minHeight && h > 0; h -= interval {
		out = append(out, h)
		if h < interval {
			break
		}
	}
	return out
}

// ─── Chunk download scheduler ───────────────────────────────────────────

type peerStat struct {
	inflight      int
	failures      int  // missing=true or hash-mismatch responses (real misbehaviour)
	disconnects   int  // socket-level drops (transient) — never used to ban directly
	banned        bool // permanently benched (only set on PeerFailLimit failures)
	provisional   bool // true until peer responds with first verified chunk; provisional peers get one in-flight slot and a single-strike ban budget
	lastDialAt    time.Time
	nextDialAfter time.Time // earliest time a redial may be attempted; computed via exponential backoff over disconnects
}

// download is the phase-3 chunk scheduler. It dispatches chunks across
// good peers, verifies SHA256 against the metadata hashes, and writes
// each verified chunk to <snapDir>/chunk_<idx>.bin. Returns total bytes
// transferred (sum of verified chunk lengths).
//
// Resilience features:
//   - Exponential-backoff redial (no ban-on-disconnect). Only
//     hash-mismatch / missing-chunk strikes ban a peer; transient socket
//     drops just defer the next dial attempt.
//   - Background peer-pool refresher. While the connected peer count is
//     below WarmPeerTarget, dials seeds in the background so newly-broken
//     good peers can be replaced.
//   - Provisional peer promotion. Connected non-good peers are added to
//     stats with provisional=true and given one in-flight slot. The first
//     verified chunk promotes them to a full-budget good peer; a hash
//     mismatch single-strikes them out.
func download(ctx context.Context, sw *p2p.Switch, ssR *statesync.Reactor,
	evs chan statesync.Event, target *snapshotOffer, chunkHashes [][]byte,
	good []p2p.ID, addrByNodeID map[string]string, snapDir string, seeds []peerSeed,
	perPeer int, chunkTimeout time.Duration, peerFailLimit int,
	redialBackoff, maxRedialBackoff time.Duration,
	warmTarget int, warmRefreshInterval time.Duration,
	watch *peerWatch,
	logger cmtlog.Logger) (uint64, error) {

	// banAndDrop disconnects + addrbook-bans a misbehaving peer so
	// PEX can dial a replacement (banned peers occupying connection
	// slots was previously starving fresh dials at TargetPeers cap).
	banAndDrop := func(pid p2p.ID, reason string) {
		if watch == nil {
			return
		}
		if peer := sw.Peers().Get(pid); peer != nil {
			watch.banPeer(peer, reason)
		}
	}

	N := target.Chunks
	pending := make([]bool, N)
	completed := make([]bool, N)
	inflight := map[uint32]struct {
		peer p2p.ID
		sent time.Time
	}{}
	for i := uint32(0); i < N; i++ {
		pending[i] = true
	}

	stats := map[p2p.ID]*peerStat{}
	for _, p := range good {
		stats[p] = &peerStat{}
	}

	var bytesTotal atomic.Uint64
	doneCount := uint32(0)

	startTime := time.Now()
	lastProgress := time.Now()
	// Aligned to the outer 2s timeoutTicker — multiples of 2s land
	// exactly on a tick (10s gives 10, 20, 30, …). Non-multiples
	// round up: 15s would fire at 16, 32, …
	progressEvery := 10 * time.Second

	// computeRedialDelay returns redialBackoff * 2^(disconnects-1), capped
	// at maxRedialBackoff. With redialBackoff=5s and cap=5m, sequence is
	// 5s, 10s, 20s, 40s, 80s, 160s, 300s, 300s, 300s, ... — keeps trying
	// indefinitely so a peer that comes back online eventually rejoins.
	computeRedialDelay := func(disconnects int) time.Duration {
		if disconnects <= 1 {
			return redialBackoff
		}
		shift := disconnects - 1
		if shift > 10 {
			shift = 10
		}
		d := redialBackoff << uint(shift)
		if d <= 0 || d > maxRedialBackoff {
			return maxRedialBackoff
		}
		return d
	}

	tryRedial := func(pid p2p.ID) {
		st, ok := stats[pid]
		if !ok || st.banned {
			return
		}
		if peer := sw.Peers().Get(pid); peer != nil {
			return
		}
		if !st.nextDialAfter.IsZero() && time.Now().Before(st.nextDialAfter) {
			return
		}
		addr, ok := addrByNodeID[string(pid)]
		if !ok {
			// Unknown address — only happens for peers that joined via
			// PEX rather than the seed list. Drop from stats so a future
			// scanForNewPeers can re-add them if they reconnect.
			delete(stats, pid)
			return
		}
		na, err := p2p.NewNetAddressString(addr)
		if err != nil {
			st.banned = true
			return
		}
		st.disconnects++
		st.lastDialAt = time.Now()
		st.nextDialAfter = st.lastDialAt.Add(computeRedialDelay(st.disconnects))
		go func(na *p2p.NetAddress) {
			_ = sw.DialPeerWithAddress(na)
		}(na)
	}

	// pickPeer prefers proven (non-provisional) peers up to perPeer
	// inflight, then falls back to a single provisional probe slot per
	// peer. This way a freshly-warm peer is never given more than one
	// concurrent chunk until it has proven it can serve.
	pickPeer := func() p2p.ID {
		var bestProven, bestProvis p2p.ID
		bestProvenInflight := perPeer + 1
		bestProvisInflight := 2 // provisional cap = 1; sentinel = 2
		for pid, st := range stats {
			if st.banned {
				continue
			}
			if sw.Peers().Get(pid) == nil {
				continue
			}
			if st.provisional {
				if st.inflight < bestProvisInflight {
					bestProvisInflight = st.inflight
					bestProvis = pid
				}
				continue
			}
			if st.inflight < bestProvenInflight {
				bestProvenInflight = st.inflight
				bestProven = pid
			}
		}
		if bestProvenInflight <= perPeer {
			return bestProven
		}
		if bestProvisInflight <= 1 {
			return bestProvis
		}
		return ""
	}

	// scanForNewPeers walks sw.Peers() and registers any connected,
	// not-yet-tracked peer in stats as provisional. This is how peers
	// arriving via the keepWarm dialer (or PEX) get drawn into the
	// scheduler without a heavyweight rescan.
	scanForNewPeers := func() {
		for _, p := range sw.Peers().List() {
			pid := p.ID()
			if _, ok := stats[pid]; ok {
				continue
			}
			stats[pid] = &peerStat{provisional: true}
			logger.Debug("provisional peer added", "peer", string(pid))
		}
	}

	dispatch := func() int {
		dispatched := 0
		for i := uint32(0); i < N; i++ {
			if !pending[i] {
				continue
			}
			pid := pickPeer()
			if pid == "" {
				return dispatched
			}
			peer := sw.Peers().Get(pid)
			if peer == nil {
				stats[pid].banned = true
				continue
			}
			if !ssR.RequestChunk(peer, target.Height, target.Format, i) {
				continue
			}
			pending[i] = false
			inflight[i] = struct {
				peer p2p.ID
				sent time.Time
			}{peer: pid, sent: time.Now()}
			stats[pid].inflight++
			dispatched++
		}
		return dispatched
	}

	logger.Info("download starting",
		"chunks", N, "good_peers", len(good),
		"per_peer_inflight", perPeer, "warm_target", warmTarget)

	dispatch()

	// Background peer-pool refresher. Keeps the connected peer count
	// hovering near warmTarget by dialing seeds (shuffled) whenever we
	// drop below the threshold. scanForNewPeers in the main loop picks
	// up the resulting connections as provisional peers.
	if len(seeds) > 0 && warmTarget > 0 {
		go runKeepWarm(ctx, sw, seeds, warmTarget, warmRefreshInterval, logger)
	}

	timeoutTicker := time.NewTicker(2 * time.Second)
	defer timeoutTicker.Stop()

	hadEvent := false

	for doneCount < N {
		alive, connected := 0, 0
		for pid, st := range stats {
			if st.banned {
				continue
			}
			alive++
			if sw.Peers().Get(pid) != nil {
				connected++
			}
		}
		if alive == 0 {
			return bytesTotal.Load(), fmt.Errorf("all peers banned (done %d/%d)", doneCount, N)
		}

		select {
		case <-ctx.Done():
			return bytesTotal.Load(), ctx.Err()

		case <-timeoutTicker.C:
			now := time.Now()
			for idx, info := range inflight {
				if now.Sub(info.sent) > chunkTimeout {
					st := stats[info.peer]
					if st != nil {
						st.inflight--
						if st.inflight < 0 {
							st.inflight = 0
						}
						// Provisional peers that time out on their
						// probe lose their slot immediately — their
						// connection is suspect.
						if st.provisional {
							st.banned = true
							logger.Debug("benching provisional peer (probe timeout)",
								"peer", string(info.peer))
							banAndDrop(info.peer, "probe timeout")
						}
					}
					delete(inflight, idx)
					pending[idx] = true
				}
			}
			for pid, st := range stats {
				if st.banned {
					continue
				}
				if sw.Peers().Get(pid) == nil {
					tryRedial(pid)
				}
			}
			scanForNewPeers()
			dispatch()

			if now.Sub(lastProgress) >= progressEvery {
				rate := float64(doneCount) / now.Sub(startTime).Seconds()
				logger.Info("download progress",
					"chunks", fmt.Sprintf("%d/%d", doneCount, N),
					"MB", bytesTotal.Load()>>20,
					"chunks_per_s", fmt.Sprintf("%.1f", rate),
					"peers", fmt.Sprintf("%d/%d", connected, alive),
					"inflight", len(inflight))
				lastProgress = now
			}

		case ev := <-evs:
			if ev.Chunk == nil {
				continue
			}
			if ev.Chunk.Height != target.Height || ev.Chunk.Format != target.Format {
				continue
			}
			hadEvent = true
			idx := ev.Chunk.Index
			peer := p2p.ID(ev.PeerID)
			st, ok := stats[peer]
			if !ok {
				st = &peerStat{}
				stats[peer] = st
			}
			if info, ok := inflight[idx]; ok && info.peer == peer {
				delete(inflight, idx)
			}
			if st.inflight > 0 {
				st.inflight--
			}

			if completed[idx] {
				continue
			}

			// Provisional peers single-strike: any failure on their
			// probe bans them. Proven peers get peerFailLimit strikes.
			banLimit := peerFailLimit
			if st.provisional {
				banLimit = 1
			}

			if ev.Chunk.Missing || len(ev.Chunk.Bytes) == 0 {
				st.failures++
				if st.failures >= banLimit {
					st.banned = true
					logger.Debug("benching peer", "peer", string(peer), "failures", st.failures, "provisional", st.provisional)
					banAndDrop(peer, "missing/empty chunk")
				}
				pending[idx] = true
				dispatch()
				continue
			}

			h := sha256.Sum256(ev.Chunk.Bytes)
			if !bytesEq(h[:], chunkHashes[idx]) {
				logger.Error("chunk hash mismatch",
					"peer", string(peer), "idx", idx,
					"got_sha", hex.EncodeToString(h[:8]),
					"want_sha", hex.EncodeToString(chunkHashes[idx][:8]))
				st.failures++
				if st.failures >= banLimit {
					st.banned = true
					banAndDrop(peer, "chunk hash mismatch")
				}
				pending[idx] = true
				dispatch()
				continue
			}

			// Verified chunk — promote a provisional peer to proven, and
			// reset the disconnect/redial backoff so a peer that came
			// back from a long outage gets a clean slate.
			if st.provisional {
				st.provisional = false
				logger.Info("peer promoted from provisional", "peer", string(peer))
			}
			st.disconnects = 0
			st.nextDialAfter = time.Time{}

			// Write the verified chunk to disk. Errors are logged and
			// ignored so a transient disk hiccup doesn't abort the
			// whole fetch — if the file is missing later, the
			// downstream import step surfaces it.
			chunkPath := filepath.Join(snapDir, fmt.Sprintf("chunk_%05d.bin", idx))
			if err := os.WriteFile(chunkPath, ev.Chunk.Bytes, 0o644); err != nil {
				logger.Error("write chunk", "idx", idx, "err", err)
			}
			completed[idx] = true
			doneCount++
			bytesTotal.Add(uint64(len(ev.Chunk.Bytes)))
			dispatch()
		}

		if !hadEvent && time.Since(startTime) > 30*time.Second {
			logger.Error("no chunk replies in 30s; check peers", "alive_peers", alive)
			startTime = time.Now()
		}
	}
	logger.Info("download finished",
		"chunks", N,
		"bytes", bytesTotal.Load(),
		"elapsed", time.Since(startTime))
	return bytesTotal.Load(), nil
}

// runKeepWarm dials seeds in the background while the connected peer
// count is below warmTarget. Each refresh tick, it dials up to
// dialBatch new seeds (seeds we haven't already connected to). The
// dialed peers are picked up by download's scanForNewPeers tick and
// added to stats as provisional.
//
// Seeds are shuffled once at start so we don't bias toward the front
// of the list, and a cursor advances through the shuffled slice with
// wraparound — over a long bench, every seed eventually gets a try.
func runKeepWarm(ctx context.Context, sw *p2p.Switch, seeds []peerSeed,
	warmTarget int, refresh time.Duration, logger cmtlog.Logger) {

	if len(seeds) == 0 {
		return
	}
	const dialBatch = 4

	shuffled := make([]peerSeed, len(seeds))
	copy(shuffled, seeds)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	cursor := 0

	t := time.NewTicker(refresh)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		connected := sw.Peers().Size()
		if connected >= warmTarget {
			continue
		}
		dialed := 0
		// Walk the seed list for at most one full pass per tick — past
		// the cap, give up for now and retry next tick.
		for tries := 0; tries < len(shuffled) && dialed < dialBatch; tries++ {
			s := shuffled[cursor]
			cursor = (cursor + 1) % len(shuffled)
			na, err := p2p.NewNetAddressString(s.addr)
			if err != nil {
				continue
			}
			if peer := sw.Peers().Get(na.ID); peer != nil {
				continue
			}
			dialed++
			go func(na *p2p.NetAddress) {
				_ = sw.DialPeerWithAddress(na)
			}(na)
		}
		if dialed > 0 {
			logger.Debug("keep-warm refresh",
				"connected", connected, "target", warmTarget, "dialed", dialed)
		}
	}
}

// ─── helpers ────────────────────────────────────────────────────────────

func bytesEq(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// parseChunkHashes decodes a cosmos-sdk format-3 snapshot Metadata blob:
//
//	repeated bytes chunk_hashes = 1;
//
// We hand-roll the decoder so we don't drag cosmos-sdk into this binary.
func parseChunkHashes(metadata []byte) ([][]byte, error) {
	var out [][]byte
	i := 0
	for i < len(metadata) {
		if metadata[i] != 0x0A {
			return nil, fmt.Errorf("unexpected tag 0x%02x at offset %d", metadata[i], i)
		}
		i++
		length, n := binary.Uvarint(metadata[i:])
		if n <= 0 {
			return nil, fmt.Errorf("bad varint at offset %d", i)
		}
		i += n
		if i+int(length) > len(metadata) {
			return nil, fmt.Errorf("hash extends past metadata (offset=%d len=%d total=%d)",
				i, length, len(metadata))
		}
		h := make([]byte, length)
		copy(h, metadata[i:i+int(length)])
		out = append(out, h)
		i += int(length)
	}
	return out, nil
}

func loadSeeds(cumPath, addrBookPath string, logger cmtlog.Logger) []peerSeed {
	out := []peerSeed{}
	seen := map[string]bool{}
	if f, err := os.Open(cumPath); err == nil {
		defer f.Close()
		var recs []crawler.PeerRecord
		if err := json.NewDecoder(f).Decode(&recs); err == nil {
			sort.Slice(recs, func(i, j int) bool { return recs[i].LatestHeight > recs[j].LatestHeight })
			for _, r := range recs {
				if r.Addr == "" || seen[r.Addr] {
					continue
				}
				seen[r.Addr] = true
				out = append(out, peerSeed{addr: r.Addr, source: "cumulative"})
			}
		}
	}
	if len(out) == 0 {
		if addrBookPath == "" {
			return out
		}
		items, err := peers.Load(addrBookPath)
		if err != nil {
			logger.Error("load addrbook failed", "err", err)
			return nil
		}
		for _, it := range items {
			s := it.String()
			if seen[s] {
				continue
			}
			seen[s] = true
			out = append(out, peerSeed{addr: s, source: "addrbook"})
		}
	}
	return out
}

func writeJSON(path string, v interface{}) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func humanBytes(n uint64) string {
	const (
		k = 1024
		m = k * 1024
		g = m * 1024
	)
	switch {
	case n >= g:
		return fmt.Sprintf("%.2f GB", float64(n)/float64(g))
	case n >= m:
		return fmt.Sprintf("%.1f MB", float64(n)/float64(m))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

