// Package snapfetch is the reusable core behind cosmos-snapshot-fetch.
// It runs a 3-phase pipeline against state-sync peers:
//
//  1. discover — short broad dial wave to harvest snapshot offers.
//  2. raceProbe — chunk-0 probe across top-N candidates, pick the one
//     with the most peers that actually return real bytes.
//  3. download — schedule every chunk across good peers, verify each
//     against the per-chunk SHA256 in the snapshot Metadata blob.
//
// Output is delivered through a Sink: chunks are not buffered to disk,
// allowing callers (e.g. cosmos-rapid-bootstrap) to stream them straight
// into a downstream importer. The original cosmos-snapshot-fetch CLI is
// a thin wrapper that implements Sink as on-disk file writes.
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
	"github.com/cometbft/cometbft/version"

	"github.com/zrbecker/cosmos-p2p/internal/crawler"
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
	PreferFresh       bool
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
	if c.Logger == nil {
		c.Logger = cmtlog.NewNopLogger()
	}
}

// Sink receives streamed events as the fetch proceeds.
//
// Lifecycle (on success): OnChosen → OnChunk × N → OnComplete. On
// failure within a single attempt the CLI will rescan with a new
// candidate, so OnChosen may be called more than once before a final
// OnComplete; sinks should therefore reset their per-snapshot state on
// each OnChosen.
type Sink interface {
	// OnChosen is called once after phase-2 selects a candidate. It
	// includes the snapshot proto metadata bytes and per-chunk SHA256
	// hashes.
	OnChosen(height uint64, format uint32, chunks uint32, hash []byte, metadata []byte, chunkHashes [][]byte) error

	// OnChunk is called for each chunk as it arrives, hash-verified, in
	// arbitrary order. Implementations should not block long-term —
	// heavy work should be pushed to another goroutine via a channel.
	OnChunk(idx uint32, data []byte) error

	// OnComplete is called once when all chunks are downloaded.
	OnComplete(bytesTotal uint64, goodPeerIDs []string, offeredBy []string) error
}

// Result is what RunFetch returns to the caller. Fields mirror what was
// delivered via Sink callbacks for callers that don't want to track
// state in the sink.
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

// SavedMeta is the JSON shape written to <dir>/meta.json by the disk
// sink. Exported so the CLI can use the same struct (and so callers
// can decode existing meta.json files if they want).
type SavedMeta struct {
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

// RunFetch is the library entry point. It builds a p2p.Switch, runs the
// 3-phase pipeline (discover → race → download), and emits results
// through sink. The returned *Result is also reflected in sink calls.
//
// On error the partial state (anything emitted via sink so far) is left
// to the caller to interpret — typically the caller cancels its context
// and lets sink consumers drain naturally.
func RunFetch(ctx context.Context, cfg Config, sink Sink) (*Result, error) {
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
	fmt.Printf("[snapfetch] node_id=%s seeds=%d\n", nodeKey.ID(), len(seeds))

	listenAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), cfg.Listen))
	if err != nil {
		return nil, fmt.Errorf("listen addr: %w", err)
	}
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      listenAddr.DialString(),
		Network:         cfg.ChainID,
		Version:         version.TMCoreSemVer,
		Channels:        []byte{statesync.SnapshotChannel, statesync.ChunkChannel},
		Moniker:         cfg.Moniker,
		Other:           p2p.DefaultNodeInfoOther{TxIndex: "off"},
	}
	if err := nodeInfo.Validate(); err != nil {
		return nil, fmt.Errorf("nodeInfo invalid: %w", err)
	}
	p2pConfig := buildP2PConfig()
	mConfig := buildMConnConfig()

	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, mConfig)
	if err := transport.Listen(*listenAddr); err != nil {
		return nil, fmt.Errorf("transport.Listen: %w", err)
	}
	ssR := statesync.NewReactor(logger.With("module", "statesync"))
	ssR.KeepBytes = true
	sw := p2p.NewSwitch(p2pConfig, transport)
	sw.SetLogger(logger.With("module", "p2p"))
	sw.SetNodeKey(nodeKey)
	sw.SetNodeInfo(nodeInfo)
	sw.AddReactor("STATESYNC", ssR)
	if err := sw.Start(); err != nil {
		return nil, fmt.Errorf("switch.Start: %w", err)
	}
	defer func() { _ = sw.Stop() }()

	flog := logger.With("module", "snapfetch")
	mux := newEventMux(ctx, ssR.Out)
	defer mux.stop()

	failedKeys := map[string]bool{}

	var (
		chosen      *snapshotOffer
		goodPeers   []p2p.ID
		bytesTotal  uint64
		chunkHashes [][]byte
	)
	attempts := cfg.MaxRescans + 1
	for attempt := 0; attempt < attempts; attempt++ {
		discDur := cfg.DiscoverFor
		if attempt > 0 {
			discDur = cfg.RescanDiscoverFor
			fmt.Printf("\n[snapfetch] === RESCAN attempt %d/%d (failed targets: %d) ===\n",
				attempt, cfg.MaxRescans, len(failedKeys))
		}

		// ─── Phase 1: discover ─────────────────────────────────────────
		offers := discover(ctx, sw, mux.subscribe(), seeds, cfg.DialParallel, discDur, flog)
		if len(offers) == 0 {
			flog.Error("no snapshots discovered")
			continue
		}
		for k := range failedKeys {
			delete(offers, k)
		}
		candidates := rankCandidates(offers, cfg.TargetHeight, cfg.MaxCandidates, cfg.PreferFresh)
		if len(candidates) == 0 {
			flog.Error("no usable candidates after exclusions")
			continue
		}
		fmt.Printf("\n[snapfetch] candidate snapshots (attempt %d):\n", attempt+1)
		for i, c := range candidates {
			fmt.Printf("  %d) height=%d format=%d chunks=%d peers=%d hash=%s\n",
				i+1, c.Height, c.Format, c.Chunks, len(c.Peers), hex.EncodeToString(c.Hash)[:16])
		}

		// ─── Phase 2: race chunk-0 probes ──────────────────────────────
		c, peerIDs := raceProbe(ctx, sw, ssR, mux.subscribe(), candidates, addrByNodeID, cfg.ProbeTimeout, cfg.MinGoodPeers, cfg.PreferFresh, flog)
		if c == nil {
			flog.Error("no candidate had enough good peers", "min_peers", cfg.MinGoodPeers)
			continue
		}
		chosen, goodPeers = c, peerIDs
		fmt.Printf("\n[snapfetch] chosen: height=%d format=%d chunks=%d good_peers=%d hash=%s\n",
			chosen.Height, chosen.Format, chosen.Chunks, len(goodPeers), hex.EncodeToString(chosen.Hash)[:16])

		var perr error
		chunkHashes, perr = parseChunkHashes(chosen.Metadata)
		if perr != nil {
			flog.Error("parse chunk_hashes", "err", perr)
			failedKeys[snapKeyOffer(chosen)] = true
			chosen = nil
			continue
		}
		if uint32(len(chunkHashes)) != chosen.Chunks {
			flog.Error("metadata mismatch",
				"chunk_hashes", len(chunkHashes), "expected", chosen.Chunks)
			failedKeys[snapKeyOffer(chosen)] = true
			chosen = nil
			continue
		}

		// Hand metadata + chunk hashes to the sink before any chunk
		// arrives. Streaming sinks need this to allocate a reorder
		// buffer / spawn a downstream importer keyed on chunk count.
		if err := sink.OnChosen(chosen.Height, chosen.Format, chosen.Chunks, chosen.Hash, chosen.Metadata, chunkHashes); err != nil {
			return nil, fmt.Errorf("sink.OnChosen: %w", err)
		}

		// ─── Phase 3: download all chunks ─────────────────────────────
		fetchCtx, fetchCancel := context.WithTimeout(ctx, cfg.MaxFetchTime)
		bt, derr := download(fetchCtx, sw, ssR, mux.subscribe(),
			chosen, chunkHashes, goodPeers, addrByNodeID, sink, seeds,
			cfg.PerPeerLimit, cfg.ChunkTimeout, cfg.PeerFailLimit,
			cfg.PeerRedialBackoff, cfg.MaxRedialBackoff,
			cfg.WarmPeerTarget, cfg.WarmRefreshInterval, flog)
		fetchCancel()
		if derr != nil {
			flog.Error("download failed; will rescan",
				"chosen_height", chosen.Height, "err", derr)
			failedKeys[snapKeyOffer(chosen)] = true
			chosen = nil
			continue
		}
		bytesTotal = bt
		break
	}
	if chosen == nil {
		return nil, fmt.Errorf("exhausted %d rescan attempts without a successful download", attempts)
	}

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

	if err := sink.OnComplete(bytesTotal, good, offered); err != nil {
		return nil, fmt.Errorf("sink.OnComplete: %w", err)
	}
	fmt.Printf("\n[snapfetch] DONE  height=%d format=%d chunks=%d bytes=%s\n",
		chosen.Height, chosen.Format, chosen.Chunks, humanBytes(bytesTotal))

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
	p.MaxNumOutboundPeers = 256
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
// Used by the disk-sink CLI; streaming sinks don't need it.
func InspectAndEnrich(dir string) error {
	fmt.Printf("\n[snapfetch] inspecting (decompress + parse SnapshotItem stream)...\n")
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
	fmt.Printf("[snapfetch] inspect: %d items, decompressed=%s, %d stores, %d extensions in %s\n",
		res.TotalItems,
		snapshotinspect.HumanBytes(res.DecompressedBytes),
		len(res.Stores), len(res.Extensions),
		time.Since(t0).Truncate(time.Millisecond))
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

// ─── Phase 1 ─────────────────────────────────────────────────────────────

func discover(ctx context.Context, sw *p2p.Switch, evs chan statesync.Event, seeds []peerSeed,
	parallel int, dur time.Duration, logger cmtlog.Logger) map[string]*snapshotOffer {

	dctx, cancel := context.WithTimeout(ctx, dur)
	defer cancel()

	offers := map[string]*snapshotOffer{}
	var mu sync.Mutex

	go func() {
		for {
			select {
			case <-dctx.Done():
				return
			case ev := <-evs:
				if ev.Snapshot == nil {
					continue
				}
				k := snapKey(ev.Snapshot)
				mu.Lock()
				rec, ok := offers[k]
				if !ok {
					rec = &snapshotOffer{
						Height:   ev.Snapshot.Height,
						Format:   ev.Snapshot.Format,
						Chunks:   ev.Snapshot.Chunks,
						Hash:     ev.Snapshot.Hash,
						Metadata: ev.Snapshot.Metadata,
						Peers:    map[string]bool{},
					}
					offers[k] = rec
				}
				rec.Peers[ev.PeerID] = true
				mu.Unlock()
			}
		}
	}()

	queue := make(chan peerSeed, len(seeds)+16)
	for _, s := range seeds {
		queue <- s
	}
	close(queue)

	var wg sync.WaitGroup
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-dctx.Done():
					return
				case s, ok := <-queue:
					if !ok {
						return
					}
					na, err := p2p.NewNetAddressString(s.addr)
					if err != nil {
						continue
					}
					_ = sw.DialPeerWithAddress(na)
					// Hold ~4s so AddPeer triggers SnapshotsRequest and replies arrive.
					select {
					case <-dctx.Done():
					case <-time.After(4 * time.Second):
					}
					if peer := sw.Peers().Get(na.ID); peer != nil {
						sw.StopPeerGracefully(peer)
					}
				}
			}
		}()
	}

	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	go func() {
		for {
			select {
			case <-dctx.Done():
				return
			case <-t.C:
				mu.Lock()
				logger.Info("phase 1 progress",
					"unique_snapshots", len(offers),
					"connected", sw.Peers().Size())
				mu.Unlock()
			}
		}
	}()

	wg.Wait()
	<-dctx.Done()
	return offers
}

func rankCandidates(offers map[string]*snapshotOffer, force uint64, max int, preferFresh bool) []*snapshotOffer {
	cands := make([]*snapshotOffer, 0, len(offers))
	for _, o := range offers {
		if force != 0 && o.Height != force {
			continue
		}
		cands = append(cands, o)
	}
	sort.Slice(cands, func(i, j int) bool {
		if preferFresh {
			if cands[i].Height != cands[j].Height {
				return cands[i].Height > cands[j].Height
			}
			return len(cands[i].Peers) > len(cands[j].Peers)
		}
		ci, cj := len(cands[i].Peers), len(cands[j].Peers)
		if ci != cj {
			return ci > cj
		}
		return cands[i].Height > cands[j].Height
	})
	if len(cands) > max {
		cands = cands[:max]
	}
	return cands
}

// ─── Phase 2: race chunk-0 probes across candidates ─────────────────────

func raceProbe(ctx context.Context, sw *p2p.Switch, ssR *statesync.Reactor,
	evs chan statesync.Event, candidates []*snapshotOffer, addrByNodeID map[string]string,
	timeout time.Duration, minGood int, preferFresh bool, logger cmtlog.Logger) (*snapshotOffer, []p2p.ID) {

	type cand struct {
		offer *snapshotOffer
		good  []p2p.ID
		mu    sync.Mutex
	}
	cands := make([]*cand, len(candidates))
	for i, o := range candidates {
		cands[i] = &cand{offer: o}
	}

	want := map[p2p.ID]string{}
	for _, c := range cands {
		for pid := range c.offer.Peers {
			id := p2p.ID(pid)
			if _, ok := want[id]; ok {
				continue
			}
			if addr, ok := addrByNodeID[pid]; ok {
				want[id] = addr
			}
		}
	}
	logger.Info("phase 2: redialing candidate peers", "count", len(want))
	var redialWG sync.WaitGroup
	for id, addr := range want {
		if peer := sw.Peers().Get(id); peer != nil {
			continue
		}
		na, err := p2p.NewNetAddressString(addr)
		if err != nil {
			continue
		}
		redialWG.Add(1)
		go func(na *p2p.NetAddress) {
			defer redialWG.Done()
			_ = sw.DialPeerWithAddress(na)
		}(na)
	}
	waitDone := make(chan struct{})
	go func() { redialWG.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(12 * time.Second):
	}

	dispatched := 0
	for _, c := range cands {
		for pid := range c.offer.Peers {
			peer := sw.Peers().Get(p2p.ID(pid))
			if peer == nil {
				continue
			}
			if ssR.RequestChunk(peer, c.offer.Height, c.offer.Format, 0) {
				dispatched++
			}
		}
	}
	logger.Info("phase 2: chunk-0 probes dispatched",
		"candidates", len(cands), "connected", sw.Peers().Size(), "requests", dispatched)

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			break
		case <-deadline.C:
			goto pick
		case ev := <-evs:
			if ev.Chunk == nil || ev.Chunk.Index != 0 {
				continue
			}
			if ev.Chunk.Missing || len(ev.Chunk.Bytes) == 0 {
				continue
			}
			for _, c := range cands {
				if c.offer.Height != ev.Chunk.Height || c.offer.Format != ev.Chunk.Format {
					continue
				}
				c.mu.Lock()
				already := false
				for _, p := range c.good {
					if string(p) == ev.PeerID {
						already = true
						break
					}
				}
				if !already {
					c.good = append(c.good, p2p.ID(ev.PeerID))
				}
				c.mu.Unlock()
				logger.Info("good peer for candidate",
					"height", ev.Chunk.Height, "format", ev.Chunk.Format,
					"peer", ev.PeerID, "chunk_bytes", len(ev.Chunk.Bytes))
				break
			}
		}
	}

pick:
	if preferFresh {
		// FORK: with prefer-fresh, height is the primary key. Pick the
		// FRESHEST candidate that meets the min-peers threshold; older
		// candidates with more peers are ignored. Cost of "more peers"
		// (better redundancy) is trivial vs. the catchup cost of
		// state-syncing an older snapshot — every 1000 older blocks is
		// ~1.5 min of extra blocksync time on cosmos-hub.
		sort.Slice(cands, func(i, j int) bool {
			return cands[i].offer.Height > cands[j].offer.Height
		})
		for _, c := range cands {
			if len(c.good) >= minGood {
				logger.Info("phase 2 result (prefer-fresh)",
					"candidates", len(cands),
					"chosen_height", c.offer.Height,
					"chosen_good_peers", len(c.good))
				return c.offer, c.good
			}
		}
		return nil, nil
	}
	sort.Slice(cands, func(i, j int) bool {
		gi, gj := len(cands[i].good), len(cands[j].good)
		if gi != gj {
			return gi > gj
		}
		return cands[i].offer.Height > cands[j].offer.Height
	})
	logger.Info("phase 2 result",
		"candidates", len(cands),
		"top_height", cands[0].offer.Height,
		"top_good_peers", len(cands[0].good))
	if len(cands) == 0 || len(cands[0].good) < minGood {
		return nil, nil
	}
	return cands[0].offer, cands[0].good
}

// ─── Phase 3: chunk download scheduler ──────────────────────────────────

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
// good peers, verifies SHA256 against the metadata hashes, and emits
// each verified chunk through sink.OnChunk. Returns total bytes
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
	good []p2p.ID, addrByNodeID map[string]string, sink Sink, seeds []peerSeed,
	perPeer int, chunkTimeout time.Duration, peerFailLimit int,
	redialBackoff, maxRedialBackoff time.Duration,
	warmTarget int, warmRefreshInterval time.Duration,
	logger cmtlog.Logger) (uint64, error) {

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
	progressEvery := 15 * time.Second

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
			logger.Info("provisional peer added", "peer", string(pid))
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

	logger.Info("phase 3: starting download",
		"chunks", N, "good_peers", len(good), "per_peer_inflight", perPeer,
		"warm_target", warmTarget, "max_redial_backoff", maxRedialBackoff)

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
							logger.Info("benching provisional peer (probe timeout)",
								"peer", string(info.peer))
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
				fmt.Printf("[snapfetch] %d/%d chunks (%dMB) %.1f c/s peers=%d/%d inflight=%d\n",
					doneCount, N, bytesTotal.Load()>>20, rate, connected, alive, len(inflight))
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
					logger.Info("benching peer", "peer", string(peer), "failures", st.failures, "provisional", st.provisional)
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

			// Hand the verified chunk to the sink. Sink errors are
			// logged and ignored — the original CLI's behaviour was a
			// fatal exit on disk-write error, but for streaming sinks
			// we want the importer's error path (ctx cancellation) to
			// surface naturally rather than aborting mid-snapshot.
			if err := sink.OnChunk(idx, ev.Chunk.Bytes); err != nil {
				logger.Error("sink.OnChunk", "idx", idx, "err", err)
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
	logger.Info("phase 3: complete",
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
			logger.Info("keep-warm refresh",
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

// WriteJSONFile is exported for the disk-sink CLI. Internal callers
// should use writeJSON.
func WriteJSONFile(path string, v interface{}) error { return writeJSON(path, v) }

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

// HumanBytes is exported for the disk-sink CLI's meta.json hooks.
func HumanBytes(n uint64) string { return humanBytes(n) }

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

