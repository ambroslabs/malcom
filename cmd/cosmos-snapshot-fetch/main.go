// cosmos-snapshot-fetch discovers a fetchable cosmoshub state-sync snapshot,
// downloads every chunk, verifies against per-chunk hashes from the metadata,
// and writes it to disk in a format suitable for serving back via state-sync.
//
// Strategy: race multiple candidate snapshots through a chunk-0 probe so we
// don't get caught up downloading from a peer that has the snapshot listed
// but pruned. Pick the candidate with the most peers that returned a real
// chunk-0 (i.e. snapshot is actually present, not just advertised). If a
// peer returns missing=true mid-download we drop it and re-dispatch the
// chunk to another good peer. If all good peers for a candidate fall away,
// fall back to the next-best candidate.
//
// Storage layout:
//
//	<out>/<height>_<format>/
//	  meta.json           — height, format, chunks, hash, peer list, timestamps
//	  metadata.bin        — raw cosmos-sdk Metadata blob (chunk_hashes proto)
//	  chunk_NNNNN.bin     — one file per chunk
//	  .complete           — empty marker written when every chunk verified
package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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

	"os/signal"
)

type peerSeed struct{ addr, source string }

type snapshotOffer struct {
	Height   uint64
	Format   uint32
	Chunks   uint32
	Hash     []byte
	Metadata []byte
	Peers    map[string]bool
}

func snapKey(s *statesync.Snapshot) string {
	return fmt.Sprintf("%d_%d_%s", s.Height, s.Format, hex.EncodeToString(s.Hash))
}

func snapKeyOffer(o *snapshotOffer) string {
	return fmt.Sprintf("%d_%d_%s", o.Height, o.Format, hex.EncodeToString(o.Hash))
}

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

func main() {
	var (
		chainID      = flag.String("chain-id", "cosmoshub-4", "expected chain ID")
		cumulativeDB = flag.String("cumulative", "data/peers-cumulative.json", "peer DB from cosmos-crawl runs")
		addrBookPath = flag.String("addrbook", "data/polkachu_cosmoshub.json", "Polkachu addrbook fallback")
		nodeKeyPath  = flag.String("node-key", "data/snapfetch_node_key.json", "node key (separate from downloader)")
		listen       = flag.String("listen", "tcp://0.0.0.0:0", "p2p listen URL")
		moniker      = flag.String("moniker", "cosmos-p2p-snapfetch", "self-reported moniker")
		outRoot      = flag.String("out", "/mnt/data/cosmos-archive/cosmoshub-4/snapshots", "snapshot store root")

		discoverFor  = flag.Duration("discover", 25*time.Second, "phase 1: time spent discovering snapshots")
		dialParallel = flag.Int("dial-parallel", 32, "max concurrent dials during discovery")
		maxCandidates = flag.Int("max-candidates", 5, "phase 2: probe top-N newest unique snapshots in parallel")
		probeTimeout = flag.Duration("probe-timeout", 12*time.Second, "phase 2: time to wait for chunk-0 probe replies")
		minGoodPeers = flag.Int("min-peers", 1, "phase 2: minimum 'good' peers required to accept a candidate")

		perPeerLimit  = flag.Int("per-peer", 2, "phase 3: max in-flight chunks per peer (low to avoid pong-timeouts on the peer side)")
		chunkTimeout  = flag.Duration("chunk-timeout", 45*time.Second, "phase 3: per-chunk wait before re-dispatching")
		maxFetchTime  = flag.Duration("max-fetch", 30*time.Minute, "phase 3: hard cap on full download")
		peerFailLimit = flag.Int("peer-fails", 3, "phase 3: bench a peer after this many missing/hash-mismatch responses (NOT counting disconnects)")
		peerRedialMax = flag.Int("peer-redials", 3, "phase 3: max redial attempts when a peer drops the connection")
		peerRedialBackoff = flag.Duration("redial-backoff", 5*time.Second, "phase 3: minimum wait between redial attempts to the same peer")

		extraSeedsCSV = flag.String("extra-seeds", "", "comma-separated nodeID@host:port seeds in addition to DB")
		targetHeight  = flag.Uint64("target-height", 0, "if non-zero, force this exact snapshot height (else pick best)")
		preferFresh   = flag.Bool("prefer-fresh", false, "rank candidates by newest-height first (peers as tiebreak). Default ranks by peer count first.")
		maxRescans    = flag.Int("max-rescans", 3, "if all peers fail for the chosen snapshot, rescan up to this many times to find a fresh snapshot (or fall back)")
		rescanDiscoverFor = flag.Duration("rescan-discover", 15*time.Second, "shorter discovery duration on rescans (peer DB is already warm)")
		debug         = flag.Bool("debug", false, "verbose logging")
	)
	flag.Parse()

	logger := cmtlog.NewTMLogger(cmtlog.NewSyncWriter(os.Stderr))
	if *debug {
		logger = cmtlog.NewFilter(logger, cmtlog.AllowDebug())
	} else {
		logger = cmtlog.NewFilter(logger, cmtlog.AllowError(),
			cmtlog.AllowInfoWith("module", "snapfetch"))
	}

	if err := os.MkdirAll(filepath.Dir(*nodeKeyPath), 0o700); err != nil {
		log.Fatalf("mkdir node-key dir: %v", err)
	}
	nodeKey, err := p2p.LoadOrGenNodeKey(*nodeKeyPath)
	if err != nil {
		log.Fatalf("node key: %v", err)
	}
	if err := os.MkdirAll(*outRoot, 0o755); err != nil {
		log.Fatalf("mkdir out root: %v", err)
	}

	seeds := loadSeeds(*cumulativeDB, *addrBookPath, logger)
	for _, s := range strings.Split(*extraSeedsCSV, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			seeds = append([]peerSeed{{addr: s, source: "flag"}}, seeds...)
		}
	}
	if len(seeds) == 0 {
		log.Fatalf("no seeds available")
	}
	// Build nodeID→address lookup so we can re-dial peers that offered
	// snapshots after phase 1 disconnects them.
	addrByNodeID := map[string]string{}
	for _, s := range seeds {
		parts := strings.SplitN(s.addr, "@", 2)
		if len(parts) == 2 {
			addrByNodeID[parts[0]] = s.addr
		}
	}
	fmt.Printf("[snapfetch] node_id=%s seeds=%d out=%s\n", nodeKey.ID(), len(seeds), *outRoot)

	listenAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), *listen))
	if err != nil {
		log.Fatalf("listen addr: %v", err)
	}
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      listenAddr.DialString(),
		Network:         *chainID,
		Version:         version.TMCoreSemVer,
		Channels:        []byte{statesync.SnapshotChannel, statesync.ChunkChannel},
		Moniker:         *moniker,
		Other:           p2p.DefaultNodeInfoOther{TxIndex: "off"},
	}
	if err := nodeInfo.Validate(); err != nil {
		log.Fatalf("nodeInfo invalid: %v", err)
	}
	p2pConfig := cfg.DefaultP2PConfig()
	p2pConfig.AllowDuplicateIP = true
	p2pConfig.HandshakeTimeout = 5 * time.Second
	p2pConfig.DialTimeout = 5 * time.Second
	p2pConfig.MaxNumOutboundPeers = 256
	mConfig := conn.DefaultMConnConfig()
	// State-sync peers commonly bump MaxPacketMsgPayloadSize past the default
	// 1024 bytes (e.g. some send single PacketMsgs of 10–100KB). With the
	// default we'd reject those connections at the protoio reader. Set to
	// 256KB to accommodate any reasonable peer config; we only send tiny
	// requests so this doesn't break anyone we connect to.
	mConfig.MaxPacketMsgPayloadSize = 256 * 1024
	// cometbft defaults to 500 KB/s per connection — at that rate one 10MB
	// chunk takes ~20s and a 3GB snapshot from 5 peers takes ~17min. Bump
	// to 10 MB/s so we're peer-side-limited, not us-limited.
	mConfig.SendRate = 10 * 1024 * 1024
	mConfig.RecvRate = 10 * 1024 * 1024

	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, mConfig)
	if err := transport.Listen(*listenAddr); err != nil {
		log.Fatalf("transport.Listen: %v", err)
	}
	ssR := statesync.NewReactor(logger.With("module", "statesync"))
	ssR.KeepBytes = true
	sw := p2p.NewSwitch(p2pConfig, transport)
	sw.SetLogger(logger.With("module", "p2p"))
	sw.SetNodeKey(nodeKey)
	sw.SetNodeInfo(nodeInfo)
	sw.AddReactor("STATESYNC", ssR)
	if err := sw.Start(); err != nil {
		log.Fatalf("switch.Start: %v", err)
	}
	defer func() { _ = sw.Stop() }()

	rootCtx, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() { <-sigCh; cancelAll() }()

	flog := logger.With("module", "snapfetch")

	// Multiplex ssR.Out so phase 1 / phase 2 / phase 3 can each subscribe.
	mux := newEventMux(rootCtx, ssR.Out)
	defer mux.stop()

	// failedKeys collects snapshot keys we've burned through. Used to skip
	// them on rescan so we don't pick the same broken target twice.
	failedKeys := map[string]bool{}

	var (
		chosen     *snapshotOffer
		goodPeers  []p2p.ID
		bytesTotal uint64
		dir        string
		chunkHashes [][]byte
	)
	attempts := *maxRescans + 1
	for attempt := 0; attempt < attempts; attempt++ {
		discDur := *discoverFor
		if attempt > 0 {
			discDur = *rescanDiscoverFor
			fmt.Printf("\n[snapfetch] === RESCAN attempt %d/%d (failed targets: %d) ===\n",
				attempt, *maxRescans, len(failedKeys))
		}

		// ─── Phase 1: discover ─────────────────────────────────────────
		offers := discover(rootCtx, sw, mux.subscribe(), seeds, *dialParallel, discDur, flog)
		if len(offers) == 0 {
			flog.Error("no snapshots discovered")
			continue
		}
		// Drop any snapshot we've already failed on so we don't loop on it.
		for k := range failedKeys {
			delete(offers, k)
		}
		candidates := rankCandidates(offers, *targetHeight, *maxCandidates, *preferFresh)
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
		c, peers := raceProbe(rootCtx, sw, ssR, mux.subscribe(), candidates, addrByNodeID, *probeTimeout, *minGoodPeers, flog)
		if c == nil {
			flog.Error("no candidate had enough good peers", "min_peers", *minGoodPeers)
			continue
		}
		chosen, goodPeers = c, peers
		fmt.Printf("\n[snapfetch] chosen: height=%d format=%d chunks=%d good_peers=%d hash=%s\n",
			chosen.Height, chosen.Format, chosen.Chunks, len(goodPeers), hex.EncodeToString(chosen.Hash)[:16])

		// Parse per-chunk hashes from metadata.
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

		dir = filepath.Join(*outRoot, fmt.Sprintf("%d_%d", chosen.Height, chosen.Format))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Fatalf("mkdir snapshot dir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "metadata.bin"), chosen.Metadata, 0o644); err != nil {
			log.Fatalf("write metadata.bin: %v", err)
		}

		// ─── Phase 3: download all chunks ─────────────────────────────
		fetchCtx, fetchCancel := context.WithTimeout(rootCtx, *maxFetchTime)
		bt, derr := download(fetchCtx, sw, ssR, mux.subscribe(),
			chosen, chunkHashes, goodPeers, addrByNodeID, dir,
			*perPeerLimit, *chunkTimeout, *peerFailLimit, *peerRedialMax, *peerRedialBackoff, flog)
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
		log.Fatalf("exhausted %d rescan attempts without a successful download", attempts)
	}

	// ─── Finalise ──────────────────────────────────────────────────────
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
	if err := writeJSON(filepath.Join(dir, "meta.json"), meta); err != nil {
		log.Fatalf("write meta: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".complete"), nil, 0o644); err != nil {
		log.Fatalf("mark complete: %v", err)
	}
	fmt.Printf("\n[snapfetch] DONE  height=%d format=%d chunks=%d bytes=%s\n",
		chosen.Height, chosen.Format, chosen.Chunks, humanBytes(bytesTotal))
	fmt.Printf("            dir=%s\n", dir)

	// Auto-inspect: enrich meta.json with stores/extensions/chunk_hashes.
	// Failures here are non-fatal — the snapshot itself is already complete
	// and verified.
	if err := inspectAndEnrich(dir); err != nil {
		fmt.Printf("[snapfetch] inspect skipped: %v\n", err)
	}
}

// inspectAndEnrich runs the snapshotinspect package on the just-downloaded
// snapshot and rewrites meta.json with structural details.
func inspectAndEnrich(dir string) error {
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

	// Re-load existing meta.json so we don't drop fields we just wrote.
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
// statesync.Reactor has one Out channel; we want multiple consumers across
// phases. eventMux fans out events to all current subscribers. Subscribers
// drop events if their inbox is full.

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
	// Two ranking modes:
	//   preferFresh=false (default): peer-count primary, height secondary —
	//     maximises odds the snapshot is genuinely fetchable.
	//   preferFresh=true: height primary, peer-count secondary — for when
	//     freshness matters more than redundancy (e.g. state-sync seed for
	//     production node bootstrap).
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
	timeout time.Duration, minGood int, logger cmtlog.Logger) (*snapshotOffer, []p2p.ID) {

	type cand struct {
		offer *snapshotOffer
		good  []p2p.ID
		mu    sync.Mutex
	}
	cands := make([]*cand, len(candidates))
	for i, o := range candidates {
		cands[i] = &cand{offer: o}
	}

	// Build the union of all peers across candidates and (re-)dial each.
	// Phase 1 disconnects after a brief hold to free dial slots; this is
	// where we bring back the peers we actually care about.
	want := map[p2p.ID]string{} // nodeID → addr
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
	// Wait for redial wave to settle; secret-handshake + verification budget.
	waitDone := make(chan struct{})
	go func() { redialWG.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(12 * time.Second):
	}

	// Dispatch chunk-0 to every connected candidate-peer.
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
			// Find which candidate this corresponds to.
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
	// Pick candidate with most good peers (≥ minGood); break ties by newer height.
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
	inflight     int
	failures     int  // missing=true or hash-mismatch responses (real misbehaviour)
	disconnects  int  // socket-level drops (transient)
	banned       bool // permanently bench (real failures or out of redials)
	lastDialAt   time.Time
}

func download(ctx context.Context, sw *p2p.Switch, ssR *statesync.Reactor,
	evs chan statesync.Event, target *snapshotOffer, chunkHashes [][]byte,
	good []p2p.ID, addrByNodeID map[string]string, dir string,
	perPeer int, chunkTimeout time.Duration, peerFailLimit int,
	peerRedialMax int, redialBackoff time.Duration, logger cmtlog.Logger) (uint64, error) {

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
	progressEvery := 5 * time.Second

	// tryRedial fires an async redial for `pid` if it isn't connected and
	// we haven't exceeded the redial budget. Caller must hold no locks.
	tryRedial := func(pid p2p.ID) {
		st, ok := stats[pid]
		if !ok || st.banned {
			return
		}
		if peer := sw.Peers().Get(pid); peer != nil {
			return
		}
		if st.disconnects >= peerRedialMax {
			st.banned = true
			logger.Info("benching peer (redial budget exhausted)",
				"peer", string(pid), "disconnects", st.disconnects)
			return
		}
		if time.Since(st.lastDialAt) < redialBackoff {
			return
		}
		addr, ok := addrByNodeID[string(pid)]
		if !ok {
			st.banned = true
			return
		}
		na, err := p2p.NewNetAddressString(addr)
		if err != nil {
			st.banned = true
			return
		}
		st.disconnects++
		st.lastDialAt = time.Now()
		go func(na *p2p.NetAddress) {
			_ = sw.DialPeerWithAddress(na)
		}(na)
	}

	pickPeer := func() p2p.ID {
		// Prefer the peer with the lowest inflight count, tie-break random-ish.
		var best p2p.ID
		bestInflight := perPeer + 1
		for pid, st := range stats {
			if st.banned {
				continue
			}
			peer := sw.Peers().Get(pid)
			if peer == nil {
				// Peer disconnected — try to bring it back. Don't ban yet;
				// the redial will either restore it or bump us toward the cap.
				continue
			}
			if st.inflight < bestInflight {
				bestInflight = st.inflight
				best = pid
			}
		}
		if bestInflight > perPeer {
			return ""
		}
		return best
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
		"chunks", N, "good_peers", len(good), "per_peer_inflight", perPeer)

	// Initial dispatch.
	dispatch()

	timeoutTicker := time.NewTicker(2 * time.Second)
	defer timeoutTicker.Stop()

	hadEvent := false

	for doneCount < N {
		// "Alive" = not permanently banned. A disconnected peer is still
		// "alive" because we may redial it.
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
			// Re-check inflight deadlines.
			for idx, info := range inflight {
				if now.Sub(info.sent) > chunkTimeout {
					st := stats[info.peer]
					if st != nil {
						st.inflight--
						if st.inflight < 0 {
							st.inflight = 0
						}
						// A timeout could be a slow peer or a disconnected
						// peer. We requeue the chunk and let the redial path
						// (below) handle reconnection. Don't count timeouts
						// as failures — only missing/hash-mismatch.
					}
					delete(inflight, idx)
					pending[idx] = true
				}
			}
			// Bring back disconnected peers within budget.
			for pid, st := range stats {
				if st.banned {
					continue
				}
				if sw.Peers().Get(pid) == nil {
					tryRedial(pid)
				}
			}
			// Try to fill any newly available capacity.
			dispatch()

			if now.Sub(lastProgress) >= progressEvery {
				rate := float64(doneCount) / now.Sub(startTime).Seconds()
				logger.Info("phase 3 progress",
					"done", doneCount, "of", N,
					"bytes", bytesTotal.Load(),
					"chunks_per_s", fmt.Sprintf("%.1f", rate),
					"alive", alive, "connected", connected,
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
			// Decrement inflight if this matches an outstanding request.
			if info, ok := inflight[idx]; ok && info.peer == peer {
				delete(inflight, idx)
			}
			if st.inflight > 0 {
				st.inflight--
			}

			if completed[idx] {
				continue // dup; ignore
			}

			if ev.Chunk.Missing || len(ev.Chunk.Bytes) == 0 {
				st.failures++
				if st.failures >= peerFailLimit {
					st.banned = true
					logger.Info("benching peer", "peer", string(peer), "failures", st.failures)
				}
				pending[idx] = true
				dispatch()
				continue
			}

			// Verify chunk hash.
			h := sha256.Sum256(ev.Chunk.Bytes)
			if !bytesEq(h[:], chunkHashes[idx]) {
				logger.Error("chunk hash mismatch",
					"peer", string(peer), "idx", idx,
					"got_sha", hex.EncodeToString(h[:8]),
					"want_sha", hex.EncodeToString(chunkHashes[idx][:8]))
				st.failures++
				if st.failures >= peerFailLimit {
					st.banned = true
				}
				pending[idx] = true
				dispatch()
				continue
			}

			// Persist.
			path := filepath.Join(dir, fmt.Sprintf("chunk_%05d.bin", idx))
			if err := os.WriteFile(path, ev.Chunk.Bytes, 0o644); err != nil {
				return bytesTotal.Load(), fmt.Errorf("write chunk %d: %w", idx, err)
			}
			completed[idx] = true
			doneCount++
			bytesTotal.Add(uint64(len(ev.Chunk.Bytes)))
			dispatch()
		}

		// If we've gone a long time with no good event, log diagnostically.
		if !hadEvent && time.Since(startTime) > 30*time.Second {
			logger.Error("no chunk replies in 30s; check peers", "alive_peers", alive)
			startTime = time.Now() // reset so the warning isn't spammy
		}
	}
	logger.Info("phase 3: complete",
		"chunks", N,
		"bytes", bytesTotal.Load(),
		"elapsed", time.Since(startTime))
	return bytesTotal.Load(), nil
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
