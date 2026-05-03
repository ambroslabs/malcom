// cosmos-statesync-probe connects to cosmoshub peers and asks each via
// channel 0x60 (SnapshotsRequest) what state-sync snapshots they offer.
// Aggregates unique snapshots by (height, format, hex(hash)) and reports
// chunk count + estimated size. Optionally fetches chunk 0 to measure
// actual chunk-size on the wire.
//
// Uses a separate node_key (default data/probe_node_key.json) and a random
// listen port so it does not collide with a long-running cosmos-archive
// downloader sharing the same data directory.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	"github.com/cometbft/cometbft/version"

	"github.com/zrbecker/cosmos-p2p/internal/crawler"
	"github.com/zrbecker/cosmos-p2p/internal/peers"
	"github.com/zrbecker/cosmos-p2p/internal/statesync"
)

type peerSeed struct {
	addr   string
	source string
}

type snapshotKey struct {
	Height uint64
	Format uint32
	Hash   string // hex
}

type snapshotAgg struct {
	Height       uint64        `json:"height"`
	Format       uint32        `json:"format"`
	Chunks       uint32        `json:"chunks"`
	HashHex      string        `json:"hash_hex"`
	MetadataLen  int           `json:"metadata_len"`
	MetadataHead string        `json:"metadata_head_hex,omitempty"` // first 64 bytes hex
	Peers        []string      `json:"peers"`
	FirstSeen    time.Time     `json:"first_seen"`
	LastSeen     time.Time     `json:"last_seen"`
	ChunkSample  *chunkSample  `json:"chunk_sample,omitempty"`
	peersSet     map[string]bool
}

type chunkSample struct {
	Index   uint32 `json:"index"`
	Bytes   int    `json:"bytes"`
	From    string `json:"from"`
	Missing bool   `json:"missing,omitempty"`
}

func main() {
	var (
		chainID       = flag.String("chain-id", "cosmoshub-4", "expected chain ID")
		cumulativeDB  = flag.String("cumulative", "data/peers-cumulative.json", "peer DB from cosmos-crawl runs")
		addrBookPath  = flag.String("addrbook", "data/polkachu_cosmoshub.json", "Polkachu-style addrbook.json (fallback if cumulative is missing)")
		nodeKeyPath   = flag.String("node-key", "data/probe_node_key.json", "node key file path (separate from downloader/crawler)")
		listen        = flag.String("listen", "tcp://0.0.0.0:0", "p2p listen URL (use :0 for random port)")
		moniker       = flag.String("moniker", "cosmos-p2p-statesync-probe", "self-reported moniker")
		duration      = flag.Duration("duration", 60*time.Second, "wall-clock budget for the probe")
		parallel      = flag.Int("parallel", 16, "max concurrent dials")
		grace         = flag.Duration("grace", 6*time.Second, "how long to keep each peer connected after first SnapshotsResponse")
		fetchChunk    = flag.Bool("fetch-chunk", false, "after seeing a snapshot, fetch chunk 0 to measure actual chunk size")
		outDir        = flag.String("out-dir", "data", "directory to write snapshots-<ts>.json into")
		extraSeedsCSV = flag.String("extra-seeds", "", "comma-separated nodeID@host:port to seed in addition to the DB")
		debug         = flag.Bool("debug", false, "verbose logging")
	)
	flag.Parse()

	logger := cmtlog.NewTMLogger(cmtlog.NewSyncWriter(os.Stderr))
	if *debug {
		logger = cmtlog.NewFilter(logger, cmtlog.AllowDebug())
	} else {
		logger = cmtlog.NewFilter(logger, cmtlog.AllowError(),
			cmtlog.AllowInfoWith("module", "probe"),
			cmtlog.AllowInfoWith("module", "statesync"))
	}

	if err := os.MkdirAll(filepath.Dir(*nodeKeyPath), 0o700); err != nil {
		log.Fatalf("mkdir node-key dir: %v", err)
	}
	nodeKey, err := p2p.LoadOrGenNodeKey(*nodeKeyPath)
	if err != nil {
		log.Fatalf("node key: %v", err)
	}

	seeds := loadSeeds(*cumulativeDB, *addrBookPath, logger)
	for _, s := range strings.Split(*extraSeedsCSV, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			seeds = append([]peerSeed{{addr: s, source: "flag"}}, seeds...)
		}
	}
	if len(seeds) == 0 {
		log.Fatalf("no peer seeds loaded; need %s or %s", *cumulativeDB, *addrBookPath)
	}
	fmt.Printf("[probe] node_id=%s  seeds=%d  duration=%s  parallel=%d  fetch-chunk=%v\n",
		nodeKey.ID(), len(seeds), *duration, *parallel, *fetchChunk)

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
	p2pConfig.MaxNumOutboundPeers = *parallel * 4
	mConfig := conn.DefaultMConnConfig()

	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, mConfig)
	if err := transport.Listen(*listenAddr); err != nil {
		log.Fatalf("transport.Listen %s: %v", listenAddr, err)
	}

	ssR := statesync.NewReactor(logger.With("module", "statesync"))

	sw := p2p.NewSwitch(p2pConfig, transport)
	sw.SetLogger(logger.With("module", "p2p"))
	sw.SetNodeKey(nodeKey)
	sw.SetNodeInfo(nodeInfo)
	sw.AddReactor("STATESYNC", ssR)

	if err := sw.Start(); err != nil {
		log.Fatalf("switch.Start: %v", err)
	}
	defer func() { _ = sw.Stop() }()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() { <-sigCh; cancel() }()

	// Aggregator + dial worker pool.
	probeLog := logger.With("module", "probe")
	aggMu := sync.Mutex{}
	agg := map[snapshotKey]*snapshotAgg{}
	connectedPeers := map[string]bool{}
	noSnapshotPeers := map[string]bool{}

	// Event consumer: pull from ssR.Out, update aggregator, optionally fire ChunkRequest.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-ssR.Out:
				if ev.Snapshot != nil {
					k := snapshotKey{
						Height: ev.Snapshot.Height,
						Format: ev.Snapshot.Format,
						Hash:   hex.EncodeToString(ev.Snapshot.Hash),
					}
					aggMu.Lock()
					rec, ok := agg[k]
					if !ok {
						head := ev.Snapshot.Metadata
						if len(head) > 64 {
							head = head[:64]
						}
						rec = &snapshotAgg{
							Height:       ev.Snapshot.Height,
							Format:       ev.Snapshot.Format,
							Chunks:       ev.Snapshot.Chunks,
							HashHex:      k.Hash,
							MetadataLen:  len(ev.Snapshot.Metadata),
							MetadataHead: hex.EncodeToString(head),
							FirstSeen:    time.Now(),
							peersSet:     map[string]bool{},
						}
						agg[k] = rec
					}
					if !rec.peersSet[ev.PeerID] {
						rec.peersSet[ev.PeerID] = true
						rec.Peers = append(rec.Peers, ev.PeerID)
					}
					rec.LastSeen = time.Now()
					shouldFetch := *fetchChunk && rec.ChunkSample == nil
					aggMu.Unlock()

					if shouldFetch {
						peer := sw.Peers().Get(p2p.ID(ev.PeerID))
						if peer != nil {
							ok := ssR.RequestChunk(peer, ev.Snapshot.Height, ev.Snapshot.Format, 0)
							probeLog.Info("ChunkRequest dispatched",
								"peer", ev.PeerID, "height", ev.Snapshot.Height,
								"format", ev.Snapshot.Format, "ok", ok)
						}
					}
				}
				if ev.Chunk != nil {
					aggMu.Lock()
					for _, rec := range agg {
						if rec.Height == ev.Chunk.Height && rec.Format == ev.Chunk.Format && rec.ChunkSample == nil {
							rec.ChunkSample = &chunkSample{
								Index:   ev.Chunk.Index,
								Bytes:   ev.Chunk.Size,
								From:    ev.PeerID,
								Missing: ev.Chunk.Missing,
							}
							break
						}
					}
					aggMu.Unlock()
				}
			}
		}
	}()

	// Dial workers.
	queue := make(chan peerSeed, len(seeds)+1024)
	self := nodeKey.ID()
	for _, s := range seeds {
		// Skip our own ID just in case the cumulative DB picked us up.
		if strings.HasPrefix(s.addr, string(self)+"@") {
			continue
		}
		queue <- s
	}
	close(queue)

	var wg sync.WaitGroup
	for i := 0; i < *parallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case s, ok := <-queue:
					if !ok {
						return
					}
					dialOne(ctx, sw, s, *grace, &aggMu, connectedPeers, noSnapshotPeers, agg, probeLog)
				}
			}
		}()
	}

	// Periodic progress.
	progressDone := make(chan struct{})
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				close(progressDone)
				return
			case <-t.C:
				aggMu.Lock()
				nUniq := len(agg)
				nConn := len(connectedPeers)
				nNoSnap := len(noSnapshotPeers)
				nWithSnap := nConn - nNoSnap
				if nWithSnap < 0 {
					nWithSnap = 0
				}
				aggMu.Unlock()
				recv, sent := ssR.Bytes()
				probeLog.Info("progress",
					"connected", nConn, "with_snapshot", nWithSnap,
					"unique_snapshots", nUniq,
					"recv_kb", recv/1024, "sent_kb", sent/1024,
					"queued", len(queue))
			}
		}
	}()

	wg.Wait()
	<-ctx.Done()
	<-progressDone

	// Finalise & dump.
	aggMu.Lock()
	out := make([]*snapshotAgg, 0, len(agg))
	for _, v := range agg {
		out = append(out, v)
	}
	connSnap := len(connectedPeers)
	noSnap := len(noSnapshotPeers)
	aggMu.Unlock()

	sort.Slice(out, func(i, j int) bool {
		if out[i].Height != out[j].Height {
			return out[i].Height > out[j].Height
		}
		return out[i].Format < out[j].Format
	})

	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("mkdir out: %v", err)
	}
	ts := time.Now().UTC().Format("20060102T150405Z")
	outPath := filepath.Join(*outDir, fmt.Sprintf("snapshots-%s.json", ts))
	if err := writeJSON(outPath, out); err != nil {
		log.Fatalf("write %s: %v", outPath, err)
	}

	summarize(out, connSnap, noSnap, outPath)
}

func dialOne(ctx context.Context, sw *p2p.Switch, s peerSeed, grace time.Duration,
	mu *sync.Mutex, conns, noSnap map[string]bool, agg map[snapshotKey]*snapshotAgg, logger cmtlog.Logger) {

	na, err := p2p.NewNetAddressString(s.addr)
	if err != nil {
		return
	}
	if err := sw.DialPeerWithAddress(na); err != nil {
		// Most errors are routine: timeouts, "already dialing", "is self", "duplicate".
		// Quietly drop them.
		return
	}
	pid := string(na.ID)

	mu.Lock()
	conns[pid] = true
	mu.Unlock()

	// Wait `grace` seconds with a poll for early disconnect.
	deadline := time.NewTimer(grace)
	defer deadline.Stop()
	select {
	case <-ctx.Done():
	case <-deadline.C:
	}

	// Note whether this peer offered any snapshot.
	mu.Lock()
	any := false
	for _, rec := range agg {
		if rec.peersSet[pid] {
			any = true
			break
		}
	}
	if !any {
		noSnap[pid] = true
	}
	mu.Unlock()

	if peer := sw.Peers().Get(na.ID); peer != nil {
		sw.StopPeerGracefully(peer)
	}
}

func loadSeeds(cumPath, addrBookPath string, logger cmtlog.Logger) []peerSeed {
	out := []peerSeed{}
	seen := map[string]bool{}

	// Cumulative DB: prefer entries that responded recently and look healthy.
	if f, err := os.Open(cumPath); err == nil {
		defer f.Close()
		var recs []crawler.PeerRecord
		if err := json.NewDecoder(f).Decode(&recs); err == nil {
			// Sort by latest_height desc — peers actively at tip are more likely
			// to be alive and to have snapshots configured.
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
		// Fallback to the static addrbook.
		items, err := peers.Load(addrBookPath)
		if err != nil {
			logger.Error("load addrbook fallback failed", "path", addrBookPath, "err", err)
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

func summarize(out []*snapshotAgg, connSnap, noSnap int, outPath string) {
	fmt.Printf("[probe] done.\n")
	fmt.Printf("        peers connected:        %d\n", connSnap)
	fmt.Printf("        peers w/ no snapshots:  %d\n", noSnap)
	withSnap := connSnap - noSnap
	if withSnap < 0 {
		withSnap = 0
	}
	fmt.Printf("        peers w/ snapshots:     %d\n", withSnap)
	fmt.Printf("        unique snapshots:       %d\n", len(out))
	if len(out) > 0 {
		fmt.Printf("\n        height       fmt   chunks   ~size@10MB   peers   meta   chunk0_bytes\n")
		for _, s := range out {
			est := uint64(s.Chunks) * 10 * 1024 * 1024
			cs := "—"
			if s.ChunkSample != nil {
				if s.ChunkSample.Missing {
					cs = "pruned"
				} else {
					cs = strconv.Itoa(s.ChunkSample.Bytes)
				}
			}
			fmt.Printf("        %-10d   %-3d   %-6d   %-10s   %-5d   %-4d   %s\n",
				s.Height, s.Format, s.Chunks, humanBytes(est), len(s.Peers), s.MetadataLen, cs)
		}
	}
	fmt.Printf("\n        wrote %s\n", outPath)
}

func humanBytes(n uint64) string {
	const (
		k = 1024
		m = k * 1024
		g = m * 1024
	)
	switch {
	case n >= g:
		return fmt.Sprintf("%.1f GB", float64(n)/float64(g))
	case n >= m:
		return fmt.Sprintf("%.1f MB", float64(n)/float64(m))
	case n >= k:
		return fmt.Sprintf("%.1f KB", float64(n)/float64(k))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
