// cosmos-archive is the multi-tool for the on-disk block archive.
//
//	cosmos-archive ranges  -archive <dir>            list contiguous-have runs
//	cosmos-archive missing -archive <dir> -lo H -hi H  list gap runs in [lo,hi]
//	cosmos-archive stats   -archive <dir>            shard-by-shard summary
//	cosmos-archive download -archive <dir> ...       fetch missing blocks
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	"github.com/cometbft/cometbft/version"

	"github.com/zrbecker/cosmos-p2p/internal/archive"
	"github.com/zrbecker/cosmos-p2p/internal/archivesync"
)

func usage() {
	fmt.Fprintf(os.Stderr, `cosmos-archive — manage the on-disk block archive

Usage:
  cosmos-archive <subcommand> [flags]

Subcommands:
  ranges     show contiguous-present height ranges
  missing    show height ranges we don't have within [lo, hi]
  stats      per-shard counts and the global summary
  download   fetch missing blocks from archive peers (long-running)

Run any subcommand with -h for its flags.
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "ranges":
		runRanges(os.Args[2:])
	case "missing":
		runMissing(os.Args[2:])
	case "stats":
		runStats(os.Args[2:])
	case "download":
		runDownload(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func openStore(dir string) *archive.Store {
	if dir == "" {
		fmt.Fprintln(os.Stderr, "error: -archive required")
		os.Exit(2)
	}
	st, err := archive.New(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open archive %s: %v\n", dir, err)
		os.Exit(1)
	}
	return st
}

// commafmt prints a uint64 with thousands separators.
func commafmt(n uint64) string {
	s := fmt.Sprintf("%d", n)
	out := make([]byte, 0, len(s)+len(s)/3)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	return string(out)
}

func runRanges(args []string) {
	fs := flag.NewFlagSet("ranges", flag.ExitOnError)
	dir := fs.String("archive", "/mnt/data/cosmos-archive/cosmoshub-4", "archive root directory")
	_ = fs.Parse(args)

	st := openStore(*dir)
	defer st.Close()

	ranges, total, err := st.Ranges()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ranges: %v\n", err)
		os.Exit(1)
	}
	if len(ranges) == 0 {
		fmt.Println("(no blocks present)")
		return
	}
	fmt.Println("have:")
	for _, r := range ranges {
		fmt.Printf("  %14s .. %14s  (%14s blocks)\n",
			commafmt(r.Lo), commafmt(r.Hi), commafmt(r.Count()))
	}
	fmt.Printf("  %s\n", "─────────────────────────────────────────────────────────")
	fmt.Printf("  total: %s blocks across %d range(s)\n", commafmt(total), len(ranges))
}

func runMissing(args []string) {
	fs := flag.NewFlagSet("missing", flag.ExitOnError)
	dir := fs.String("archive", "/mnt/data/cosmos-archive/cosmoshub-4", "archive root directory")
	lo := fs.Uint64("lo", 0, "lowest height of the query window (inclusive). 0 ⇒ first present height (or 1)")
	hi := fs.Uint64("hi", 0, "highest height of the query window (inclusive). 0 ⇒ last present height")
	_ = fs.Parse(args)

	st := openStore(*dir)
	defer st.Close()

	have, _, err := st.Ranges()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ranges: %v\n", err)
		os.Exit(1)
	}
	if *lo == 0 {
		if len(have) > 0 {
			*lo = have[0].Lo
		} else {
			*lo = 1
		}
	}
	if *hi == 0 {
		if len(have) > 0 {
			*hi = have[len(have)-1].Hi
		} else {
			*hi = *lo
		}
	}

	gaps, missing, err := st.Missing(*lo, *hi)
	if err != nil {
		fmt.Fprintf(os.Stderr, "missing: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("query: [%s .. %s]\n", commafmt(*lo), commafmt(*hi))
	if len(gaps) == 0 {
		fmt.Println("no missing blocks in window")
		return
	}
	fmt.Println("missing:")
	for _, r := range gaps {
		fmt.Printf("  %14s .. %14s  (%14s blocks)\n",
			commafmt(r.Lo), commafmt(r.Hi), commafmt(r.Count()))
	}
	fmt.Printf("  %s\n", "─────────────────────────────────────────────────────────")
	fmt.Printf("  total: %s blocks across %d gap(s)\n", commafmt(missing), len(gaps))
}

func runStats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	dir := fs.String("archive", "/mnt/data/cosmos-archive/cosmoshub-4", "archive root directory")
	_ = fs.Parse(args)

	st := openStore(*dir)
	defer st.Close()

	bases, err := st.ShardBases()
	if err != nil {
		fmt.Fprintf(os.Stderr, "list shards: %v\n", err)
		os.Exit(1)
	}
	if len(bases) == 0 {
		fmt.Println("(no shards)")
		return
	}
	var totalCount uint64
	fmt.Printf("%-14s  %-14s  %-14s  %-10s\n", "shard_base", "min_height", "max_height", "count")
	for _, base := range bases {
		// Have to open shard to inspect; openStore caches.
		_, _, _ = st.Ranges() // ensures all shards opened
		// Use Has + AllEntries for counts via Ranges machinery would re-walk.
		// Cheaper: just read shard, call PresentRange.
		// Open the shard ourselves:
		// (We don't have a public accessor; use Has on first/last via ranges already.)
		// Simpler: re-implement via a fresh open — the Store keeps it cached.
		// For each shard fetch PresentRange via a tiny indirection:
		//
		// We'll just use the existing scanned ranges per-shard by computing
		// the intersection — but that's an O(R) loop where R = # ranges.
		// For 257 shards × small R, fine.
		_ = base
	}
	// Walk shards via a fresh per-shard scan for simplicity — re-use Ranges().
	allRanges, total, err := st.Ranges()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ranges: %v\n", err)
		os.Exit(1)
	}
	for _, base := range bases {
		shardLo := base
		shardHi := base + archive.ChunkSize - 1
		var minH, maxH uint64
		var cnt uint64
		for _, r := range allRanges {
			if r.Hi < shardLo || r.Lo > shardHi {
				continue
			}
			lo, hi := r.Lo, r.Hi
			if lo < shardLo {
				lo = shardLo
			}
			if hi > shardHi {
				hi = shardHi
			}
			if cnt == 0 {
				minH = lo
			}
			maxH = hi
			cnt += hi - lo + 1
		}
		if cnt == 0 {
			fmt.Printf("%14s  %-14s  %-14s  %10s\n", commafmt(base), "—", "—", "0")
		} else {
			fmt.Printf("%14s  %14s  %14s  %10s\n",
				commafmt(base), commafmt(minH), commafmt(maxH), commafmt(cnt))
		}
		totalCount += cnt
	}
	fmt.Printf("─────────────────────────────────────────────────────────────\n")
	fmt.Printf("shards=%d  blocks=%s  ranges=%d\n", len(bases), commafmt(total), len(allRanges))
	_ = totalCount
}

// peerCand mirrors what the cosmos-blockcache cumulative DB stores.
type peerCand struct {
	Addr         string `json:"addr"`
	NodeID       string `json:"node_id"`
	BaseHeight   int64  `json:"base_height"`
	LatestHeight int64  `json:"latest_height"`
	Network      string `json:"network"`
}

func loadArchivePeers(path string, archiveBase int64) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var all []peerCand
	if err := json.NewDecoder(f).Decode(&all); err != nil {
		return nil
	}
	good := make([]peerCand, 0, len(all))
	for _, p := range all {
		if p.Network != "cosmoshub-4" {
			continue
		}
		if p.LatestHeight == 0 {
			continue
		}
		// Only keep peers reaching back at least to archiveBase.
		if p.BaseHeight == 0 || p.BaseHeight > archiveBase {
			continue
		}
		good = append(good, p)
	}
	// Prefer deepest history first.
	sort.Slice(good, func(i, j int) bool {
		return good[i].BaseHeight < good[j].BaseHeight
	})
	out := make([]string, 0, len(good))
	for _, p := range good {
		out = append(out, p.Addr)
	}
	return out
}

func runDownload(args []string) {
	fs := flag.NewFlagSet("download", flag.ExitOnError)
	dir := fs.String("archive", "/mnt/data/cosmos-archive/cosmoshub-4", "archive root directory")
	chainID := fs.String("chain-id", "cosmoshub-4", "expected chain ID")
	nodeKeyPath := fs.String("node-key", "data/node_key.json", "node key file path")
	listen := fs.String("listen", "tcp://0.0.0.0:0", "p2p bind address")
	moniker := fs.String("moniker", "cosmos-archive-fetcher", "self moniker")
	peersDB := fs.String("peers", "data/peers-cumulative.json", "load archive peers from this cumulative DB")
	maxPeers := fs.Int("max-peers", 16, "concurrent outbound peer connections")
	maxInflight := fs.Int("max-inflight", 512, "global concurrent BlockRequests")
	maxInflightPeer := fs.Int("max-inflight-per-peer", 32, "concurrent BlockRequests per peer (cosmoshub archive nodes seem to handle ≥32 fine)")
	dialWorkers := fs.Int("dial-workers", 8, "parallel dial workers (helps when target peers are slow to handshake)")
	loFlag := fs.Int64("lo", 5_200_791, "lowest height to download")
	hiFlag := fs.Int64("hi", 0, "highest height to download (0 ⇒ derived from peer status, capped to network tip)")
	debug := fs.Bool("debug", false, "verbose logging")
	_ = fs.Parse(args)

	logger := cmtlog.NewTMLogger(cmtlog.NewSyncWriter(os.Stderr))
	if *debug {
		logger = cmtlog.NewFilter(logger, cmtlog.AllowDebug())
	} else {
		logger = cmtlog.NewFilter(logger, cmtlog.AllowError(),
			cmtlog.AllowInfoWith("module", "archivesync"))
	}

	// Open archive store.
	st, err := archive.New(*dir)
	if err != nil {
		log.Fatalf("open archive: %v", err)
	}
	defer st.Close()

	// Compute initial work queue from missing-set.
	logger.Info("scanning archive", "root", *dir)
	have, _, err := st.Ranges()
	if err != nil {
		log.Fatalf("scan ranges: %v", err)
	}
	initialHi := *hiFlag
	if initialHi == 0 {
		// Default to a few blocks behind the live tip; we'll widen on
		// status responses.
		if len(have) > 0 {
			initialHi = int64(have[len(have)-1].Hi)
		}
		if initialHi < *loFlag {
			initialHi = *loFlag + 1_000_000 // probe; will be raised
		}
	}
	gaps, missing, err := st.Missing(uint64(*loFlag), uint64(initialHi))
	if err != nil {
		log.Fatalf("compute missing: %v", err)
	}
	queue := archivesync.NewQueue()
	for _, g := range gaps {
		queue.AddRange(int64(g.Lo), int64(g.Hi))
	}
	logger.Info("initial work queue",
		"lo", *loFlag, "hi", initialHi,
		"missing_blocks", missing, "gap_count", len(gaps))

	// Set up p2p.
	if err := os.MkdirAll(filepath.Dir(*nodeKeyPath), 0o700); err != nil {
		log.Fatal(err)
	}
	nodeKey, err := p2p.LoadOrGenNodeKey(*nodeKeyPath)
	if err != nil {
		log.Fatalf("node key: %v", err)
	}
	listenAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), *listen))
	if err != nil {
		log.Fatal(err)
	}
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      listenAddr.DialString(),
		Network:         *chainID,
		Version:         version.TMCoreSemVer,
		Channels:        []byte{archivesync.Channel},
		Moniker:         *moniker,
		Other:           p2p.DefaultNodeInfoOther{TxIndex: "off"},
	}
	if err := nodeInfo.Validate(); err != nil {
		log.Fatalf("nodeInfo: %v", err)
	}
	p2pCfg := cfg.DefaultP2PConfig()
	p2pCfg.AllowDuplicateIP = true
	p2pCfg.HandshakeTimeout = 5 * time.Second
	p2pCfg.DialTimeout = 5 * time.Second
	p2pCfg.MaxNumOutboundPeers = *maxPeers
	mConfig := conn.DefaultMConnConfig()

	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, mConfig)
	if err := transport.Listen(*listenAddr); err != nil {
		log.Fatalf("transport.Listen: %v", err)
	}

	reactor := archivesync.NewReactor(st, queue, logger.With("module", "archivesync"))
	reactor.MaxInflight = *maxInflight
	reactor.MaxInflightPerPeer = *maxInflightPeer
	reactor.MinPeerBase = *loFlag

	sw := p2p.NewSwitch(p2pCfg, transport)
	sw.SetLogger(logger.With("module", "p2p"))
	sw.SetNodeKey(nodeKey)
	sw.SetNodeInfo(nodeInfo)
	sw.AddReactor("ARCHIVE", reactor)
	if err := sw.Start(); err != nil {
		log.Fatalf("switch.Start: %v", err)
	}
	defer func() { _ = sw.Stop() }()

	candidates := loadArchivePeers(*peersDB, *loFlag)
	if len(candidates) == 0 {
		log.Fatalf("no archive-eligible peers in %s (require base ≤ %d)", *peersDB, *loFlag)
	}
	logger.Info("archive-eligible peer candidates", "count", len(candidates))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println()
		logger.Info("interrupt; shutting down")
		_ = st.Sync()
		cancel()
	}()

	// Dial candidates in parallel; each worker pulls from a shared cursor.
	pool := &dialPoolState{}
	for i := 0; i < *dialWorkers; i++ {
		go dialCycle(ctx, sw, candidates, *maxPeers, pool, logger.With("module", "archivesync", "dial", i))
	}

	// Drive the reactor.
	go reactor.SyncLoop(ctx)

	// Periodic fsync so a kill -9 doesn't lose more than a few seconds.
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				_ = st.Sync()
			}
		}
	}()

	// Progress printer.
	startTime := time.Now()
	prev := reactor.Snapshot()
	prevAt := startTime
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s := reactor.Snapshot()
			elapsed := time.Since(startTime).Seconds()
			fmt.Printf("[exit] received=%d written=%d noblock=%d timedout=%d  inflight=%d  bytes_in=%dKB  over %.0fs (%.1f blk/s)\n",
				s.Received, s.Written, s.NoBlock, s.TimedOut, s.Inflight, s.BytesIn/1024, elapsed, float64(s.Written)/elapsed)
			return
		case now := <-t.C:
			s := reactor.Snapshot()
			dt := now.Sub(prevAt).Seconds()
			rate := float64(s.Written-prev.Written) / dt
			pending := queue.Size()
			eta := time.Duration(0)
			if rate > 0 {
				eta = time.Duration(float64(pending)/rate) * time.Second
			}
			fmt.Printf("[archive] queue=%d written=%d (Δ%d, %.1f/s) recv=%d noblock=%d timedout=%d  peers=%d (eligible=%d)  inflight=%d  eta=%s\n",
				pending,
				s.Written, s.Written-prev.Written, rate,
				s.Received, s.NoBlock, s.TimedOut,
				s.Peers, s.EligibleP, s.Inflight,
				eta.Truncate(time.Second))
			prev, prevAt = s, now
		}
	}
}

// dialPoolState shares a round-robin cursor across parallel dialCycle workers.
type dialPoolState struct {
	mu     sync.Mutex
	cursor int
}

// dialCycle keeps trying candidates until we hit `want` outbound peers,
// pulling from a shared cursor so multiple instances don't all attempt
// the same address at once.
func dialCycle(ctx context.Context, sw *p2p.Switch, candidates []string, want int, pool *dialPoolState, logger cmtlog.Logger) {
	if pool == nil {
		pool = &dialPoolState{}
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		out, _, _ := sw.NumPeers()
		if out >= want {
			time.Sleep(2 * time.Second)
			continue
		}
		// Pop the next candidate.
		pool.mu.Lock()
		if pool.cursor >= len(candidates) {
			pool.cursor = 0
			pool.mu.Unlock()
			time.Sleep(2 * time.Second)
			continue
		}
		addr := candidates[pool.cursor]
		pool.cursor++
		pool.mu.Unlock()

		na, err := p2p.NewNetAddressString(addr)
		if err != nil {
			continue
		}
		if na.ID == sw.NodeInfo().ID() {
			continue
		}
		if err := sw.DialPeerWithAddress(na); err != nil {
			logger.Debug("dial failed", "peer", na.ID, "err", err)
		} else {
			logger.Info("connected", "peer", na.ID)
		}
	}
}
