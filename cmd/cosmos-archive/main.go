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
	"strings"
	"sync"
	"syscall"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	tmp2pproto "github.com/cometbft/cometbft/proto/tendermint/p2p"
	"github.com/cometbft/cometbft/version"
	gnet "net"

	"github.com/zrbecker/cosmos-p2p/internal/archive"
	"github.com/zrbecker/cosmos-p2p/internal/archivesync"
	"github.com/zrbecker/cosmos-p2p/internal/pex"
)

func usage() {
	fmt.Fprintf(os.Stderr, `cosmos-archive — manage the on-disk block archive

Usage:
  cosmos-archive <subcommand> [flags]

Subcommands:
  ranges     show contiguous-present height ranges
  missing    show height ranges we don't have within [lo, hi]
  stats      per-shard counts and the global summary
  status     one-screen dashboard (latest [archive] line + ranges + disk)
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
	case "status":
		runStatus(os.Args[2:])
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

// runStatus prints a snapshot of download progress + on-disk state. Use
// with `watch` for a live dashboard:
//   watch -n 2 'cosmos-archive status'
func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	dir := fs.String("archive", "/mnt/data/cosmos-archive/cosmoshub-4", "archive root directory")
	logPath := fs.String("log", "/mnt/data/cosmos-archive/logs/download-current.out", "downloader stdout log to tail")
	tailN := fs.Int("n", 3, "number of recent [archive] lines to show")
	_ = fs.Parse(args)

	// process running?
	running := false
	if out, err := os.ReadFile("/proc/self/status"); err == nil {
		_ = out
	}
	// pgrep cosmos-archive download
	if entries, err := os.ReadDir("/proc"); err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			cmdline, err := os.ReadFile(filepath.Join("/proc", e.Name(), "cmdline"))
			if err != nil {
				continue
			}
			s := string(cmdline)
			if strings.Contains(s, "cosmos-archive") && strings.Contains(s, "download") {
				running = true
				fmt.Printf("downloader: running (pid %s)\n", e.Name())
				break
			}
		}
	}
	if !running {
		fmt.Println("downloader: NOT running")
	}

	// last N [archive] lines from the log
	if data, err := os.ReadFile(*logPath); err == nil {
		lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
		var hits []string
		for _, l := range lines {
			if strings.HasPrefix(l, "[archive]") {
				hits = append(hits, l)
			}
		}
		fmt.Printf("\n=== last %d [archive] lines ===\n", *tailN)
		from := 0
		if len(hits) > *tailN {
			from = len(hits) - *tailN
		}
		for _, l := range hits[from:] {
			fmt.Println(l)
		}
	} else {
		fmt.Printf("\n(log %s unavailable: %v)\n", *logPath, err)
	}

	// ranges
	st, err := archive.New(*dir)
	if err != nil {
		fmt.Printf("\nopen archive: %v\n", err)
		return
	}
	defer st.Close()
	ranges, total, err := st.Ranges()
	if err == nil {
		fmt.Printf("\n=== on-disk ranges ===\n")
		if len(ranges) == 0 {
			fmt.Println("(no blocks present)")
		} else {
			for _, r := range ranges {
				fmt.Printf("  %14s .. %14s  (%14s blocks)\n",
					commafmt(r.Lo), commafmt(r.Hi), commafmt(r.Count()))
			}
			fmt.Printf("  total: %s blocks across %d range(s)\n",
				commafmt(total), len(ranges))
		}
	}

	// disk usage of the shards dir
	shardsDir := filepath.Join(*dir, "shards")
	if du, err := dirSize(shardsDir); err == nil {
		fmt.Printf("\n=== disk ===\n")
		fmt.Printf("  shards: %s on disk (%s files)\n", humanBytes(du.bytes), commafmt(uint64(du.files)))
	}
}

type duInfo struct {
	bytes int64
	files int
}

func dirSize(dir string) (duInfo, error) {
	var info duInfo
	ents, err := os.ReadDir(dir)
	if err != nil {
		return info, err
	}
	for _, e := range ents {
		fi, err := e.Info()
		if err != nil {
			continue
		}
		info.bytes += fi.Size()
		info.files++
	}
	return info, nil
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

// chainRegistrySeeds: hand-curated cosmoshub seeds (PEX-rich nodes that
// hand out address batches). Same list cosmos-blockcache uses.
func chainRegistrySeeds() []string {
	return []string{
		"ba3bacc714817218562f743178228f23678b2873@public-seed-node.cosmoshub.certus.one:26656",
		"ade4d8bc8cbe014af6ebdf3cb7b1e9ad36f412c0@seeds.polkachu.com:14956",
		"20e1000e88125698264454a884812746c2eb4807@seeds.lavenderfive.com:14956",
		"57a5297537b9b6ef8b105c08a8ad3f6ac452c423@seeds.goldenratiostaking.net:1618",
		"c28827cb96c14c905b127b92065a3fb4cd77d7f6@seeds.whispernode.com:14956",
		"8542cd7e6bf9d260fef543bc49e59be5a3fa9074@seed.publicnode.com:26656",
		"400f3d9e30b69e78a7fb891f60d76fa3c73f0ecc@cosmoshub.rpc.kjnodes.com:11359",
		"fe21dd474640247888fc7c4dce82da8da08a8bfd@seed-cosmos-hub-01.stakeflow.io:26656",
		"11c6114a18f7b380e536b0bd17c031f4746e4ded@seed-node.mms.team:43656",
		"87ccc1dcc0b846fc1623ab9a5ab55682e8e2ad2e@seed-cosmoshub.freshstaking.com:26656",
		"b85358e035343a3b15e77e1102857dcdaf70053b@seeds.bluestake.net:28156",
		"00bf1f9d3c65137dc99c40cd03864384ce0ef7c3@cosmoshub-mainnet-seed.itrocket.net:34656",
		"10ed1e176d874c8bb3c7c065685d2da6a4b86475@seed-cosmos.ibs.team:16685",
		"d567c93fa5b646c8cca8ba0a2d7499bca6aeba52@mainnet.seednode.citizenweb3.com:26656",
	}
}

// loadAllPeers returns every peer address from the cumulative DB,
// regardless of base height. Used as a low-priority dial pool feeder so we
// keep many connections open for PEX gossip.
func loadAllPeers(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var all []peerCand
	if err := json.NewDecoder(f).Decode(&all); err != nil {
		return nil
	}
	out := make([]string, 0, len(all))
	for _, p := range all {
		if p.Network == "" || p.Network == "cosmoshub-4" {
			if p.Addr != "" {
				out = append(out, p.Addr)
			}
		}
	}
	return out
}

// consumePEX pulls every PexAddrs batch into the dialer. The reactor's
// backoff and seen-set make this idempotent.
func consumePEX(ctx context.Context, pexR *pex.Reactor, d *dialer, logger cmtlog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-pexR.Out:
			added := 0
			for _, na := range ev.Addrs {
				if na.IP == "" || na.Port == 0 || na.ID == "" {
					continue
				}
				host := na.IP
				if ip := gnet.ParseIP(host); ip != nil && ip.To4() == nil {
					host = "[" + host + "]"
				}
				addr := fmt.Sprintf("%s@%s:%d", na.ID, host, na.Port)
				if d.add(addr) {
					added++
				}
			}
			if added > 0 {
				logger.Info("pex grew dialer", "new", added, "from", ev.Source[:10], "pool_size", d.size())
			}
		}
	}
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
	maxPeers := fs.Int("max-peers", 50, "concurrent outbound peer connections (most are non-archive; we keep them for PEX gossip and only BlockRequest archive-eligible peers)")
	maxInflight := fs.Int("max-inflight", 512, "global concurrent BlockRequests")
	maxInflightPeer := fs.Int("max-inflight-per-peer", 32, "concurrent BlockRequests per peer (cosmoshub archive nodes seem to handle ≥32 fine)")
	dialWorkers := fs.Int("dial-workers", 16, "parallel dial workers (helps when target peers are slow to handshake)")
	externalAddr := fs.String("external-addr", "", "publicly-dialable host:port to advertise via PEX. Empty disables PEX-server-side. (e.g. 64.23.187.105:26656)")
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
	advertisedAddr := listenAddr.DialString()
	if *externalAddr != "" {
		advertisedAddr = *externalAddr
	}
	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      advertisedAddr,
		Network:         *chainID,
		Version:         version.TMCoreSemVer,
		Channels:        []byte{pex.Channel, archivesync.Channel},
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

	pexR := pex.NewReactor(logger.With("module", "pex"))
	if *externalAddr != "" {
		if naSelf, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), "tcp://"+*externalAddr)); err == nil {
			pexR.SetSelf(*naSelf)
		}
	}

	sw := p2p.NewSwitch(p2pCfg, transport)
	sw.SetLogger(logger.With("module", "p2p"))
	sw.SetNodeKey(nodeKey)
	sw.SetNodeInfo(nodeInfo)
	sw.AddReactor("PEX", pexR)
	sw.AddReactor("ARCHIVE", reactor)
	if err := sw.Start(); err != nil {
		log.Fatalf("switch.Start: %v", err)
	}
	defer func() { _ = sw.Stop() }()

	// Build the dial pool with three sources:
	//   1. The 8 known archive nodes (highest priority — pinned first).
	//   2. Chain-registry seed nodes (PEX-rich; gossip more peers to us).
	//   3. The full cumulative DB (most non-archive but we keep them for
	//      PEX gossip; the archivesync reactor's MinPeerBase filter means
	//      we won't BlockRequest from them).
	dialer := newDialer(string(nodeKey.ID()))
	archiveCands := loadArchivePeers(*peersDB, *loFlag)
	for _, a := range archiveCands {
		dialer.add(a)
	}
	for _, a := range chainRegistrySeeds() {
		dialer.add(a)
	}
	for _, a := range loadAllPeers(*peersDB) {
		dialer.add(a)
	}
	if dialer.size() == 0 {
		log.Fatalf("no peer candidates")
	}
	logger.Info("dial pool",
		"archive_nodes", len(archiveCands),
		"chain_registry_seeds", len(chainRegistrySeeds()),
		"total_initial", dialer.size())

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

	// Dial workers pull from the shared dialer (with per-peer backoff).
	for i := 0; i < *dialWorkers; i++ {
		go dialCycle(ctx, sw, dialer, *maxPeers, logger.With("module", "archivesync", "dial", i))
	}

	// PEX consumer: every PexAddrs batch we receive feeds into the dialer.
	go consumePEX(ctx, pexR, dialer, logger.With("module", "archivesync"))

	// Periodic re-PEX: ask each currently-connected peer for fresh peer
	// addresses every 60s. Cometbft enforces a 40s minimum between PEX
	// requests; 60s leaves margin.
	go func() {
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		req := &tmp2pproto.PexRequest{}
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				for _, p := range sw.Peers().List() {
					p.TrySend(p2p.Envelope{ChannelID: pex.Channel, Message: req})
				}
			}
		}
	}()

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

// dialer is a thread-safe pool of nodeID@host:port candidates with
// per-peer exponential backoff after failures. Cycles forever; the dial
// loop pulls the next ready candidate.
//
// Backoff schedule: failure i waits min(initialBackoff << i, maxBackoff)
// before the peer is eligible again. Successful dials reset the counter.
type dialer struct {
	self string
	mu   sync.Mutex
	// Stable arrival order so we always retry the original archive nodes
	// first, even after PEX adds thousands of new entries.
	order   []string
	state   map[string]*dialState
	cursor  int

	initialBackoff time.Duration
	maxBackoff     time.Duration
}

type dialState struct {
	failures   int
	nextOK     time.Time
	lastResult string
}

func newDialer(self string) *dialer {
	return &dialer{
		self:           self,
		state:          make(map[string]*dialState),
		initialBackoff: 1 * time.Second,
		maxBackoff:     5 * time.Minute,
	}
}

// add registers a candidate. Returns true if it was new.
func (d *dialer) add(addr string) bool {
	at := strings.IndexByte(addr, '@')
	if at <= 0 {
		return false
	}
	if addr[:at] == d.self {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.state[addr]; ok {
		return false
	}
	d.state[addr] = &dialState{}
	d.order = append(d.order, addr)
	return true
}

func (d *dialer) size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.order)
}

// next returns the next candidate that's past its backoff window. Returns
// "" if every candidate is currently in cooldown.
func (d *dialer) next() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.order) == 0 {
		return ""
	}
	now := time.Now()
	for tries := 0; tries < len(d.order); tries++ {
		if d.cursor >= len(d.order) {
			d.cursor = 0
		}
		addr := d.order[d.cursor]
		d.cursor++
		st, ok := d.state[addr]
		if !ok {
			continue
		}
		if now.Before(st.nextOK) {
			continue
		}
		// Pre-emptively push the next attempt out by a tiny amount so two
		// workers don't pick the same address at once before the first one
		// records a result.
		st.nextOK = now.Add(500 * time.Millisecond)
		return addr
	}
	return ""
}

// recordResult notes the outcome of a dial attempt. err==nil ⇒ success.
func (d *dialer) recordResult(addr string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	st, ok := d.state[addr]
	if !ok {
		return
	}
	if err == nil {
		st.failures = 0
		st.nextOK = time.Time{}
		st.lastResult = "ok"
		return
	}
	st.failures++
	wait := d.initialBackoff << uint(min(st.failures-1, 10))
	if wait > d.maxBackoff || wait <= 0 {
		wait = d.maxBackoff
	}
	st.nextOK = time.Now().Add(wait)
	emsg := err.Error()
	if len(emsg) > 80 {
		emsg = emsg[:80]
	}
	st.lastResult = emsg
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// dialCycle keeps trying ready candidates until we hit `want` outbound
// peers, then idles. Multiple instances share one dialer.
func dialCycle(ctx context.Context, sw *p2p.Switch, d *dialer, want int, logger cmtlog.Logger) {
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
		addr := d.next()
		if addr == "" {
			// All candidates in cooldown; nap before re-checking.
			time.Sleep(2 * time.Second)
			continue
		}
		na, err := p2p.NewNetAddressString(addr)
		if err != nil {
			d.recordResult(addr, err)
			continue
		}
		if na.ID == sw.NodeInfo().ID() {
			d.recordResult(addr, fmt.Errorf("self"))
			continue
		}
		err = sw.DialPeerWithAddress(na)
		d.recordResult(addr, err)
		if err != nil {
			logger.Debug("dial failed", "peer", na.ID, "err", err)
		} else {
			logger.Info("connected", "peer", na.ID)
		}
	}
}
