// cosmos-crawl walks the Cosmos Hub gossip graph: it dials a few seed peers
// from a Polkachu-style addrbook, asks each via PEX (channel 0x00) for more
// peers, and asks each via block-sync (channel 0x40) StatusRequest for its
// (base_height, latest_height). Runs for -duration, then dumps a JSON list
// to data/peers-<timestamp>.json and exits.
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
	"strings"
	"syscall"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	"github.com/cometbft/cometbft/version"

	"github.com/zrbecker/cosmos-p2p/internal/blocksync"
	"github.com/zrbecker/cosmos-p2p/internal/crawler"
	"github.com/zrbecker/cosmos-p2p/internal/peers"
	"github.com/zrbecker/cosmos-p2p/internal/pex"
)

func main() {
	var (
		chainID      = flag.String("chain-id", "cosmoshub-4", "expected chain ID")
		addrBookPath = flag.String("addrbook", "data/polkachu_cosmoshub.json", "Polkachu-style addrbook.json")
		nodeKeyPath  = flag.String("node-key", "data/node_key.json", "node key file path")
		listen       = flag.String("listen", "tcp://0.0.0.0:0", "p2p listen URL")
		moniker      = flag.String("moniker", "cosmos-p2p-crawler", "self-reported moniker")
		duration     = flag.Duration("duration", 60*time.Second, "wall-clock budget for the crawl")
		parallel     = flag.Int("parallel", 32, "max concurrent dials")
		seedCount     = flag.Int("seeds", 1024, "initial seed addresses pulled from the addrbook (when -all-addrbook=false)")
		useAll        = flag.Bool("all-addrbook", true, "seed from every entry in the addrbook (~1500 peers) rather than just FreshTop(seeds)")
		extraSeedsCSV = flag.String("extra-seeds", "", "comma-separated nodeID@host:port to seed in addition to the addrbook")
		cumulativeDB  = flag.String("cumulative", "data/peers-cumulative.json", "load+update this file across runs (set empty to disable)")
		archiveBase   = flag.Int64("archive-base", 6_000_000, "report peers as 'archive' if their base_height is below this")
		outDir        = flag.String("out-dir", "data", "directory to write peers-<ts>.json into")
		debug         = flag.Bool("debug", false, "verbose logging")
	)
	flag.Parse()

	logger := cmtlog.NewTMLogger(cmtlog.NewSyncWriter(os.Stderr))
	switch {
	case *debug:
		logger = cmtlog.NewFilter(logger, cmtlog.AllowDebug())
	default:
		logger = cmtlog.NewFilter(logger, cmtlog.AllowError(),
			cmtlog.AllowInfoWith("module", "crawler"),
			cmtlog.AllowInfoWith("module", "pex"))
	}

	if err := os.MkdirAll(filepath.Dir(*nodeKeyPath), 0o700); err != nil {
		log.Fatalf("mkdir: %v", err)
	}
	nodeKey, err := p2p.LoadOrGenNodeKey(*nodeKeyPath)
	if err != nil {
		log.Fatalf("node key: %v", err)
	}

	items, err := peers.Load(*addrBookPath)
	if err != nil {
		log.Fatalf("load addrbook: %v", err)
	}
	var seedAddrs []string
	if *useAll {
		seedAddrs = peers.All(items)
	} else {
		seedAddrs = peers.FreshTop(items, *seedCount)
	}
	// Always prepend the chain-registry seed list — most cosmoshub full
	// nodes run with `pex = false`, but seed-mode nodes will hand us address
	// batches. Without these, the gossip graph never expands.
	seedAddrs = append(chainRegistrySeeds(), seedAddrs...)
	for _, s := range strings.Split(*extraSeedsCSV, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			seedAddrs = append([]string{s}, seedAddrs...)
		}
	}
	if len(seedAddrs) == 0 {
		log.Fatalf("no seed addresses in %s", *addrBookPath)
	}

	// Load any existing cumulative DB so prior runs aren't discarded. Each
	// known peer's address is added to the seed queue too, so we re-probe
	// stale entries and refresh their (base, height).
	var prior []crawler.PeerRecord
	if *cumulativeDB != "" {
		if pr, err := loadCumulative(*cumulativeDB); err != nil {
			log.Printf("warn: load cumulative %s: %v", *cumulativeDB, err)
		} else {
			prior = pr
			for _, r := range prior {
				if r.Addr != "" {
					seedAddrs = append(seedAddrs, r.Addr)
				}
			}
		}
	}

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
		Channels:        []byte{pex.Channel, blocksync.Channel},
		Moniker:         *moniker,
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    "off",
			RPCAddress: "",
		},
	}
	if err := nodeInfo.Validate(); err != nil {
		log.Fatalf("nodeInfo invalid: %v", err)
	}

	p2pConfig := cfg.DefaultP2PConfig()
	p2pConfig.AllowDuplicateIP = true
	p2pConfig.HandshakeTimeout = 5 * time.Second
	p2pConfig.DialTimeout = 5 * time.Second
	// Default is 10. We churn through peers fast (drop after status), so let
	// many run in parallel — anything we collide with the cap is just thrown
	// back into the queue as an error.
	p2pConfig.MaxNumOutboundPeers = *parallel * 4
	mConfig := conn.DefaultMConnConfig()

	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, mConfig)
	if err := transport.Listen(*listenAddr); err != nil {
		log.Fatalf("transport.Listen %s: %v", listenAddr, err)
	}

	pexR := pex.NewReactor(logger.With("module", "pex"))
	bsR := blocksync.NewReactor(logger.With("module", "blocksync"))
	bsR.StatusOnly = true

	sw := p2p.NewSwitch(p2pConfig, transport)
	sw.SetLogger(logger.With("module", "p2p"))
	sw.SetNodeKey(nodeKey)
	sw.SetNodeInfo(nodeInfo)
	sw.AddReactor("PEX", pexR)
	sw.AddReactor("BLOCKSYNC", bsR)

	if err := sw.Start(); err != nil {
		log.Fatalf("switch.Start: %v", err)
	}
	defer func() { _ = sw.Stop() }()

	cr := crawler.New(sw, nodeKey.ID(), pexR, bsR, logger.With("module", "crawler"), *parallel)
	cr.AlwaysRetry = chainRegistrySeeds()
	if len(prior) > 0 {
		cr.Preload(prior)
	}
	cr.Seed(seedAddrs)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	fmt.Printf("[crawl] node_id=%s  seeds=%d  duration=%s  parallel=%d\n",
		nodeKey.ID(), len(seedAddrs), *duration, *parallel)
	cr.Run(ctx)

	snap := cr.Snapshot()
	ts := time.Now().UTC().Format("20060102T150405Z")
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatalf("mkdir out: %v", err)
	}
	out := filepath.Join(*outDir, fmt.Sprintf("peers-%s.json", ts))
	if err := writeJSON(out, snap); err != nil {
		log.Fatal(err)
	}

	// Update the cumulative DB by merging this snapshot into the prior set.
	if *cumulativeDB != "" {
		merged := mergePeers(prior, snap)
		if err := writeJSON(*cumulativeDB, merged); err != nil {
			log.Printf("warn: write cumulative: %v", err)
		}
	}

	summarize(snap, out, *archiveBase)
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

func loadCumulative(path string) ([]crawler.PeerRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	var out []crawler.PeerRecord
	if err := json.NewDecoder(f).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// mergePeers takes the prior cumulative records and the latest snapshot and
// returns a merged list keyed by node ID. For each peer we keep the lowest
// observed base_height (deepest history floor seen) and the highest
// latest_height. Other metadata is taken from the most recent observation.
func mergePeers(prior, latest []crawler.PeerRecord) []crawler.PeerRecord {
	by := map[string]*crawler.PeerRecord{}
	apply := func(r crawler.PeerRecord) {
		cur, ok := by[r.NodeID]
		if !ok {
			cp := r
			by[r.NodeID] = &cp
			return
		}
		if r.BaseHeight > 0 && (cur.BaseHeight == 0 || r.BaseHeight < cur.BaseHeight) {
			cur.BaseHeight = r.BaseHeight
		}
		if r.LatestHeight > cur.LatestHeight {
			cur.LatestHeight = r.LatestHeight
		}
		if r.Connected {
			cur.Connected = true
		}
		if r.Moniker != "" {
			cur.Moniker = r.Moniker
		}
		if r.Network != "" {
			cur.Network = r.Network
		}
		if r.Version != "" {
			cur.Version = r.Version
		}
		if !r.GotStatusAt.IsZero() && r.GotStatusAt.After(cur.GotStatusAt) {
			cur.GotStatusAt = r.GotStatusAt
		}
		if !r.DialedAt.IsZero() && r.DialedAt.After(cur.DialedAt) {
			cur.DialedAt = r.DialedAt
		}
		// Only keep DialErr if we never got a status — once a peer has
		// answered, the previous error is stale.
		if cur.LatestHeight == 0 && r.DialErr != "" {
			cur.DialErr = r.DialErr
		} else if cur.LatestHeight > 0 {
			cur.DialErr = ""
		}
		if r.Source != "" && cur.Source == "" {
			cur.Source = r.Source
		}
	}
	for _, r := range prior {
		apply(r)
	}
	for _, r := range latest {
		apply(r)
	}
	out := make([]crawler.PeerRecord, 0, len(by))
	for _, r := range by {
		out = append(out, *r)
	}
	// Same sort as Snapshot: deepest history first.
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			ai, aj := out[i].BaseHeight, out[j].BaseHeight
			swap := false
			switch {
			case ai == 0 && aj != 0:
				swap = true
			case ai != 0 && aj != 0 && ai > aj:
				swap = true
			}
			if swap {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

// chainRegistrySeeds is the seeds list from
// https://github.com/cosmos/chain-registry/blob/master/cosmoshub/chain.json
// Snapshotted because seeds rotate slowly and we want this binary to work
// without a live HTTP fetch on startup.
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

func summarize(snap []crawler.PeerRecord, outFile string, archiveBase int64) {
	var connected, withStatus int
	var minBase, maxBase int64 = 1 << 62, 0
	var minH, maxH int64 = 1 << 62, 0
	var archive int
	var deepestArchive int64 = 1 << 62
	for _, r := range snap {
		if r.Connected {
			connected++
		}
		if r.LatestHeight > 0 {
			withStatus++
			if r.BaseHeight < minBase {
				minBase = r.BaseHeight
			}
			if r.BaseHeight > maxBase {
				maxBase = r.BaseHeight
			}
			if r.LatestHeight < minH {
				minH = r.LatestHeight
			}
			if r.LatestHeight > maxH {
				maxH = r.LatestHeight
			}
			if r.BaseHeight > 0 && r.BaseHeight < archiveBase {
				archive++
				if r.BaseHeight < deepestArchive {
					deepestArchive = r.BaseHeight
				}
			}
		}
	}
	fmt.Printf("[crawl] done.\n")
	fmt.Printf("        discovered=%d  connected=%d  status=%d  archive(base<%d)=%d\n",
		len(snap), connected, withStatus, archiveBase, archive)
	if withStatus > 0 {
		fmt.Printf("        latest_height range: %d .. %d  (spread %d)\n", minH, maxH, maxH-minH)
		fmt.Printf("        base_height   range: %d .. %d\n", minBase, maxBase)
	}
	if archive > 0 {
		fmt.Printf("        deepest archive base_height: %d\n", deepestArchive)
	}
	fmt.Printf("        wrote %s\n", outFile)
}
