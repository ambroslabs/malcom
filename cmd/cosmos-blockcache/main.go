// cosmos-blockcache runs as a long-lived peer on cosmoshub-4: it connects
// to a few known good full nodes, maintains the latest 1000 contiguous
// blocks in a local cache (raw cmtproto.Block bytes on disk), and serves
// inbound BlockRequest / StatusRequest from any peer that asks.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
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

	"github.com/zrbecker/cosmos-p2p/internal/blockcache"
	"github.com/zrbecker/cosmos-p2p/internal/observer"
	"github.com/zrbecker/cosmos-p2p/internal/pex"
)

func main() {
	var (
		chainID      = flag.String("chain-id", "cosmoshub-4", "expected chain ID")
		nodeKeyPath  = flag.String("node-key", "data/node_key.json", "node key file path")
		listen       = flag.String("listen", "tcp://0.0.0.0:26656", "p2p bind address (what we listen on locally)")
		externalAddr = flag.String("external-addr", "", "publicly-dialable host:port to advertise in NodeInfo (e.g. 64.23.187.105:26656). Empty = use the bind address (only OK on a public-IP'd box with the bind addr already public).")
		moniker      = flag.String("moniker", "cosmos-p2p-blockcache", "self-reported moniker")
		blocksDir    = flag.String("blocks-dir", "data/blocks", "where to persist raw block bytes")
		capacity     = flag.Int("capacity", 1000, "sliding-window size in blocks")
		cumulativeDB    = flag.String("peers", "data/peers-cumulative.json", "load good peers from this file")
		discoveriesPath = flag.String("discoveries", "data/peers-discovered.json", "append every connecting peer (in or out) here, deduped by node_id")
		maxPeers     = flag.Int("max-peers", 8, "how many concurrent outbound peer connections to maintain")
		maxInbound   = flag.Int("max-inbound", 64, "max accepted inbound peer connections")
		moniker2     = flag.String("upstream-min-range", "", "(unused; placeholder)")
		debug        = flag.Bool("debug", false, "verbose p2p logging")
	)
	_ = moniker2
	flag.Parse()

	logger := cmtlog.NewTMLogger(cmtlog.NewSyncWriter(os.Stderr))
	if *debug {
		logger = cmtlog.NewFilter(logger, cmtlog.AllowDebug())
	} else {
		logger = cmtlog.NewFilter(logger, cmtlog.AllowError(),
			cmtlog.AllowInfoWith("module", "blockcache"))
	}

	if err := os.MkdirAll(filepath.Dir(*nodeKeyPath), 0o700); err != nil {
		log.Fatalf("mkdir: %v", err)
	}
	nodeKey, err := p2p.LoadOrGenNodeKey(*nodeKeyPath)
	if err != nil {
		log.Fatalf("node key: %v", err)
	}

	cache, err := blockcache.New(*blocksDir, *capacity)
	if err != nil {
		log.Fatalf("cache init: %v", err)
	}
	if s := cache.Stats(); s.Count > 0 {
		logger.Info("loaded cache from disk", "blocks", s.Count, "base", s.Base, "tip", s.Tip)
	}

	listenAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), *listen))
	if err != nil {
		log.Fatalf("listen addr: %v", err)
	}

	// What we ADVERTISE to dialing peers (and what they share over PEX) is
	// distinct from the bind address. If you're on a public-IP'd box and the
	// bind addr is "0.0.0.0:26656", peers will get told to dial 0.0.0.0,
	// which fails. Set -external-addr to the publicly-dialable host:port.
	advertisedAddr := listenAddr.DialString()
	if *externalAddr != "" {
		advertisedAddr = *externalAddr
		// Validate it can round-trip through NewNetAddressString.
		if _, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), "tcp://"+*externalAddr)); err != nil {
			log.Fatalf("invalid -external-addr %q: %v", *externalAddr, err)
		}
	}

	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      advertisedAddr,
		Network:         *chainID,
		Version:         version.TMCoreSemVer,
		Channels: []byte{
			pex.Channel,
			observer.StateChannel, observer.DataChannel,
			observer.VoteChannel, observer.VoteSetBitsChannel,
			observer.MempoolChannel, observer.EvidenceChannel,
			blockcache.Channel,
		},
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
	p2pConfig.MaxNumOutboundPeers = *maxPeers
	p2pConfig.MaxNumInboundPeers = *maxInbound
	mConfig := conn.DefaultMConnConfig()

	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, mConfig)
	if err := transport.Listen(*listenAddr); err != nil {
		log.Fatalf("transport.Listen %s: %v", listenAddr, err)
	}

	reactor := blockcache.NewReactor(cache, logger.With("module", "blockcache"))

	pexR := pex.NewReactor(logger.With("module", "pex"))
	if *externalAddr != "" {
		// Tell PEX what to advertise. We point peers at our public address
		// so they can dial back; we never relay other peers' addresses.
		if naSelf, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), "tcp://"+*externalAddr)); err == nil {
			pexR.SetSelf(*naSelf)
		}
	}

	obsR := observer.NewReactor(logger.With("module", "observer"))
	obsR.SetTipFn(func() int64 {
		_, tip := cache.Range()
		return tip
	})

	sw := p2p.NewSwitch(p2pConfig, transport)
	sw.SetLogger(logger.With("module", "p2p"))
	sw.SetNodeKey(nodeKey)
	sw.SetNodeInfo(nodeInfo)
	sw.AddReactor("PEX", pexR)
	sw.AddReactor("OBSERVER", obsR)
	sw.AddReactor("BLOCKCACHE", reactor)

	if err := sw.Start(); err != nil {
		log.Fatalf("switch.Start: %v", err)
	}
	defer func() { _ = sw.Stop() }()

	// Pull every known cosmoshub-4 peer; the dial loop cycles through them
	// and only the responsive ones will end up connected. Bound is set high
	// so we don't artificially restrict the outbound pool.
	dialer := NewDialer()
	candidates := pickPeers(*cumulativeDB, 4096)
	dialer.AddAll(candidates)
	if dialer.Size() == 0 {
		log.Fatalf("no candidate peers in %s", *cumulativeDB)
	}
	logger.Info("connecting", "initial_candidates", dialer.Size(), "want_outbound", *maxPeers, "self_id", nodeKey.ID())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("interrupt; shutting down")
		cancel()
	}()

	// Start dialing in the background; keep adding peers until we hit the cap.
	go dialUntilFull(ctx, sw, dialer, *maxPeers, logger.With("module", "blockcache"))

	// Feed PEX-discovered peers into the dialer.
	go consumePEX(ctx, pexR, dialer, logger.With("module", "blockcache"))

	// Periodically re-issue PexRequest so seeds give us fresh batches.
	go rePEXLoop(ctx, sw, logger.With("module", "blockcache"))

	// Drive the sync loop.
	go reactor.SyncLoop(ctx)

	// Heartbeat: refresh the consensus state-claim we send to peers as our
	// cache advances, so they keep gossiping votes/proposals/blockparts.
	stopHeartbeat := make(chan struct{})
	defer close(stopHeartbeat)
	go obsR.Heartbeat(stopHeartbeat)

	// Periodic discoveries persist: merge live peer activity into the on-disk
	// list, deduplicated by node ID, so subsequent runs can dial them.
	go discoveriesLoop(ctx, reactor, *discoveriesPath)

	// Periodic status print + per-second rates.
	startTime := time.Now()
	prevC := reactor.Counters()
	prevObs := obsR.Snapshot()
	prevAt := startTime
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s := cache.Stats()
			c := reactor.Counters()
			elapsed := time.Since(startTime).Seconds()
			fmt.Printf("[exit] cache: count=%d/%d base=%d tip=%d bytes=%dKB\n",
				s.Count, s.Capacity, s.Base, s.Tip, s.BytesRAM/1024)
			fmt.Printf("       totals: fetched=%d served=%d missed=%d status_served=%d  (over %.1fs)\n",
				c.Fetched, c.ServedBlocks, c.MissedBlocks, c.StatusServed, elapsed)
			if elapsed > 0 {
				fmt.Printf("       rates:  served=%.3f/s status_served=%.3f/s fetched=%.3f/s\n",
					float64(c.ServedBlocks)/elapsed,
					float64(c.StatusServed)/elapsed,
					float64(c.Fetched)/elapsed)
				fmt.Printf("       per_min: served=%.1f/min status_served=%.1f/min\n",
					60*float64(c.ServedBlocks)/elapsed,
					60*float64(c.StatusServed)/elapsed)
			}
			dumpPeerActivity(reactor.PeerActivity())
			return
		case now := <-t.C:
			s := cache.Stats()
			c := reactor.Counters()
			pexServed := pexR.ServedCount()
			obs := obsR.Snapshot()
			dt := now.Sub(prevAt).Seconds()
			fmt.Printf("[stat] cache: %d/%d tip=%d bytes=%dKB  fetched=%d served=%d (Δ%d) status_served=%d pex_served=%d  peers=%d (out=%d in=%d)\n",
				s.Count, s.Capacity, s.Tip, s.BytesRAM/1024,
				c.Fetched, c.ServedBlocks, c.ServedBlocks-prevC.ServedBlocks,
				c.StatusServed, pexServed,
				c.PeerCount, c.OutboundPeers, c.InboundPeers)
			fmt.Printf("[gossip] txs=%d (uniq=%d, Δ%d %.1f/s) batches=%d  votes=%d (Δ%d %.0f/s)  block_parts=%d (Δ%d)  proposals=%d (Δ%d)  hasVote=%d  newRoundStep=%d  evidence=%d\n",
				obs.Txs, obs.UniqueTxs, obs.Txs-prevObs.Txs, float64(obs.Txs-prevObs.Txs)/dt,
				obs.TxBatches,
				obs.Votes, obs.Votes-prevObs.Votes, float64(obs.Votes-prevObs.Votes)/dt,
				obs.BlockParts, obs.BlockParts-prevObs.BlockParts,
				obs.Proposals, obs.Proposals-prevObs.Proposals,
				obs.HasVotes, obs.NewRoundSteps, obs.EvidenceItems)
			fmt.Printf("[relay]  txs_out=%d (Δ%d) votes_out=%d (Δ%d) block_parts_out=%d (Δ%d) proposals_out=%d (Δ%d)\n",
				obs.TxsRelayed, obs.TxsRelayed-prevObs.TxsRelayed,
				obs.VotesRelayed, obs.VotesRelayed-prevObs.VotesRelayed,
				obs.BlockPartsRelayed, obs.BlockPartsRelayed-prevObs.BlockPartsRelayed,
				obs.ProposalsRelayed, obs.ProposalsRelayed-prevObs.ProposalsRelayed)
			prevC, prevObs, prevAt = c, obs, now
		}
	}
}

// dumpPeerActivity writes a per-peer JSON breakdown to data/serves-<ts>.json
// and prints the top 10 requesters (by blocks served + status served) so the
// operator can see who actually pulled from us.
func dumpPeerActivity(acts []blockcache.PeerActivity) {
	ts := time.Now().UTC().Format("20060102T150405Z")
	out := filepath.Join("data", fmt.Sprintf("serves-%s.json", ts))
	f, err := os.Create(out)
	if err == nil {
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		_ = enc.Encode(acts)
		_ = f.Close()
		fmt.Printf("       wrote per-peer activity → %s\n", out)
	}
	// Sort by total inbound activity desc.
	sort.Slice(acts, func(i, j int) bool {
		ai := acts[i].BlocksServed + acts[i].StatusReqsRecv + acts[i].NoBlockSent
		aj := acts[j].BlocksServed + acts[j].StatusReqsRecv + acts[j].NoBlockSent
		return ai > aj
	})
	any := false
	for _, p := range acts {
		if p.BlocksServed+p.StatusReqsRecv+p.NoBlockSent == 0 {
			continue
		}
		if !any {
			fmt.Printf("       --- peers that pulled data from us ---\n")
			fmt.Printf("       %-3s %-12s %-21s %-22s %-10s %8s %8s %8s %8s %12s\n",
				"dir", "node_id", "remote_addr", "moniker", "ver", "blk_req", "blk_srv", "noblk", "stat_req", "bytes_out")
			any = true
		}
		fmt.Printf("       %-3s %-12s %-21s %-22s %-10s %8d %8d %8d %8d %12d\n",
			p.Direction, short(p.NodeID), trunc(p.RemoteAddr, 21),
			trunc(p.Moniker, 22), trunc(p.NodeVersion, 10),
			p.BlockReqsRecv, p.BlocksServed, p.NoBlockSent, p.StatusReqsRecv, p.BytesServedOut)
	}
	if !any {
		fmt.Printf("       (no peer made any inbound request)\n")
	}

	// Always show inbound peer count even if they didn't request anything,
	// so we know whether anyone connected to us at all.
	var inN, outN int
	for _, p := range acts {
		if p.Direction == "in" {
			inN++
		} else {
			outN++
		}
	}
	fmt.Printf("       peer mix at exit: outbound=%d  inbound=%d\n", outN, inN)
}

func short(s string) string {
	if len(s) > 10 {
		return s[:10]
	}
	return s
}
func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// discoveryRecord is the persisted form per peer-ID.
type discoveryRecord struct {
	NodeID         string    `json:"node_id"`
	AdvertisedAddr string    `json:"advertised_addr,omitempty"`
	LastRemoteAddr string    `json:"last_remote_addr,omitempty"`
	Moniker        string    `json:"moniker,omitempty"`
	NodeVersion    string    `json:"node_version,omitempty"`
	Network        string    `json:"network,omitempty"`
	BaseHeight     int64     `json:"base_height,omitempty"`
	LatestHeight   int64     `json:"latest_height,omitempty"`
	SeenInbound    bool      `json:"seen_inbound"`
	SeenOutbound   bool      `json:"seen_outbound"`
	FirstSeen      time.Time `json:"first_seen,omitempty"`
	LastSeen       time.Time `json:"last_seen,omitempty"`
}

func loadDiscoveries(path string) map[string]*discoveryRecord {
	out := map[string]*discoveryRecord{}
	f, err := os.Open(path)
	if err != nil {
		return out
	}
	defer f.Close()
	var arr []discoveryRecord
	if err := json.NewDecoder(f).Decode(&arr); err != nil {
		return out
	}
	for i := range arr {
		r := arr[i]
		out[r.NodeID] = &r
	}
	return out
}

func writeDiscoveries(path string, m map[string]*discoveryRecord) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	arr := make([]*discoveryRecord, 0, len(m))
	for _, r := range m {
		arr = append(arr, r)
	}
	sort.Slice(arr, func(i, j int) bool {
		// Inbound-only first, then by deepest base, then by node_id.
		ai, aj := arr[i], arr[j]
		ainOnly := ai.SeenInbound && !ai.SeenOutbound
		ajinOnly := aj.SeenInbound && !aj.SeenOutbound
		if ainOnly != ajinOnly {
			return ainOnly
		}
		if ai.BaseHeight != aj.BaseHeight {
			return ai.BaseHeight < aj.BaseHeight
		}
		return ai.NodeID < aj.NodeID
	})
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(arr); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func discoveriesLoop(ctx context.Context, reactor *blockcache.Reactor, path string) {
	disc := loadDiscoveries(path)
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	flush := func() {
		acts := reactor.PeerActivity()
		now := time.Now()
		for _, a := range acts {
			r, ok := disc[a.NodeID]
			if !ok {
				r = &discoveryRecord{NodeID: a.NodeID, FirstSeen: now}
				disc[a.NodeID] = r
			}
			if a.Direction == "in" {
				r.SeenInbound = true
			} else {
				r.SeenOutbound = true
			}
			r.LastSeen = now
			if a.RemoteAddr != "" {
				r.LastRemoteAddr = a.RemoteAddr
			}
			if a.Moniker != "" {
				r.Moniker = a.Moniker
			}
			if a.NodeVersion != "" {
				r.NodeVersion = a.NodeVersion
			}
			if a.Base > 0 {
				r.BaseHeight = a.Base
			}
			if a.Tip > r.LatestHeight {
				r.LatestHeight = a.Tip
			}
		}
		if err := writeDiscoveries(path, disc); err != nil {
			fmt.Fprintf(os.Stderr, "[discoveries] write %s: %v\n", path, err)
		}
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-t.C:
			flush()
		}
	}
}

// Dialer is a thread-safe pool of nodeID@host:port candidates that grows
// over time as PEX gossips new addresses to us. It cycles forever; the dial
// loop just keeps pulling the next candidate.
type Dialer struct {
	mu     sync.Mutex
	seen   map[string]struct{}
	queue  []string
	cursor int
}

func NewDialer() *Dialer {
	return &Dialer{seen: make(map[string]struct{})}
}

// Add registers a candidate. Returns true if it was new.
func (d *Dialer) Add(addr string) bool {
	at := strings.IndexByte(addr, '@')
	if at <= 0 {
		return false
	}
	nodeID := addr[:at]
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[nodeID]; ok {
		return false
	}
	d.seen[nodeID] = struct{}{}
	d.queue = append(d.queue, addr)
	return true
}

// AddAll dedups and returns the count of newly-added addresses.
func (d *Dialer) AddAll(addrs []string) int {
	n := 0
	for _, a := range addrs {
		if d.Add(a) {
			n++
		}
	}
	return n
}

// Next returns the next candidate to try (round-robin), or "" if empty.
func (d *Dialer) Next() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.queue) == 0 {
		return ""
	}
	if d.cursor >= len(d.queue) {
		d.cursor = 0
	}
	addr := d.queue[d.cursor]
	d.cursor++
	return addr
}

// Size reports the current pool size.
func (d *Dialer) Size() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.queue)
}

func dialUntilFull(ctx context.Context, sw *p2p.Switch, dialer *Dialer, want int, logger cmtlog.Logger) {
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
		addr := dialer.Next()
		if addr == "" {
			time.Sleep(5 * time.Second)
			continue
		}
		na, err := p2p.NewNetAddressString(addr)
		if err != nil {
			continue
		}
		if err := sw.DialPeerWithAddress(na); err != nil {
			logger.Debug("dial failed", "peer", na.ID, "err", err)
		} else {
			logger.Info("connected", "peer", na.ID)
		}
	}
}

// consumePEX pulls every PexAddrs batch we receive and feeds new addresses
// into the dialer. This is the missing wire that turns PEX from "we ask but
// drop the answer" into a self-growing peer pool.
func consumePEX(ctx context.Context, pexR *pex.Reactor, dialer *Dialer, logger cmtlog.Logger) {
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
				if ip := net.ParseIP(host); ip != nil && ip.To4() == nil {
					host = "[" + host + "]" // bracket IPv6
				}
				addr := fmt.Sprintf("%s@%s:%d", na.ID, host, na.Port)
				if dialer.Add(addr) {
					added++
				}
			}
			if added > 0 {
				logger.Info("pex grew dialer pool", "new", added, "from", ev.Source[:10], "queue_size", dialer.Size())
			}
		}
	}
}

// rePEXLoop periodically asks every connected peer for fresh addresses.
// Cometbft enforces a min interval ≈ 40 s (defaultEnsurePeersPeriod + 10s)
// per peer; asking more often gets us disconnected with
// ErrReceivedPEXRequestTooSoon. 60 s leaves margin.
func rePEXLoop(ctx context.Context, sw *p2p.Switch, logger cmtlog.Logger) {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			req := &tmp2pproto.PexRequest{}
			for _, p := range sw.Peers().List() {
				p.TrySend(p2p.Envelope{ChannelID: pex.Channel, Message: req})
			}
		}
	}
}

type peerCand struct {
	Addr         string `json:"addr"`
	BaseHeight   int64  `json:"base_height"`
	LatestHeight int64  `json:"latest_height"`
	Network      string `json:"network"`
}

// pickPeers reads the cumulative DB and returns peer addresses we know
// have responded with cosmoshub-4 status, sorted to prefer:
// 1. archival nodes (deepest base_height) — they have the widest serve window
// 2. then most-recent latest_height (live nodes)
func pickPeers(path string, n int) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var all []peerCand
	if err := json.NewDecoder(f).Decode(&all); err != nil {
		return nil
	}
	good := all[:0]
	for _, p := range all {
		if p.LatestHeight > 0 && p.Network == "cosmoshub-4" {
			good = append(good, p)
		}
	}
	sort.Slice(good, func(i, j int) bool {
		// Prefer deeper history; tie-break on higher tip.
		if good[i].BaseHeight != good[j].BaseHeight {
			return good[i].BaseHeight < good[j].BaseHeight
		}
		return good[i].LatestHeight > good[j].LatestHeight
	})
	out := make([]string, 0, len(good))
	for _, p := range good {
		out = append(out, p.Addr)
		if len(out) >= n {
			break
		}
	}
	return out
}
