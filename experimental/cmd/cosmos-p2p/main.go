// cosmos-p2p connects to a Cosmos Hub full node over the CometBFT P2P
// protocol, performs the secret handshake + NodeInfo exchange, and uses the
// block-sync reactor on channel 0x40 to ask for a recent block.
//
// Steps:
//  1. load/generate an Ed25519 node key
//  2. pick a candidate peer from a Polkachu-style addrbook.json
//  3. build a Switch over a MultiplexTransport with our minimal block-sync
//     reactor, dial the peer
//  4. wait for StatusResponse, then BlockResponse, print and exit
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
	"github.com/cometbft/cometbft/types"
	"github.com/cometbft/cometbft/version"
	"github.com/cosmos/gogoproto/proto"

	"github.com/zrbecker/cosmos-p2p/internal/blocksync"
	"github.com/zrbecker/cosmos-p2p/internal/helpers/addrbook"
)

func main() {
	var (
		chainID      = flag.String("chain-id", "cosmoshub-4", "expected chain ID")
		addrBookPath = flag.String("addrbook", "data/polkachu_cosmoshub.json", "Polkachu-style addrbook.json")
		nodeKeyPath  = flag.String("node-key", "data/node_key.json", "node key file path")
		peerAddr     = flag.String("peer", "", "explicit peer address (nodeID@host:port); overrides addrbook")
		listen       = flag.String("listen", "tcp://0.0.0.0:0", "p2p listen URL (we don't really serve)")
		moniker      = flag.String("moniker", "cosmos-p2p-explorer", "self-reported moniker")
		timeout      = flag.Duration("timeout", 45*time.Second, "wall-clock budget for the whole exchange")
		attempts     = flag.Int("attempts", 8, "max peer addresses to try from the addrbook")
		offset       = flag.Int64("offset", 100, "request a block this many heights below the peer's tip")
		debug        = flag.Bool("debug", false, "verbose p2p logging")
		outDir       = flag.String("out-dir", "data", "directory to write block-<height>.json into")
	)
	flag.Parse()

	logger := cmtlog.NewTMLogger(cmtlog.NewSyncWriter(os.Stderr))
	if *debug {
		logger = cmtlog.NewFilter(logger, cmtlog.AllowDebug())
	} else {
		logger = cmtlog.NewFilter(logger, cmtlog.AllowInfo())
	}

	if err := os.MkdirAll(filepath.Dir(*nodeKeyPath), 0o700); err != nil {
		log.Fatalf("mkdir node-key dir: %v", err)
	}
	nodeKey, err := p2p.LoadOrGenNodeKey(*nodeKeyPath)
	if err != nil {
		log.Fatalf("load/gen node key: %v", err)
	}
	logger.Info("node identity", "id", nodeKey.ID(), "path", *nodeKeyPath)

	candidates, err := pickCandidates(*peerAddr, *addrBookPath, *attempts)
	if err != nil {
		log.Fatal(err)
	}
	if len(candidates) == 0 {
		log.Fatalf("no candidate peers (try refreshing %s from snapshots.polkachu.com)", *addrBookPath)
	}
	logger.Info("candidate peers", "count", len(candidates))

	listenAddr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), *listen))
	if err != nil {
		log.Fatalf("parse listen addr: %v", err)
	}

	nodeInfo := p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(version.P2PProtocol, version.BlockProtocol, 0),
		DefaultNodeID:   nodeKey.ID(),
		ListenAddr:      listenAddr.DialString(),
		Network:         *chainID,
		Version:         version.TMCoreSemVer,
		Channels:        []byte{blocksync.Channel},
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
	p2pConfig.HandshakeTimeout = 10 * time.Second
	p2pConfig.DialTimeout = 10 * time.Second
	mConfig := conn.DefaultMConnConfig()

	transport := p2p.NewMultiplexTransport(nodeInfo, *nodeKey, mConfig)
	if err := transport.Listen(*listenAddr); err != nil {
		log.Fatalf("transport.Listen %s: %v", listenAddr, err)
	}

	reactor := blocksync.NewReactor(logger.With("module", "blocksync"))
	reactor.HeightOffset = *offset

	sw := p2p.NewSwitch(p2pConfig, transport)
	sw.SetLogger(logger.With("module", "p2p"))
	sw.SetNodeKey(nodeKey)
	sw.SetNodeInfo(nodeInfo)
	sw.AddReactor("BLOCKSYNC", reactor)

	if err := sw.Start(); err != nil {
		log.Fatalf("switch.Start: %v", err)
	}
	defer func() { _ = sw.Stop() }()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("interrupt; shutting down")
		cancel()
	}()

	dialed := dial(sw, logger, candidates)
	if dialed == "" {
		log.Fatalf("could not connect to any candidate peer (tried %d)", len(candidates))
	}

	gotStatus := false
	gotBlock := false
	for !(gotStatus && gotBlock) {
		select {
		case res := <-reactor.Done:
			printResult(res)
			if res.Status != nil {
				gotStatus = true
			}
			if res.Block != nil {
				gotBlock = true
				if err := dumpBlock(*outDir, res.Block); err != nil {
					logger.Error("dump block", "err", err)
				}
			}
			if res.NoBlock != nil {
				gotBlock = true
			}
		case <-ctx.Done():
			log.Fatalf("timed out: gotStatus=%v gotBlock=%v: %v", gotStatus, gotBlock, ctx.Err())
		}
	}
}

// WireSizes breaks the block's protobuf-marshaled size down by sub-message,
// so we can answer "how big is a block on the wire?" concretely.
type WireSizes struct {
	Total       int   `json:"total"`
	Header      int   `json:"header"`
	Data        int   `json:"data"` // == sum(tx wire sizes) + a few framing bytes
	TxBytesSum  int   `json:"tx_bytes_sum"`
	Evidence    int   `json:"evidence"`
	LastCommit  int   `json:"last_commit"`
	NumSigs     int   `json:"num_signatures"`
	AvgSigSize  int   `json:"avg_signature_size"`
	NumTxs      int   `json:"num_txs"`
	AvgTxSize   int   `json:"avg_tx_size"`
}

func measureWireSizes(b *types.Block) (WireSizes, error) {
	var w WireSizes
	bp, err := b.ToProto()
	if err != nil {
		return w, fmt.Errorf("Block.ToProto: %w", err)
	}
	full, err := proto.Marshal(bp)
	if err != nil {
		return w, err
	}
	w.Total = len(full)
	if hb, err := proto.Marshal(&bp.Header); err == nil {
		w.Header = len(hb)
	}
	if db, err := proto.Marshal(&bp.Data); err == nil {
		w.Data = len(db)
	}
	if eb, err := proto.Marshal(&bp.Evidence); err == nil {
		w.Evidence = len(eb)
	}
	if cb, err := proto.Marshal(bp.LastCommit); err == nil {
		w.LastCommit = len(cb)
	}
	for _, tx := range b.Txs {
		w.TxBytesSum += len(tx)
	}
	w.NumTxs = len(b.Txs)
	if w.NumTxs > 0 {
		w.AvgTxSize = w.TxBytesSum / w.NumTxs
	}
	w.NumSigs = len(b.LastCommit.Signatures)
	if w.NumSigs > 0 {
		// average wire size of one CommitSig (proto-marshaled, includes outer field)
		var sigSum int
		for i := range bp.LastCommit.Signatures {
			sb, err := proto.Marshal(&bp.LastCommit.Signatures[i])
			if err == nil {
				sigSum += len(sb)
			}
		}
		w.AvgSigSize = sigSum / w.NumSigs
	}
	return w, nil
}

// dumpBlock writes the decoded block as JSON to <dir>/block-<height>.json.
// We attach a few derived convenience fields (tx_hashes, computed block hash,
// wire-size breakdown) alongside the raw cometbft Block struct.
func dumpBlock(dir string, b *types.Block) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	type out struct {
		Height      int64        `json:"height"`
		Hash        string       `json:"hash"`
		TimeRFC3339 string       `json:"time"`
		ChainID     string       `json:"chain_id"`
		Proposer    string       `json:"proposer_address"`
		NumTxs      int          `json:"num_txs"`
		TxHashes    []string     `json:"tx_hashes"`
		WireSizes   WireSizes    `json:"wire_sizes"`
		Block       *types.Block `json:"block"`
	}
	hashes := make([]string, len(b.Txs))
	for i, tx := range b.Txs {
		hashes[i] = strings.ToUpper(fmt.Sprintf("%X", tx.Hash()))
	}
	ws, err := measureWireSizes(b)
	if err != nil {
		return err
	}
	o := out{
		Height:      b.Height,
		Hash:        strings.ToUpper(fmt.Sprintf("%X", b.Hash())),
		TimeRFC3339: b.Time.UTC().Format(time.RFC3339Nano),
		ChainID:     b.ChainID,
		Proposer:    strings.ToUpper(fmt.Sprintf("%X", b.ProposerAddress)),
		NumTxs:      len(b.Txs),
		TxHashes:    hashes,
		WireSizes:   ws,
		Block:       b,
	}
	path := filepath.Join(dir, fmt.Sprintf("block-%d.json", b.Height))
	buf, err := json.MarshalIndent(o, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		return err
	}
	fmt.Printf("[wrote]    %s  (%d bytes JSON)\n", path, len(buf))
	fmt.Printf("[wire]     total=%d  header=%d  data=%d (txs_sum=%d)  evidence=%d  last_commit=%d (%d sigs × ~%dB)\n",
		ws.Total, ws.Header, ws.Data, ws.TxBytesSum, ws.Evidence,
		ws.LastCommit, ws.NumSigs, ws.AvgSigSize)
	return nil
}

func pickCandidates(explicit, path string, max int) ([]string, error) {
	if explicit != "" {
		return []string{explicit}, nil
	}
	items, err := addrbook.Load(path)
	if err != nil {
		return nil, fmt.Errorf("load addrbook %s: %w", path, err)
	}
	return addrbook.FreshTop(items, max), nil
}

func dial(sw *p2p.Switch, logger cmtlog.Logger, candidates []string) string {
	for _, p := range candidates {
		na, err := p2p.NewNetAddressString(p)
		if err != nil {
			logger.Info("skip peer (parse)", "peer", p, "err", err)
			continue
		}
		logger.Info("dialing", "peer", na.String())
		if err := sw.DialPeerWithAddress(na); err != nil {
			logger.Info("dial failed", "peer", na.String(), "err", err)
			continue
		}
		return na.String()
	}
	return ""
}

func printResult(r blocksync.Result) {
	switch {
	case r.Status != nil:
		fmt.Printf("[status]   peer=%s  base=%d  height=%d\n",
			short(r.PeerID), r.Status.Base, r.Status.Height)
	case r.Block != nil:
		b := r.Block
		fmt.Printf("[block]    peer=%s  height=%d  hash=%X\n           time=%s  txs=%d  proposer=%X\n",
			short(r.PeerID), b.Height, b.Hash(),
			b.Time.UTC().Format(time.RFC3339), len(b.Txs), b.ProposerAddress)
	case r.NoBlock != nil:
		fmt.Printf("[no-block] peer=%s  height=%d (peer pruned this height)\n",
			short(r.PeerID), r.NoBlock.Height)
	}
}

func short(id string) string {
	if len(id) > 10 {
		return id[:10]
	}
	return id
}
