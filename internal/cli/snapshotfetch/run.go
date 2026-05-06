// Package snapshotfetch is the `malcom snapshot fetch` subcommand:
// discover a fetchable cosmoshub state-sync snapshot, download every
// chunk with per-chunk hash verification, and write the result to disk
// in the layout cosmos-bootstrap-gaia / snapshotimport consume.
//
// Output:
//
//	<out>/<height>_<format>/
//	  meta.json           — height, format, chunks, hash, peers, timing
//	  metadata.bin        — raw cosmos-sdk Metadata blob
//	  chunk_NNNNN.bin     — one file per chunk
//	  .complete           — empty marker, written when every chunk verified
package snapshotfetch

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"

	"github.com/zrbecker/cosmos-p2p/internal/snapfetch"
)

// Run is the malcom subcommand entry point. Returns the process exit
// code (0 on success).
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom snapshot fetch", flag.ContinueOnError)
	var (
		chainID      = fs.String("chain-id", "cosmoshub-4", "expected chain ID")
		cumulativeDB = fs.String("cumulative", "data/peers-cumulative.json", "peer DB from cosmos-crawl runs")
		addrBookPath = fs.String("addrbook", "data/polkachu_cosmoshub.json", "Polkachu addrbook fallback")
		nodeKeyPath  = fs.String("node-key", "data/snapfetch_node_key.json", "node key (separate from downloader)")
		listen       = fs.String("listen", "tcp://0.0.0.0:0", "p2p listen URL")
		moniker      = fs.String("moniker", "cosmos-p2p-snapfetch", "self-reported moniker")
		outRoot      = fs.String("out", "/mnt/data/cosmos-archive/cosmoshub-4/snapshots", "snapshot store root")

		discoverFor       = fs.Duration("discover", 25*time.Second, "phase 1: time spent discovering snapshots")
		dialParallel      = fs.Int("dial-parallel", 32, "max concurrent dials during discovery")
		maxCandidates     = fs.Int("max-candidates", 5, "phase 2: probe top-N newest unique snapshots in parallel")
		probeTimeout      = fs.Duration("probe-timeout", 12*time.Second, "phase 2: time to wait for chunk-0 probe replies")
		minGoodPeers      = fs.Int("min-peers", 1, "phase 2: minimum 'good' peers required to accept a candidate")
		perPeerLimit      = fs.Int("per-peer", 2, "phase 3: max in-flight chunks per peer (low to avoid pong-timeouts on the peer side)")
		chunkTimeout      = fs.Duration("chunk-timeout", 45*time.Second, "phase 3: per-chunk wait before re-dispatching")
		maxFetchTime      = fs.Duration("max-fetch", 30*time.Minute, "phase 3: hard cap on full download")
		peerFailLimit     = fs.Int("peer-fails", 3, "phase 3: bench a peer after this many missing/hash-mismatch responses (NOT counting disconnects)")
		peerRedialMax     = fs.Int("peer-redials", 3, "phase 3: max redial attempts when a peer drops the connection")
		peerRedialBackoff = fs.Duration("redial-backoff", 5*time.Second, "phase 3: minimum wait between redial attempts to the same peer")

		extraSeedsCSV     = fs.String("extra-seeds", "", "comma-separated nodeID@host:port seeds in addition to DB")
		targetHeight      = fs.Uint64("target-height", 0, "if non-zero, force this exact snapshot height (else pick best)")
		preferFresh       = fs.Bool("prefer-fresh", false, "rank candidates by newest-height first (peers as tiebreak). Default ranks by peer count first.")
		maxRescans        = fs.Int("max-rescans", 3, "if all peers fail for the chosen snapshot, rescan up to this many times to find a fresh snapshot (or fall back)")
		rescanDiscoverFor = fs.Duration("rescan-discover", 15*time.Second, "shorter discovery duration on rescans (peer DB is already warm)")
		debug             = fs.Bool("debug", false, "verbose logging")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}

	logger := cmtlog.NewTMLogger(cmtlog.NewSyncWriter(os.Stderr))
	if *debug {
		logger = cmtlog.NewFilter(logger, cmtlog.AllowDebug())
	} else {
		logger = cmtlog.NewFilter(logger, cmtlog.AllowError(),
			cmtlog.AllowInfoWith("module", "snapfetch"))
	}

	if err := os.MkdirAll(*outRoot, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir out root: %v\n", err)
		return 1
	}
	fmt.Printf("[snapfetch] out=%s\n", *outRoot)

	cfg := snapfetch.Config{
		ChainID:           *chainID,
		NodeKeyPath:       *nodeKeyPath,
		Listen:            *listen,
		Moniker:           *moniker,
		Cumulative:        *cumulativeDB,
		AddrBook:          *addrBookPath,
		ExtraSeedsCSV:     *extraSeedsCSV,
		DiscoverFor:       *discoverFor,
		DialParallel:      *dialParallel,
		MaxCandidates:     *maxCandidates,
		ProbeTimeout:      *probeTimeout,
		MinGoodPeers:      *minGoodPeers,
		PerPeerLimit:      *perPeerLimit,
		ChunkTimeout:      *chunkTimeout,
		MaxFetchTime:      *maxFetchTime,
		PeerFailLimit:     *peerFailLimit,
		PeerRedialMax:     *peerRedialMax,
		PeerRedialBackoff: *peerRedialBackoff,
		TargetHeight:      *targetHeight,
		PreferFresh:       *preferFresh,
		MaxRescans:        *maxRescans,
		RescanDiscoverFor: *rescanDiscoverFor,
		Logger:            logger,
	}

	rootCtx, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() { <-sigCh; cancelAll() }()

	sink := &diskSink{outRoot: *outRoot}
	if _, err := snapfetch.RunFetch(rootCtx, cfg, sink); err != nil {
		fmt.Fprintf(os.Stderr, "snapfetch: %v\n", err)
		return 1
	}
	return 0
}

// diskSink writes streamed snapshot output under outRoot/<height>_<format>/.
// Behaviour matches the original cosmos-snapshot-fetch storage layout
// exactly so existing snapshotimport.Import + snapshot inspectors keep
// working unchanged.
type diskSink struct {
	outRoot string
	mu      sync.Mutex
	dir     string

	height uint64
	format uint32
	chunks uint32
	hash   []byte
	mdLen  int
}

func (d *diskSink) OnChosen(height uint64, format uint32, chunks uint32, hash []byte, metadata []byte, _ [][]byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dir = filepath.Join(d.outRoot, fmt.Sprintf("%d_%d", height, format))
	if err := os.MkdirAll(d.dir, 0o755); err != nil {
		return fmt.Errorf("mkdir snapshot dir: %w", err)
	}
	if err := os.WriteFile(filepath.Join(d.dir, "metadata.bin"), metadata, 0o644); err != nil {
		return fmt.Errorf("write metadata.bin: %w", err)
	}
	d.height = height
	d.format = format
	d.chunks = chunks
	d.hash = hash
	d.mdLen = len(metadata)
	return nil
}

func (d *diskSink) OnChunk(idx uint32, data []byte) error {
	d.mu.Lock()
	dir := d.dir
	d.mu.Unlock()
	if dir == "" {
		return fmt.Errorf("OnChunk before OnChosen")
	}
	path := filepath.Join(dir, fmt.Sprintf("chunk_%05d.bin", idx))
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write chunk %d: %w", idx, err)
	}
	return nil
}

func (d *diskSink) OnComplete(bytesTotal uint64, goodPeerIDs []string, offeredBy []string) error {
	d.mu.Lock()
	dir := d.dir
	height := d.height
	format := d.format
	chunks := d.chunks
	hash := d.hash
	mdLen := d.mdLen
	d.mu.Unlock()

	meta := snapfetch.SavedMeta{
		Height:          height,
		Format:          format,
		Chunks:          chunks,
		HashHex:         hex.EncodeToString(hash),
		MetadataLen:     mdLen,
		GoodPeers:       goodPeerIDs,
		OfferedBy:       offeredBy,
		DownloadedAt:    time.Now().UTC(),
		BytesTotal:      bytesTotal,
		BytesTotalHuman: snapfetch.HumanBytes(bytesTotal),
	}
	if err := snapfetch.WriteJSONFile(filepath.Join(dir, "meta.json"), meta); err != nil {
		return fmt.Errorf("write meta: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".complete"), nil, 0o644); err != nil {
		return fmt.Errorf("mark complete: %w", err)
	}
	fmt.Printf("            dir=%s\n", dir)
	if err := snapfetch.InspectAndEnrich(dir); err != nil {
		fmt.Printf("[snapfetch] inspect skipped: %v\n", err)
	}
	return nil
}
