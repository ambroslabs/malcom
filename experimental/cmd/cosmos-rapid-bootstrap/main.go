// cosmos-rapid-bootstrap drives a fast snapshot → caught-up flow as a
// single command, mirroring the structured event log of cosmos-statesync-
// bench so wall-clock comparisons are direct.
//
// Pipeline:
//
//  1. malcom snapshot fetch (in-process via snapfetch + diskSink)
//  2. malcom snapshot import (in-process via internal/snapshotimport)
//  3. malcom bootstrap (in-process via internal/cli/bootstrap)
//  4. gaiad init        node_key.json + priv_validator
//  5. config edits      persistent_peers, app-db-backend, statesync.enable
//  6. gaiad start       blocksync to mainnet tip
//  7. RPC poll          wait until catching_up: false
//
// Output: bench-style "[HH:MM:SS +X] event" lines on stdout, plus a
// >>> PHASE: ... line at each major boundary.
//
// Status: this binary is in experimental/ pending a design rework.
// The pipelined fetch+import overlap has been removed (it lived in
// the deleted internal/snapshotappdb wave-parallel path); current
// behaviour is serial fetch → import → bootstrap → start.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"

	"github.com/zrbecker/cosmos-p2p/internal/cli/bootstrap"
	"github.com/zrbecker/cosmos-p2p/internal/pebbleutil"
	"github.com/zrbecker/cosmos-p2p/internal/snapfetch"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotimport"
)

var (
	homeDir      = flag.String("home", "", "gaiad home directory (out)")
	gaiadPath    = flag.String("gaiad", "", "path to gaiad binary")
	chainID      = flag.String("chain-id", "cosmoshub-4", "chain id")
	genesisPath  = flag.String("genesis", "", "path to chain genesis.json")
	rpcsFlag     = flag.String("rpcs", "", "comma-separated RPC URLs (for trust hash + bootstrap)")
	addrbookURL  = flag.String("addrbook", "", "URL of addrbook.json (downloaded once, fed to snapfetch as fallback seed source)")
	peersFile    = flag.String("peers-file", "", "JSON peer DB (cumulative crawl output, used as snapfetch seed source)")
	peersLimit   = flag.Int("peers-limit", 25, "max peers from peers-file to feed gaiad as persistent_peers")
	maxOutbound  = flag.Int("max-outbound", 25, "config.toml [p2p].max_num_outbound_peers")
	rpcPort      = flag.Int("rpc-port", 26657, "local cometbft RPC port")
	freshFlag    = flag.Bool("fresh", false, "wipe -home before starting")
	pollInterval = flag.Duration("poll-interval", 15*time.Second, "RPC poll interval")
	nodeKeyPath  = flag.String("snapfetch-node-key", "", "path to snapfetch p2p node key (default: <home>/snapfetch_node_key.json)")
	preferFresh  = flag.Bool("prefer-fresh", true, "snapfetch: rank candidates by newest height first")
	debugFetch   = flag.Bool("snapfetch-debug", false, "snapfetch: verbose logging")
)

type Bench struct {
	start time.Time
	mu    sync.Mutex
}

func (b *Bench) emit(format string, args ...any) {
	now := time.Now()
	elapsed := now.Sub(b.start)
	b.mu.Lock()
	defer b.mu.Unlock()
	fmt.Printf("[%s +%s] ", now.Format("15:04:05"), formatDur(elapsed))
	fmt.Printf(format+"\n", args...)
}

func (b *Bench) phase(format string, args ...any) {
	b.emit(">>> PHASE: "+format, args...)
}

func formatDur(d time.Duration) string {
	s := int(d.Seconds())
	if s < 60 {
		return fmt.Sprintf("%ds", s)
	}
	if s < 3600 {
		return fmt.Sprintf("%dm%02ds", s/60, s%60)
	}
	return fmt.Sprintf("%dh%02dm%02ds", s/3600, (s%3600)/60, s%60)
}

func main() {
	flag.Parse()
	if *homeDir == "" || *gaiadPath == "" || *genesisPath == "" || *rpcsFlag == "" || *peersFile == "" {
		fmt.Fprintln(os.Stderr, "required: -home -gaiad -genesis -rpcs -peers-file")
		flag.Usage()
		os.Exit(2)
	}
	if *nodeKeyPath == "" {
		*nodeKeyPath = filepath.Join(*homeDir, "snapfetch_node_key.json")
	}

	rpcs := splitRPCs(*rpcsFlag)
	if len(rpcs) == 0 {
		fmt.Fprintln(os.Stderr, "no RPCs after splitting -rpcs")
		os.Exit(2)
	}

	bench := &Bench{start: time.Now()}
	bench.emit("[rapid-bootstrap] home=%s gaiad=%s chain=%s", *homeDir, *gaiadPath, *chainID)

	// 0. Optional fresh wipe.
	if *freshFlag {
		bench.emit("--fresh: wiping %s", *homeDir)
		if err := os.RemoveAll(*homeDir); err != nil {
			fatal("rm home: %v", err)
		}
	}
	if err := os.MkdirAll(filepath.Join(*homeDir, "data"), 0o755); err != nil {
		fatal("mkdir data: %v", err)
	}

	// Trap SIGINT so we can shut gaiad down cleanly if started.
	gaiadCtx, gaiadCancel := context.WithCancel(context.Background())
	defer gaiadCancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		bench.emit("(signal received — shutting down)")
		gaiadCancel()
	}()

	// 1. Snapshot fetch to staging dir.
	stagingRoot := filepath.Join(*homeDir, "snapshot-stage")
	bench.phase("snapfetch starting")
	t0 := time.Now()
	fetchedDir, height := runFetch(bench, gaiadCtx, stagingRoot)
	bench.phase("snapfetch complete elapsed=%s height=%d dir=%s", formatDur(time.Since(t0)), height, fetchedDir)

	// 2. Snapshot import → application.db + extensions/.
	importOut := filepath.Join(stagingRoot, "import-out")
	bench.phase("snapshot import starting")
	t0 = time.Now()
	importStats := runImport(fetchedDir, importOut, height)
	bench.phase("snapshot import complete elapsed=%s stores=%d items=%d ext=%d",
		formatDur(time.Since(t0)),
		len(importStats.Stores), importStats.Items, importStats.Extensions)

	// 3. Pebble cleanup compaction reclaims slack from bulk-load mode.
	// (snapshotimport.Import already does this internally; left here as a
	// belt-and-braces pass — cheap if there's nothing to do.)
	bench.phase("pebble cleanup compaction starting")
	t0 = time.Now()
	if err := pebbleutil.CleanupCompact(filepath.Join(importOut, "application.db")); err != nil {
		fatal("pebble cleanup: %v", err)
	}
	bench.phase("pebble cleanup complete elapsed=%s", formatDur(time.Since(t0)))

	// 4. Bootstrap state.db + blockstore.db + configs.
	bench.phase("cometbft bootstrap-state starting")
	t0 = time.Now()
	runBootstrapInProcess(importOut, height, rpcs)
	bench.phase("cometbft bootstrap-state complete elapsed=%s", formatDur(time.Since(t0)))

	// 5. gaiad init (cherry-pick node_key + priv_validator from a tmp init).
	bench.phase("gaiad init")
	if err := runGaiadInitIfMissing(); err != nil {
		fatal("gaiad init: %v", err)
	}

	// 6. Edit config.toml + app.toml.
	if err := configurePeersAndBackend(bench); err != nil {
		fatal("configure: %v", err)
	}

	// 7. Launch gaiad, poll until caught up.
	bench.phase("gaiad start")
	wg := &sync.WaitGroup{}
	wg.Add(1)
	cmd, gaiadLogPath := startGaiad(bench, gaiadCtx, wg)
	bench.emit("gaiad pid=%d log=%s", cmd.Process.Pid, gaiadLogPath)

	bench.phase("waiting for catchup")
	pollUntilCaughtUp(bench, gaiadCtx, *rpcPort)
	bench.phase("CAUGHT UP")

	bench.emit("stopping gaiad pid=%d", cmd.Process.Pid)
	_ = cmd.Process.Signal(syscall.SIGTERM)
	wg.Wait()
	bench.emit("done. total wall=%s", formatDur(time.Since(bench.start)))
}

// ─── pipeline steps ──────────────────────────────────────────────────────

// diskSink writes streamed snapshot output under outRoot/<height>_<format>/.
// Mirrors cli/snapshotfetch's sink so the staging dir can be fed straight
// to snapshotimport.Import.
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
		return err
	}
	if err := os.WriteFile(filepath.Join(d.dir, "metadata.bin"), metadata, 0o644); err != nil {
		return err
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
	path := filepath.Join(dir, fmt.Sprintf("chunk_%05d.bin", idx))
	return os.WriteFile(path, data, 0o644)
}

func (d *diskSink) OnComplete(bytesTotal uint64, goodPeers []string, offeredBy []string) error {
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
		GoodPeers:       goodPeers,
		OfferedBy:       offeredBy,
		DownloadedAt:    time.Now().UTC(),
		BytesTotal:      bytesTotal,
		BytesTotalHuman: snapfetch.HumanBytes(bytesTotal),
	}
	if err := snapfetch.WriteJSONFile(filepath.Join(dir, "meta.json"), meta); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, ".complete"), nil, 0o644)
}

func runFetch(b *Bench, ctx context.Context, stagingRoot string) (string, int64) {
	if err := os.MkdirAll(stagingRoot, 0o755); err != nil {
		fatal("mkdir staging: %v", err)
	}
	cfg := snapfetch.Config{
		ChainID:     *chainID,
		NodeKeyPath: *nodeKeyPath,
		Cumulative:  *peersFile,
		Logger:      buildSnapfetchLogger(*debugFetch),
	}
	_ = *preferFresh // legacy flag, ignored after PreferFresh was replaced by MinHeight
	if *addrbookURL != "" {
		path, err := materializeAddrbook(*addrbookURL)
		if err != nil {
			fatal("addrbook fetch: %v", err)
		}
		cfg.AddrBook = path
	}

	sink := &diskSink{outRoot: stagingRoot}
	if _, err := snapfetch.RunFetch(ctx, cfg, sink); err != nil {
		fatal("snapfetch: %v", err)
	}
	if sink.dir == "" {
		fatal("snapfetch returned without choosing a snapshot")
	}
	return sink.dir, int64(sink.height)
}

func runImport(snapshotDir, outDir string, height int64) *snapshotimport.Stats {
	stats, err := snapshotimport.Import(snapshotimport.Options{
		SnapshotDir: snapshotDir,
		OutDir:      outDir,
		Height:      height,
		Log:         os.Stdout,
	})
	if err != nil {
		fatal("snapshot import: %v", err)
	}
	return stats
}

func buildSnapfetchLogger(debug bool) cmtlog.Logger {
	logger := cmtlog.NewTMLogger(cmtlog.NewSyncWriter(os.Stderr))
	if debug {
		return cmtlog.NewFilter(logger, cmtlog.AllowDebug())
	}
	return cmtlog.NewFilter(logger, cmtlog.AllowError(),
		cmtlog.AllowInfoWith("module", "snapfetch"))
}

func runBootstrapInProcess(importOut string, height int64, rpcs []string) {
	args := []string{
		"-appdb", importOut,
		"-genesis", *genesisPath,
		"-rpc", strings.Join(rpcs, ","),
		"-height", strconv.FormatInt(height, 10),
		"-out", *homeDir,
		"-write-configs",
		"-app-db-backend", "pebbledb",
		"-place-wasm",
	}
	if rc := bootstrap.Run(args); rc != 0 {
		fatal("bootstrap returned non-zero exit code %d", rc)
	}
}

func runGaiadInitIfMissing() error {
	configDir := filepath.Join(*homeDir, "config")
	dataDir := filepath.Join(*homeDir, "data")
	for _, f := range []struct{ src, dst string }{
		{"config/node_key.json", filepath.Join(configDir, "node_key.json")},
		{"config/priv_validator_key.json", filepath.Join(configDir, "priv_validator_key.json")},
		{"data/priv_validator_state.json", filepath.Join(dataDir, "priv_validator_state.json")},
	} {
		if _, err := os.Stat(f.dst); err == nil {
			continue
		}
		if err := generateInitFile(f.dst, filepath.Base(f.src)); err != nil {
			return fmt.Errorf("%s: %w", f.dst, err)
		}
	}
	return nil
}

func generateInitFile(dst, name string) error {
	tmp, err := os.MkdirTemp("", "gaiad-init-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	cmd := exec.Command(*gaiadPath, "init", "rapid-bootstrap-tmp",
		"--home", tmp, "--chain-id", *chainID)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("gaiad init: %w", err)
	}
	var src string
	switch name {
	case "node_key.json", "priv_validator_key.json":
		src = filepath.Join(tmp, "config", name)
	case "priv_validator_state.json":
		src = filepath.Join(tmp, "data", name)
	default:
		return fmt.Errorf("unexpected init file %q", name)
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

func configurePeersAndBackend(b *Bench) error {
	cfgPath := filepath.Join(*homeDir, "config", "config.toml")
	appPath := filepath.Join(*homeDir, "config", "app.toml")

	peersList, count, err := loadPeers(*peersFile, *peersLimit)
	if err != nil {
		return fmt.Errorf("load peers: %w", err)
	}
	b.emit("loaded %d peers from %s (limit %d)", count, *peersFile, *peersLimit)

	cfgEdits := map[string]string{
		"max_num_outbound_peers":      strconv.Itoa(*maxOutbound),
		"max_packet_msg_payload_size": "262144",
	}
	if peersList != "" {
		cfgEdits["persistent_peers"] = strconv.Quote(peersList)
	}
	if err := setInSection(cfgPath, "[p2p]", cfgEdits); err != nil {
		return fmt.Errorf("set [p2p]: %w", err)
	}
	if err := setInSection(cfgPath, "[statesync]", map[string]string{
		"enable": "false",
	}); err != nil {
		return fmt.Errorf("set [statesync]: %w", err)
	}
	if err := setInSection(appPath, "", map[string]string{
		"minimum-gas-prices": `"0.0025uatom"`,
		"app-db-backend":     `"pebbledb"`,
	}); err != nil {
		return fmt.Errorf("set app.toml top: %w", err)
	}
	if err := setInSection(appPath, "[state-sync]", map[string]string{
		"snapshot-interval": "0",
	}); err != nil {
		return fmt.Errorf("set [state-sync]: %w", err)
	}
	return nil
}

func startGaiad(b *Bench, ctx context.Context, wg *sync.WaitGroup) (*exec.Cmd, string) {
	logPath := filepath.Join(*homeDir, "rapid-bootstrap-gaiad.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		fatal("create gaiad log: %v", err)
	}
	cmd := exec.CommandContext(ctx, *gaiadPath, "start", "--home", *homeDir)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		fatal("gaiad start: %v", err)
	}
	go func() {
		defer wg.Done()
		_ = cmd.Wait()
		_ = logFile.Close()
	}()
	return cmd, logPath
}

func pollUntilCaughtUp(b *Bench, ctx context.Context, port int) {
	t := time.NewTicker(*pollInterval)
	defer t.Stop()
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	var prevHeight int64 = -1
	var prevTs time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		var st statusResp
		if err := fetchJSON(base+"/status", &st); err != nil {
			continue
		}
		h, _ := strconv.ParseInt(st.Result.SyncInfo.Height, 10, 64)
		catching := st.Result.SyncInfo.Catching
		now := time.Now()
		if prevHeight > 0 && h > prevHeight {
			dt := now.Sub(prevTs).Seconds()
			rate := float64(h-prevHeight) / dt
			b.emit("rpc: height %d → %d (Δ%d in %.0fs = %.1f blk/s) catching=%v",
				prevHeight, h, h-prevHeight, dt, rate, catching)
		} else if prevHeight == -1 {
			b.emit("rpc: height %d catching=%v", h, catching)
		}
		prevHeight = h
		prevTs = now
		if !catching {
			return
		}
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────

type statusResp struct {
	Result struct {
		SyncInfo struct {
			Height   string `json:"latest_block_height"`
			Catching bool   `json:"catching_up"`
		} `json:"sync_info"`
	} `json:"result"`
}

func fetchJSON(url string, dst any) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

func splitRPCs(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

type peerEntry struct {
	Addr string `json:"addr"`
}

func loadPeers(path string, limit int) (string, int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", 0, err
	}
	var peers []peerEntry
	if err := json.Unmarshal(data, &peers); err != nil {
		return "", 0, err
	}
	if limit > 0 && len(peers) > limit {
		peers = peers[:limit]
	}
	addrs := make([]string, 0, len(peers))
	for _, p := range peers {
		if p.Addr != "" {
			addrs = append(addrs, p.Addr)
		}
	}
	return strings.Join(addrs, ","), len(addrs), nil
}

func setInSection(path, section string, kv map[string]string) error {
	in, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(in), "\n")
	inSec := section == ""
	written := make(map[string]bool, len(kv))
	out := make([]string, 0, len(lines)+len(kv))

	flush := func() {
		for k, v := range kv {
			if !written[k] {
				out = append(out, fmt.Sprintf("%s = %s", k, v))
				written[k] = true
			}
		}
	}

	for _, ln := range lines {
		trim := strings.TrimSpace(ln)
		if strings.HasPrefix(trim, "[") && strings.HasSuffix(trim, "]") {
			if inSec {
				flush()
			}
			inSec = (trim == section)
			out = append(out, ln)
			continue
		}
		if inSec {
			eq := strings.Index(trim, "=")
			if eq > 0 {
				k := strings.TrimSpace(trim[:eq])
				if v, ok := kv[k]; ok {
					out = append(out, fmt.Sprintf("%s = %s", k, v))
					written[k] = true
					continue
				}
			}
		}
		out = append(out, ln)
	}
	if inSec {
		flush()
	}
	return os.WriteFile(path, []byte(strings.Join(out, "\n")), 0o644)
}

func materializeAddrbook(url string) (string, error) {
	if !strings.HasPrefix(url, "http") {
		return url, nil
	}
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("addrbook HTTP %d", resp.StatusCode)
	}
	tmp, err := os.CreateTemp("", "addrbook-*.json")
	if err != nil {
		return "", err
	}
	defer tmp.Close()
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		return "", err
	}
	return tmp.Name(), nil
}

// kept for future log-prefixing if rapid-bootstrap regrows subprocess steps.
type prefixWriter struct {
	prefix string
	buf    []byte
	mu     sync.Mutex
	calls  atomic.Int64
}

func newPrefixWriter(prefix string) *prefixWriter {
	return &prefixWriter{prefix: prefix}
}

func (w *prefixWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		idx := bytesIndexNL(w.buf)
		if idx < 0 {
			break
		}
		line := w.buf[:idx]
		w.buf = w.buf[idx+1:]
		fmt.Print(w.prefix)
		fmt.Println(string(line))
		w.calls.Add(1)
	}
	return len(p), nil
}

func bytesIndexNL(b []byte) int {
	for i, c := range b {
		if c == '\n' {
			return i
		}
	}
	return -1
}

var _ = newPrefixWriter // keep referenced

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "fatal: "+format+"\n", args...)
	os.Exit(1)
}
