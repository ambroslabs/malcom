// cosmos-rapid-bootstrap drives a fast snapshot → caught-up flow as a
// single command, mirroring the structured event log of cosmos-statesync-
// bench so wall-clock comparisons are direct.
//
// Pipeline:
//
//  1. cosmos-snapshot-fetch    snapshot chunks via raw p2p
//  2. snapshotappdb.Import     application.db with wave-parallel iavl
//                              + bulk-load-tuned pebble. Writes directly
//                              into <out>/data so step 4 can skip its copy.
//  3. cosmos-bootstrap-gaia    state.db + blockstore.db + minimal configs
//                              -skip-app-copy because we already placed
//                              application.db at the destination.
//  4. gaiad init               just enough for node_key.json + priv_validator
//                              (we then overwrite genesis from -genesis).
//  5. config edits             persistent_peers, app-db-backend = pebbledb,
//                              statesync.enable = false (we already have
//                              state.db from step 3).
//  6. gaiad start              blocksync to mainnet tip.
//  7. RPC poll                 wait until catching_up: false.
//
// Output: bench-style "[HH:MM:SS +X] event" lines on stdout, plus a
// >>> PHASE: ... line at each major boundary so the resulting log can be
// archived alongside the cosmos-statesync-bench runs and compared.
package main

import (
	"bufio"
	"context"
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

	"github.com/zrbecker/cosmos-p2p/internal/snapshotappdb"
)

var (
	homeDir       = flag.String("home", "", "gaiad home directory (out)")
	gaiadPath     = flag.String("gaiad", "", "path to gaiad binary")
	chainID       = flag.String("chain-id", "cosmoshub-4", "chain id")
	genesisPath   = flag.String("genesis", "", "path to chain genesis.json")
	rpcsFlag      = flag.String("rpcs", "", "comma-separated RPC URLs (for trust hash + bootstrap)")
	addrbookURL   = flag.String("addrbook", "", "URL of addrbook.json (passed to snapshot-fetch + cached locally)")
	peersFile     = flag.String("peers-file", "", "JSON peer DB (passed to snapshot-fetch as -cumulative)")
	peersLimit    = flag.Int("peers-limit", 25, "max peers from peers-file to feed gaiad as persistent_peers")
	maxOutbound   = flag.Int("max-outbound", 25, "config.toml [p2p].max_num_outbound_peers")
	rpcPort       = flag.Int("rpc-port", 26657, "local cometbft RPC port")
	freshFlag     = flag.Bool("fresh", false, "wipe -home before starting")
	pollInterval  = flag.Duration("poll-interval", 15*time.Second, "RPC poll interval")
	snapshotFetch = flag.String("snapshot-fetch", "", "path to cosmos-snapshot-fetch binary (default: look on PATH)")
	bootstrapBin  = flag.String("bootstrap-bin", "", "path to cosmos-bootstrap-gaia binary (default: look on PATH)")
	concurrency   = flag.Int("import-concurrency", 0, "snapshotappdb.Import concurrency (0 = auto min(NumCPU,8))")
	trustOffset   = flag.Int64("trust-offset", 1000, "trust_height = chain_tip - this (used by bootstrap-gaia)")
	stagingDir    = flag.String("staging", "", "snapshot chunk staging dir (default: <home>/snapshots-staging)")
)

// ─── event-log helpers (match cosmos-statesync-bench's format) ────────────

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

// ─── flags + main ────────────────────────────────────────────────────────

func main() {
	flag.Parse()
	if *homeDir == "" || *gaiadPath == "" || *genesisPath == "" || *rpcsFlag == "" || *peersFile == "" {
		fmt.Fprintln(os.Stderr, "required: -home -gaiad -genesis -rpcs -peers-file")
		flag.Usage()
		os.Exit(2)
	}
	if *snapshotFetch == "" {
		*snapshotFetch = which("cosmos-snapshot-fetch")
		if *snapshotFetch == "" {
			fmt.Fprintln(os.Stderr, "cosmos-snapshot-fetch not in PATH; pass -snapshot-fetch")
			os.Exit(2)
		}
	}
	if *bootstrapBin == "" {
		*bootstrapBin = which("cosmos-bootstrap-gaia")
		if *bootstrapBin == "" {
			fmt.Fprintln(os.Stderr, "cosmos-bootstrap-gaia not in PATH; pass -bootstrap-bin")
			os.Exit(2)
		}
	}
	if *stagingDir == "" {
		*stagingDir = filepath.Join(*homeDir, "snapshots-staging")
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
	if err := os.MkdirAll(*stagingDir, 0o755); err != nil {
		fatal("mkdir staging: %v", err)
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

	// 1. Snapshot fetch (subprocess).
	bench.phase("snapshot fetch starting")
	t0 := time.Now()
	snapDir := runSnapshotFetch(bench)
	bench.phase("snapshot fetch complete dir=%s elapsed=%s", snapDir, formatDur(time.Since(t0)))

	// Parse meta.json for height.
	height := readHeightFromMeta(filepath.Join(snapDir, "meta.json"))

	// 2. Import to application.db (in-process, wave-parallel iavl + tuned
	//    pebble). Pass <home>/data as -out so application.db lands at
	//    <home>/data/application.db — exactly where gaiad expects it.
	bench.phase("import starting (height=%d)", height)
	t0 = time.Now()
	dataDir := filepath.Join(*homeDir, "data")
	stats, err := snapshotappdb.Import(snapDir, dataDir, height,
		snapshotappdb.BackendPebble, filepath.Join(dataDir, "extensions"), *concurrency)
	if err != nil {
		fatal("import: %v", err)
	}
	bench.phase("import complete elapsed=%s stores=%d items=%d uncompressed=%s",
		formatDur(time.Since(t0)), stats.Stores, stats.Items, snapshotappdb.HumanBytes(stats.BytesUncompressed))

	// 3. Pebble cleanup compaction reclaims slack from bulk-load mode.
	bench.phase("pebble cleanup compaction starting")
	t0 = time.Now()
	if err := snapshotappdb.PebbleCleanupCompact(filepath.Join(dataDir, "application.db")); err != nil {
		fatal("pebble cleanup: %v", err)
	}
	bench.phase("pebble cleanup complete elapsed=%s", formatDur(time.Since(t0)))

	// 4. Bootstrap state.db + blockstore.db + configs (subprocess to
	//    cosmos-bootstrap-gaia with -skip-app-copy since application.db
	//    is already at <home>/data/application.db).
	bench.phase("cometbft bootstrap-state starting")
	t0 = time.Now()
	runBootstrapGaia(bench, height, rpcs)
	bench.phase("cometbft bootstrap-state complete elapsed=%s", formatDur(time.Since(t0)))

	// 5. gaiad init (idempotent — gives us node_key.json + priv_validator
	//    files + a default app.toml/config.toml). bootstrap-gaia already
	//    wrote its own minimal app/config.toml; we'll do edits next.
	bench.phase("gaiad init")
	if err := runGaiadInitIfMissing(); err != nil {
		fatal("gaiad init: %v", err)
	}

	// 6. Edit config.toml + app.toml: load persistent_peers, set app-db-
	//    backend, disable state-sync (we already have state).
	if err := configurePeersAndBackend(bench); err != nil {
		fatal("configure: %v", err)
	}

	// 7. Launch gaiad, tail log into our event stream, poll RPC.
	bench.phase("gaiad start")
	wg := &sync.WaitGroup{}
	wg.Add(1)
	cmd, gaiadLogPath := startGaiad(bench, gaiadCtx, wg)
	bench.emit("gaiad pid=%d log=%s", cmd.Process.Pid, gaiadLogPath)

	// 8. Poll RPC until catching_up: false.
	bench.phase("waiting for catchup")
	pollUntilCaughtUp(bench, gaiadCtx, *rpcPort)
	bench.phase("CAUGHT UP")

	// 9. Shut down gaiad gracefully.
	bench.emit("stopping gaiad pid=%d", cmd.Process.Pid)
	_ = cmd.Process.Signal(syscall.SIGTERM)
	wg.Wait()
	bench.emit("done. total wall=%s", formatDur(time.Since(bench.start)))
}

// ─── pipeline steps ──────────────────────────────────────────────────────

func runSnapshotFetch(b *Bench) string {
	args := []string{
		"-chain-id", *chainID,
		"-cumulative", *peersFile,
		"-out", *stagingDir,
		"-prefer-fresh",
	}
	if *addrbookURL != "" {
		// snapshot-fetch wants a local file. If it's a URL, download once
		// to a tempfile.
		path, err := materializeAddrbook(*addrbookURL)
		if err != nil {
			fatal("addrbook fetch: %v", err)
		}
		args = append(args, "-addrbook", path)
	}
	cmd := exec.Command(*snapshotFetch, args...)
	cmd.Stdout = newPrefixWriter("[snap-fetch] ")
	cmd.Stderr = newPrefixWriter("[snap-fetch] ")
	if err := cmd.Run(); err != nil {
		fatal("snapshot-fetch: %v", err)
	}
	// Find the most-recent <H>_<fmt> dir under staging that has .complete.
	dir, err := newestCompleteSnap(*stagingDir)
	if err != nil {
		fatal("locate fetched snapshot: %v", err)
	}
	return dir
}

func runBootstrapGaia(b *Bench, height int64, rpcs []string) {
	args := []string{
		"-appdb", filepath.Join(*homeDir, "data"), // unused with skip-app-copy but required arg
		"-genesis", *genesisPath,
		"-rpc", strings.Join(rpcs, ","),
		"-height", strconv.FormatInt(height, 10),
		"-out", *homeDir,
		"-write-configs",
		"-app-db-backend", "pebbledb",
		"-skip-app-copy",
		"-place-wasm",
	}
	cmd := exec.Command(*bootstrapBin, args...)
	cmd.Stdout = newPrefixWriter("[bootstrap] ")
	cmd.Stderr = newPrefixWriter("[bootstrap] ")
	if err := cmd.Run(); err != nil {
		fatal("bootstrap-gaia: %v", err)
	}
}

func runGaiadInitIfMissing() error {
	// gaiad init writes config/{config.toml, app.toml, client.toml,
	// node_key.json, priv_validator_key.json} and data/{priv_validator_state.json}.
	// bootstrap-gaia already wrote config.toml, app.toml, client.toml. So
	// we only need node_key.json + priv_validator_key.json + state.
	//
	// gaiad init will REFUSE if config/genesis.json already exists, but
	// it has --overwrite. We don't want to overwrite genesis, so we use
	// a different approach: run init with a TMP home, then cherry-pick
	// the auto-generated files we need.
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

// generateInitFile shells out a one-off `gaiad init` into a tmp home and
// copies the requested file out. Yes, gaiad init is overkill for one
// file — but the alternative is reproducing tendermint key generation
// here, which is more code.
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

	// Load peers from peers-file, capped at *peersLimit.
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
	// Disable statesync — bootstrap-state already populated state.db.
	if err := setInSection(cfgPath, "[statesync]", map[string]string{
		"enable": "false",
	}); err != nil {
		return fmt.Errorf("set [statesync]: %w", err)
	}
	// app.toml: ensure pebbledb + min-gas + snapshot-interval=0.
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

func which(bin string) string {
	if p, err := exec.LookPath(bin); err == nil {
		return p
	}
	return ""
}

func newestCompleteSnap(root string) (string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return "", err
	}
	var best string
	var bestH int64
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		marker := filepath.Join(root, e.Name(), ".complete")
		if _, err := os.Stat(marker); err != nil {
			continue
		}
		// Parse <height>_<format> directory name.
		parts := strings.SplitN(e.Name(), "_", 2)
		if len(parts) < 2 {
			continue
		}
		h, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			continue
		}
		if h > bestH {
			bestH = h
			best = filepath.Join(root, e.Name())
		}
	}
	if best == "" {
		return "", fmt.Errorf("no completed snapshot under %s", root)
	}
	return best, nil
}

func readHeightFromMeta(metaPath string) int64 {
	b, err := os.ReadFile(metaPath)
	if err != nil {
		fatal("read meta.json: %v", err)
	}
	var m struct {
		Height int64 `json:"height"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		fatal("parse meta.json: %v", err)
	}
	return m.Height
}

// peerEntry mirrors the JSON shape produced by cosmos-archive's peer DB.
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

// setInSection edits a TOML file: for each key in kv, sets `key = value`
// inside [section] (use "" for top-level).
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

	for i, ln := range lines {
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
		_ = i
	}
	if inSec {
		flush()
	}
	return os.WriteFile(path, []byte(strings.Join(out, "\n")), 0o644)
}

// materializeAddrbook downloads URL to a tempfile and returns the path.
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

// ─── prefix writer ───────────────────────────────────────────────────────

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

// avoid unused warning for bufio (kept in case prefix writer is later
// rewritten to use it)
var _ = bufio.NewReader

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "fatal: "+format+"\n", args...)
	os.Exit(1)
}
