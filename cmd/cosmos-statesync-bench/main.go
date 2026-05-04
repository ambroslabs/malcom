// cosmos-statesync-bench drives a cometbft state-sync run end to end and
// emits a phase-by-phase timing CSV plus a live event stream to stdout.
//
// Phases tracked:
//
//	1. State sync located         — gaiad picked a snapshot from peers
//	2. State sync downloaded      — all chunks delivered + applied to app
//	3. Application DB constructed — IAVL nodes written and committed
//	4. IAVL upgrade fast lookup   — gaiad's first-start fast-storage build
//	5. Gaiad bootstrapped         — node service up, RPC listening
//	6. Block sync started         — first block executed past snapshot height
//	7. Node caught up             — catching_up flips to false
//
// Output:
//
//	<home>/bench/timing.csv          — phase,wall_seconds,height,run,note
//	<home>/bench/gaiad.log           — current attempt's stdout/stderr
//	<home>/bench/runs/gaiad.log.NNN  — preserved logs from earlier attempts
//	<home>/bench/cache/addrbook.json — cached upstream addrbook (6h TTL)
//
// The bash predecessor (scripts/statesync-bench.sh) hit too many bash
// footguns: pipefail+SIGPIPE, set -e killing the watcher subshell on a
// grep miss, ANSI escape codes in cometbft logs invisibly breaking
// regex parsing, subshell state isolation, stale tail processes
// double-emitting events. This is the same logic in Go: typed JSON
// for RPC, regex on pre-stripped log lines, goroutines + channels +
// atomic for shared state, single binary.
package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// ─── flags ────────────────────────────────────────────────────────────

var (
	homeDir      = flag.String("home", "", "gaiad home directory")
	gaiadPath    = flag.String("gaiad", "", "path to gaiad binary")
	chainID      = flag.String("chain-id", "", "chain id (e.g. cosmoshub-4)")
	genesisPath  = flag.String("genesis", "", "path to genesis.json")
	rpcsFlag     = flag.String("rpcs", "", "comma-separated RPC URLs for state-sync trust")
	addrbookURL  = flag.String("addrbook", "", "URL to download addrbook.json (cached locally for 6h)")
	peersFile    = flag.String("peers-file", "", "JSON file with [{addr:\"node@host:port\"}] (downloader's peers-cumulative.json)")
	peersLimit   = flag.Int("peers-limit", 80, "max peers to feed into persistent_peers")
	maxOutbound  = flag.Int("max-outbound", 60, "config.toml [p2p].max_num_outbound_peers")
	trustOffset  = flag.Int64("trust-offset", 1000, "trust_height = chain_tip - this")
	rpcPort      = flag.Int("rpc-port", 26657, "local cometbft RPC port to poll")
	freshFlag    = flag.Bool("fresh", false, "wipe data/ and clear CSV history")
	pollInterval = flag.Duration("poll-interval", 15*time.Second, "RPC poll interval")
)

// ─── helpers ──────────────────────────────────────────────────────────

func formatDur(d time.Duration) string {
	s := int(d.Seconds())
	switch {
	case s < 60:
		return fmt.Sprintf("%ds", s)
	case s < 3600:
		return fmt.Sprintf("%dm%02ds", s/60, s%60)
	default:
		return fmt.Sprintf("%dh%02dm%02ds", s/3600, (s%3600)/60, s%60)
	}
}

var ansiRE = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func stripANSI(s string) string { return ansiRE.ReplaceAllString(s, "") }

var (
	heightRE      = regexp.MustCompile(`height=(\d+)`)
	heightHashRE  = regexp.MustCompile(`height #(\d+)`)
	chunkRE       = regexp.MustCompile(`chunk=(\d+)`)
	totalRE       = regexp.MustCompile(`total=(\d+)`)
	storeKeyRE    = regexp.MustCompile(`store_key=(\S+)`)
)

func extractInt64(re *regexp.Regexp, s string) int64 {
	m := re.FindStringSubmatch(s)
	if m == nil {
		return 0
	}
	n, _ := strconv.ParseInt(m[1], 10, 64)
	return n
}

func extractInt(re *regexp.Regexp, s string) int {
	m := re.FindStringSubmatch(s)
	if m == nil {
		return 0
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

// ─── phases + bench state ─────────────────────────────────────────────

type Phase int

const (
	phaseLocated Phase = iota
	phaseDownloaded
	phaseAppDB
	phaseIAVL
	phaseBootstrapped
	phaseBlocksync
	phaseCaughtUp
)

func phaseName(p Phase) string {
	return [...]string{
		"State sync located",
		"State sync downloaded",
		"Application DB constructed",
		"IAVL upgrade fast lookup",
		"Gaiad bootstrapped",
		"Block sync started",
		"Node caught up",
	}[p]
}

func phaseFromName(s string) (Phase, bool) {
	for p := phaseLocated; p <= phaseCaughtUp; p++ {
		if phaseName(p) == s {
			return p, true
		}
	}
	return 0, false
}

// stageForPhase: minimum stage value once that phase has fired.
func stageForPhase(p Phase) int {
	return [...]int{1, 2, 2, 3, 4, 5, 6}[p]
}

type Bench struct {
	homeDir  string
	benchDir string
	csvPath  string
	logPath  string

	runNum   int
	runStart time.Time

	csvFile *os.File
	csvW    *csv.Writer
	csvMu   sync.Mutex

	firedMu sync.Mutex
	fired   map[Phase]bool
	stage   atomic.Int32

	// lastEventTs is unix seconds of the last "interesting" event.
	// Heartbeat uses it for backoff. emit() updates it; heartbeat()
	// does not.
	lastEventTs atomic.Int64

	emitMu sync.Mutex
}

func (b *Bench) emit(format string, args ...any) {
	b.emitLine(true, format, args...)
}

func (b *Bench) heartbeat(format string, args ...any) {
	b.emitLine(false, format, args...)
}

func (b *Bench) emitLine(interesting bool, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	now := time.Now()
	elapsed := now.Sub(b.runStart)
	b.emitMu.Lock()
	fmt.Printf("[%s +%s] %s\n", now.Format("15:04:05"), formatDur(elapsed), msg)
	b.emitMu.Unlock()
	if interesting {
		b.lastEventTs.Store(now.Unix())
	}
}

func (b *Bench) phaseHit(p Phase, height int64, note string) {
	b.firedMu.Lock()
	if b.fired[p] {
		b.firedMu.Unlock()
		return
	}
	b.fired[p] = true
	b.firedMu.Unlock()

	elapsed := time.Since(b.runStart)
	if note != "" {
		b.emit(">>> PHASE: %s height=%d (%s)", phaseName(p), height, note)
	} else {
		b.emit(">>> PHASE: %s height=%d", phaseName(p), height)
	}

	b.csvMu.Lock()
	b.csvW.Write([]string{
		phaseName(p),
		strconv.Itoa(int(elapsed.Seconds())),
		strconv.FormatInt(height, 10),
		strconv.Itoa(b.runNum),
		strings.ReplaceAll(note, ",", ";"),
	})
	b.csvW.Flush()
	b.csvMu.Unlock()

	if s := int32(stageForPhase(p)); s > b.stage.Load() {
		b.stage.Store(s)
	}
}

// ─── log watcher ──────────────────────────────────────────────────────

func (b *Bench) watchLog(ctx context.Context) {
	// Open with retry — gaiad may not have created the file yet.
	var f *os.File
	for {
		if ctx.Err() != nil {
			return
		}
		var err error
		f, err = os.Open(b.logPath)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	defer f.Close()
	f.Seek(0, io.SeekEnd)

	reader := bufio.NewReader(f)
	seenDisc := make(map[int64]bool)
	rejected := 0

	for ctx.Err() == nil {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if err != nil {
			return
		}
		b.parseLine(stripANSI(strings.TrimRight(line, "\n")), seenDisc, &rejected)
	}
}

func (b *Bench) parseLine(line string, seenDisc map[int64]bool, rejected *int) {
	switch {
	case strings.Contains(line, "Discovered new snapshot"):
		h := extractInt64(heightRE, line)
		if !seenDisc[h] {
			seenDisc[h] = true
			b.emit("snapshot discovered height=%d", h)
		}
		return
	case strings.Contains(line, "Snapshot rejected"):
		h := extractInt64(heightRE, line)
		*rejected++
		b.emit("snapshot REJECTED height=%d (total rejected=%d)", h, *rejected)
		return
	case strings.Contains(line, "failed to fetch and verify app hash"):
		h := extractInt64(heightHashRE, line)
		if h == 0 {
			h = extractInt64(heightRE, line)
		}
		b.emit("verification FAILED at height %d", h)
		return
	case strings.Contains(line, "FATAL"), strings.Contains(line, "panic:"):
		b.emit("!!! %s", line)
		return
	}

	stage := int(b.stage.Load())
	switch stage {
	case 0:
		if strings.Contains(line, "VerifyAt") ||
			strings.Contains(line, "Snapshot accepted") ||
			strings.Contains(line, "snapshot offered") ||
			strings.Contains(line, "Offering snapshot") ||
			strings.Contains(line, "Verifying app hash") {
			b.phaseHit(phaseLocated, extractInt64(heightRE, line), "")
		}
	case 1:
		switch {
		case strings.Contains(line, "Fetching snapshot chunk"):
			c := extractInt(chunkRE, line)
			t := extractInt(totalRE, line)
			if c == 0 || c == t-1 || c%25 == 0 {
				b.emit("fetching chunk %d/%d", c, t)
			}
		case strings.Contains(line, "Applied snapshot chunk"):
			// IMPORTANT: cometbft logs "Applied snapshot chunk" after
			// app.ApplySnapshotChunk() returns ACCEPT, but cosmos-sdk's
			// Manager.RestoreChunk just enqueues the chunk ID onto a
			// 1024-buffered channel and returns ACCEPT for chunks
			// 0..total-2. A background goroutine drains the queue and
			// does the real IAVL writes. Only the LAST chunk
			// (total-1) blocks until the restorer goroutine fully
			// drains, so its "Applied" log truly means "IAVL writes
			// complete". For the rest we emit "queued chunk" to
			// avoid the misleading "applied" wording.
			c := extractInt(chunkRE, line)
			t := extractInt(totalRE, line)
			if c == 0 || c%25 == 0 || c >= t-3 {
				if c == t-1 {
					b.emit("queued chunk %d/%d (final — IAVL writer fully drained)", c, t)
				} else {
					b.emit("queued chunk %d/%d (handed to restorer goroutine)", c, t)
				}
			}
		case strings.Contains(line, "Restored snapshot chunk"):
			// Emitted by our forked cosmossdk.io/store: fires when the
			// StreamReader's ChunkReader closes a chunk's
			// io.ReadCloser, i.e. the chunk's compressed bytes have
			// been fully fed into zlib → protobuf → IAVL writer. This
			// is the closest signal we have to "chunk N has actually
			// been restored" without instrumenting the IAVL writer.
			c := extractInt(chunkRE, line)
			t := extractInt(totalRE, line)
			if c == 0 || c%25 == 0 || c >= t-3 {
				b.emit("restored chunk %d/%d (drained from chunk channel)", c, t)
			}
		case strings.Contains(line, "Verified ABCI app"):
			// Fires after the LAST RestoreChunk returned + appHash
			// matched the trusted hash from the light client. This
			// is the unambiguous "snapshot is in the app, IAVL is
			// built, hashes match consensus" point.
			b.emit("Verified ABCI app — IAVL fully restored, appHash matches consensus")
			b.phaseHit(phaseDownloaded, 0, "")
			b.phaseHit(phaseAppDB, 0, "")
		case strings.Contains(line, "Snapshot restored"):
			// cometbft's "Done! 🎉" line, fires after Verified ABCI
			// app. Fall through to phase if we missed the verify
			// line (older versions don't always emit it).
			b.emit("Snapshot restored — state-sync done")
			b.phaseHit(phaseDownloaded, 0, "")
			b.phaseHit(phaseAppDB, 0, "")
		case strings.Contains(line, "State sync completed"),
			strings.Contains(line, "Applied snapshot to state machine"):
			b.emit("state sync downloaded")
			b.phaseHit(phaseDownloaded, 0, "")
			b.phaseHit(phaseAppDB, 0, "")
		}
	case 2, 3:
		switch {
		case strings.Contains(line, "Upgrading IAVL storage"),
			strings.Contains(line, "upgradeToFastStorage"):
			store := storeKeyRE.FindString(line)
			b.emit("IAVL fast-storage upgrade: %s", store)
			if stage == 2 {
				b.phaseHit(phaseIAVL, 0, "")
			}
		case strings.Contains(line, "Started node"),
			strings.Contains(line, "Starting Node service"),
			strings.Contains(line, "Starting RPC HTTP server"):
			if stage == 2 {
				b.phaseHit(phaseIAVL, 0, "no log emitted; skipped")
			}
			b.phaseHit(phaseBootstrapped, 0, "")
		}
	case 4:
		if strings.Contains(line, "executed block") ||
			strings.Contains(line, "finalized block") ||
			strings.Contains(line, "committed state") {
			b.phaseHit(phaseBlocksync, extractInt64(heightRE, line), "")
		}
	}
}

// ─── RPC poll ─────────────────────────────────────────────────────────

type statusResp struct {
	Result struct {
		SyncInfo struct {
			Height   string `json:"latest_block_height"`
			Catching bool   `json:"catching_up"`
		} `json:"sync_info"`
	} `json:"result"`
}

type netInfoResp struct {
	Result struct {
		NPeers string `json:"n_peers"`
	} `json:"result"`
}

func fetchJSON(url string, v any) error {
	c := &http.Client{Timeout: 5 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(v)
}

func (b *Bench) pollRPC(ctx context.Context, port int) {
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	var (
		prevHeight   int64 = -1
		prevHeightTs time.Time
		prevCatching string
		prevPeers    int = -1
		pollCount    int
	)
	t := time.NewTicker(*pollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		pollCount++

		var st statusResp
		if err := fetchJSON(base+"/status", &st); err != nil {
			continue
		}
		var ni netInfoResp
		nPeers := -1
		if err := fetchJSON(base+"/net_info", &ni); err == nil {
			nPeers, _ = strconv.Atoi(ni.Result.NPeers)
		}

		height, _ := strconv.ParseInt(st.Result.SyncInfo.Height, 10, 64)
		catching := strconv.FormatBool(st.Result.SyncInfo.Catching)
		now := time.Now()

		if prevHeight == -1 {
			prevHeight = height
			prevHeightTs = now
			prevCatching = catching
			prevPeers = nPeers
			continue
		}

		switch {
		case height != prevHeight:
			delta := height - prevHeight
			dt := now.Sub(prevHeightTs).Seconds()
			rate := float64(delta) / dt
			b.emit("rpc: height %d → %d (Δ%d in %.0fs = %.1f blk/s) catching=%s peers=%d",
				prevHeight, height, delta, dt, rate, catching, nPeers)
			prevHeight = height
			prevHeightTs = now
		case catching != prevCatching:
			b.emit("rpc: catching_up: %s → %s height=%d peers=%d",
				prevCatching, catching, height, nPeers)
		case nPeers != -1 && nPeers != prevPeers:
			b.emit("peers: %d → %d connected", prevPeers, nPeers)
		case pollCount%4 == 0 && nPeers != -1:
			b.heartbeat("peers: %d connected (height=%d)", nPeers, height)
		}
		prevCatching = catching
		prevPeers = nPeers

		if !st.Result.SyncInfo.Catching {
			b.phaseHit(phaseCaughtUp, height, "")
			return
		}
	}
}

// ─── heartbeat ────────────────────────────────────────────────────────

func (b *Bench) runHeartbeat(ctx context.Context) {
	// Quiet schedule: 30s, 1m, 2m, 5m, then +5m forever.
	// Earlier-too-chatty thresholds (5s, 15s) were dropped per user
	// preference once we learned how often "real" gaiad events
	// land naturally.
	triggers := []int{30, 60, 120, 300}
	nextTrigger := func(i int) int {
		if i < len(triggers) {
			return triggers[i]
		}
		// i=4 → 600, i=5 → 900, i=6 → 1200, ...
		return 300 * (i - 2)
	}

	i := 0
	prevLast := b.lastEventTs.Load()
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		last := b.lastEventTs.Load()
		if last != prevLast {
			i = 0
			prevLast = last
			continue
		}
		idle := time.Now().Unix() - last
		if int(idle) >= nextTrigger(i) {
			b.heartbeat("(quiet — %s since last event)", formatDur(time.Duration(idle)*time.Second))
			i++
		}
	}
}

// ─── config.toml + app.toml editing ───────────────────────────────────

// setInSection replaces key=value lines for the given map within the
// given TOML section. section="" targets top-level keys (everything
// before the first [section] header). Returns an error if a key
// listed in kv isn't present in the target section.
func setInSection(path, section string, kv map[string]string) error {
	in, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(in), "\n")
	cur := "" // current section, "" = top level
	out := make([]string, 0, len(lines))
	found := make(map[string]bool, len(kv))

	for _, line := range lines {
		trim := strings.TrimSpace(line)
		// A section header transitions cur. Header lines themselves
		// are never replaced.
		if strings.HasPrefix(trim, "[") && strings.HasSuffix(trim, "]") {
			cur = trim
			out = append(out, line)
			continue
		}
		replaced := false
		if cur == section {
			for k, v := range kv {
				fields := strings.Fields(trim)
				if len(fields) >= 2 && fields[0] == k && fields[1] == "=" {
					out = append(out, fmt.Sprintf("%s = %s", k, v))
					found[k] = true
					replaced = true
					break
				}
			}
		}
		if !replaced {
			out = append(out, line)
		}
	}
	for k := range kv {
		if !found[k] {
			return fmt.Errorf("section %q key %s not found in %s", section, k, path)
		}
	}
	return os.WriteFile(path, []byte(strings.Join(out, "\n")), 0644)
}

// ─── peers-cumulative.json loader ─────────────────────────────────────

type peerEntry struct {
	Addr string `json:"addr"`
}

func loadPeers(path string, limit int) (string, int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", 0, err
	}
	var ps []peerEntry
	if err := json.Unmarshal(data, &ps); err != nil {
		return "", 0, err
	}
	if len(ps) > limit {
		ps = ps[:limit]
	}
	addrs := make([]string, 0, len(ps))
	for _, p := range ps {
		addrs = append(addrs, p.Addr)
	}
	return strings.Join(addrs, ","), len(addrs), nil
}

// ─── addrbook cache ───────────────────────────────────────────────────

func cachedAddrbook(cacheDir, url string, ttl time.Duration) (string, error) {
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return "", err
	}
	cachePath := filepath.Join(cacheDir, "addrbook.json")
	if fi, err := os.Stat(cachePath); err == nil {
		if age := time.Since(fi.ModTime()); age < ttl {
			fmt.Printf("[bench] using cached addrbook (%s old): %s\n", formatDur(age), cachePath)
			return cachePath, nil
		}
	}
	fmt.Printf("[bench] fetching addrbook %s ...\n", url)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("addrbook fetch: HTTP %d", resp.StatusCode)
	}
	tmp := cachePath + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		out.Close()
		os.Remove(tmp)
		return "", err
	}
	out.Close()
	return cachePath, os.Rename(tmp, cachePath)
}

// ─── trust hash fetch ─────────────────────────────────────────────────

func fetchTrust(rpc string, offset int64) (int64, string, error) {
	var st statusResp
	if err := fetchJSON(rpc+"/status", &st); err != nil {
		return 0, "", fmt.Errorf("status: %w", err)
	}
	tip, _ := strconv.ParseInt(st.Result.SyncInfo.Height, 10, 64)
	height := tip - offset
	var blk struct {
		Result struct {
			BlockID struct {
				Hash string `json:"hash"`
			} `json:"block_id"`
		} `json:"result"`
	}
	url := fmt.Sprintf("%s/block?height=%d", rpc, height)
	if err := fetchJSON(url, &blk); err != nil {
		return 0, "", fmt.Errorf("block: %w", err)
	}
	return height, blk.Result.BlockID.Hash, nil
}

// ─── main ─────────────────────────────────────────────────────────────

func main() {
	flag.Parse()
	for _, req := range []struct {
		name, val string
	}{
		{"home", *homeDir}, {"gaiad", *gaiadPath}, {"chain-id", *chainID},
		{"genesis", *genesisPath}, {"rpcs", *rpcsFlag},
	} {
		if req.val == "" {
			fmt.Fprintf(os.Stderr, "missing -%s\n", req.name)
			os.Exit(2)
		}
	}
	if _, err := os.Stat(*gaiadPath); err != nil {
		fmt.Fprintf(os.Stderr, "gaiad not found: %v\n", err)
		os.Exit(2)
	}
	if _, err := os.Stat(*genesisPath); err != nil {
		fmt.Fprintf(os.Stderr, "genesis not found: %v\n", err)
		os.Exit(2)
	}

	rpcs := strings.Split(*rpcsFlag, ",")
	primary := strings.TrimRight(rpcs[0], "/")

	benchDir := filepath.Join(*homeDir, "bench")
	runsDir := filepath.Join(benchDir, "runs")
	cacheDir := filepath.Join(benchDir, "cache")
	logPath := filepath.Join(benchDir, "gaiad.log")
	csvPath := filepath.Join(benchDir, "timing.csv")
	for _, d := range []string{benchDir, runsDir, cacheDir} {
		os.MkdirAll(d, 0755)
	}

	// Run number = how many rotated logs already exist + 1.
	prior, _ := filepath.Glob(filepath.Join(runsDir, "gaiad.log.*"))
	runNum := len(prior) + 1

	// Rotate previous gaiad.log if present.
	if _, err := os.Stat(logPath); err == nil {
		dst := filepath.Join(runsDir, fmt.Sprintf("gaiad.log.%03d", runNum-1))
		if err := os.Rename(logPath, dst); err == nil {
			fmt.Printf("[bench] previous gaiad.log preserved at %s\n", dst)
		}
	}

	if *freshFlag {
		fmt.Printf("[bench] --fresh: wiping data/ + clearing CSV history\n")
		os.RemoveAll(filepath.Join(*homeDir, "data"))
		os.MkdirAll(filepath.Join(*homeDir, "data"), 0755)
		os.WriteFile(filepath.Join(*homeDir, "data", "priv_validator_state.json"),
			[]byte(`{"height":"0","round":0,"step":0}`), 0644)
		// Wipe rotated logs + reset run number.
		for _, p := range prior {
			os.Remove(p)
		}
		runNum = 1
		// Clear CSV so resume logic starts fresh.
		os.Remove(csvPath)
	} else {
		fmt.Printf("[bench] resume mode (no --fresh) — keeping data/\n")
		if _, err := os.Stat(filepath.Join(*homeDir, "data")); err != nil {
			os.MkdirAll(filepath.Join(*homeDir, "data"), 0755)
			os.WriteFile(filepath.Join(*homeDir, "data", "priv_validator_state.json"),
				[]byte(`{"height":"0","round":0,"step":0}`), 0644)
		}
	}
	fmt.Printf("[bench] run #%d\n", runNum)

	// gaiad init if needed.
	if _, err := os.Stat(filepath.Join(*homeDir, "config")); err != nil {
		fmt.Printf("[bench] init gaiad home at %s\n", *homeDir)
		c := exec.Command(*gaiadPath, "init", "bench", "--chain-id="+*chainID, "--home="+*homeDir)
		c.Stdout = io.Discard
		c.Stderr = io.Discard
		if err := c.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "gaiad init: %v\n", err)
			os.Exit(1)
		}
	}

	// 2. Genesis
	mustCopy(*genesisPath, filepath.Join(*homeDir, "config", "genesis.json"))

	// 3. Trust hash
	fmt.Printf("[bench] fetching trust hash from %s ...\n", primary)
	tHeight, tHash, err := fetchTrust(primary, *trustOffset)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetch trust: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[bench] trust_height=%d trust_hash=%s\n", tHeight, tHash)

	// 4. app.toml min-gas-prices
	if err := setInSection(filepath.Join(*homeDir, "config", "app.toml"),
		"", map[string]string{"minimum-gas-prices": `"0.0025uatom"`}); err != nil {
		// app.toml min-gas-prices is at top-level (no [section]).
		fmt.Fprintf(os.Stderr, "set app.toml: %v\n", err)
		os.Exit(1)
	}

	// 5. config.toml [statesync]
	cfgPath := filepath.Join(*homeDir, "config", "config.toml")
	if err := setInSection(cfgPath, "[statesync]", map[string]string{
		"enable":       "true",
		"rpc_servers":  strconv.Quote(*rpcsFlag),
		"trust_height": strconv.FormatInt(tHeight, 10),
		"trust_hash":   strconv.Quote(tHash),
		"trust_period": `"168h0m0s"`,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "set [statesync]: %v\n", err)
		os.Exit(1)
	}

	// 6. Addrbook cache
	if *addrbookURL != "" {
		ab, err := cachedAddrbook(cacheDir, *addrbookURL, 6*time.Hour)
		if err != nil {
			fmt.Fprintf(os.Stderr, "addrbook: %v\n", err)
			os.Exit(1)
		}
		mustCopy(ab, filepath.Join(*homeDir, "config", "addrbook.json"))
	}

	// 7. peers file → persistent_peers
	peersList := ""
	if *peersFile != "" {
		var n int
		peersList, n, err = loadPeers(*peersFile, *peersLimit)
		if err != nil {
			fmt.Fprintf(os.Stderr, "peers: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("[bench] loaded %d peers from %s (limit %d)\n", n, *peersFile, *peersLimit)
	}

	// 8. config.toml [p2p]
	//
	// max_packet_msg_payload_size: cometbft defaults to 1024, which
	// is far too small for cosmoshub peers — many of them ship PEX
	// address responses or other internal messages in 10-100 KB
	// chunks. The default makes us drop the connection with
	// "message exceeds max size (10124 > 1034)" as soon as a real
	// peer interaction happens, then reconnect, then drop again,
	// then... that loop is exactly what we observed: 80 persistent
	// peers configured but only 2 connections holding. Our own
	// snapshot-fetch tool sets the same value (256 KB) for the same
	// reason; matching it here.
	p2p := map[string]string{
		"max_num_outbound_peers":      strconv.Itoa(*maxOutbound),
		"max_packet_msg_payload_size": "262144",
	}
	if peersList != "" {
		p2p["persistent_peers"] = strconv.Quote(peersList)
	}
	if err := setInSection(cfgPath, "[p2p]", p2p); err != nil {
		fmt.Fprintf(os.Stderr, "set [p2p]: %v\n", err)
		os.Exit(1)
	}

	// 9. Bench struct + CSV.
	csvFile, err := openCSV(csvPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "csv: %v\n", err)
		os.Exit(1)
	}
	bench := &Bench{
		homeDir:  *homeDir,
		benchDir: benchDir,
		csvPath:  csvPath,
		logPath:  logPath,
		runNum:   runNum,
		runStart: time.Now(),
		csvFile:  csvFile,
		csvW:     csv.NewWriter(csvFile),
		fired:    make(map[Phase]bool),
	}
	bench.lastEventTs.Store(time.Now().Unix())
	defer csvFile.Close()

	// 10. Replay phases from CSV (set fired + advance stage).
	if existing, err := readPhasesFromCSV(csvPath); err == nil {
		var keys []Phase
		for p := range existing {
			keys = append(keys, p)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		for _, p := range keys {
			bench.fired[p] = true
			fmt.Printf("[bench] resume: '%s' already recorded\n", phaseName(p))
			if s := int32(stageForPhase(p)); s > bench.stage.Load() {
				bench.stage.Store(s)
			}
		}
		if bench.fired[phaseCaughtUp] {
			fmt.Printf("[bench] resume: 'Node caught up' already recorded — nothing to do\n")
			return
		}
	}
	fmt.Printf("[bench] watcher initial stage=%d\n", bench.stage.Load())

	// 11. Launch gaiad with stdout/stderr → logPath
	gaiadOut, err := os.Create(logPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create log: %v\n", err)
		os.Exit(1)
	}
	defer gaiadOut.Close()

	fmt.Printf("\n[bench] launching gaiad — log=%s\n", logPath)
	cmd := exec.Command(*gaiadPath, "start", "--home="+*homeDir)
	cmd.Stdout = gaiadOut
	cmd.Stderr = gaiadOut
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "gaiad start: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[bench] gaiad pid=%d\n", cmd.Process.Pid)

	// 12. Goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bench.emit("polling http://127.0.0.1:%d/status every %s until caught up", *rpcPort, *pollInterval)

	// Two goroutines: log watcher + RPC poller. The dedicated
	// heartbeat goroutine was removed in favour of the RPC poll's
	// every-60s "peers: N connected" line, which already serves as
	// a steady "I'm alive" cadence without the quiet-noise.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); bench.watchLog(ctx) }()
	go func() { defer wg.Done(); bench.pollRPC(ctx, *rpcPort); cancel() }()

	// Forward signals → graceful shutdown of bench (gaiad keeps running).
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	gaiadDone := make(chan error, 1)
	go func() { gaiadDone <- cmd.Wait() }()

	select {
	case <-ctx.Done():
		// pollRPC saw caught_up=false → cancelled.
	case err := <-gaiadDone:
		bench.emit("!!! gaiad exited unexpectedly: %v", err)
		// Tail last 15 lines of log.
		tailLog(logPath, 15)
		os.Exit(1)
	case s := <-sigCh:
		bench.emit("received %v — leaving gaiad running, bench exiting", s)
	}

	cancel()
	wg.Wait()
	bench.emit("gaiad still running pid=%d — kill with: kill %d", cmd.Process.Pid, cmd.Process.Pid)
}

// ─── small utilities ──────────────────────────────────────────────────

func mustCopy(src, dst string) {
	in, err := os.Open(src)
	if err != nil {
		fmt.Fprintf(os.Stderr, "copy %s: %v\n", src, err)
		os.Exit(1)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", dst, err)
		os.Exit(1)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		fmt.Fprintf(os.Stderr, "copy contents: %v\n", err)
		os.Exit(1)
	}
}

// openCSV opens (creating if needed) the timing CSV and writes the
// header if the file is new/empty. Subsequent invocations append.
func openCSV(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	st, _ := f.Stat()
	if st.Size() == 0 {
		w := csv.NewWriter(f)
		if err := w.Write([]string{"phase", "wall_seconds", "height", "run", "note"}); err != nil {
			f.Close()
			return nil, err
		}
		w.Flush()
	}
	// Position for append.
	f.Seek(0, io.SeekEnd)
	return f, nil
}

func readPhasesFromCSV(path string) (map[Phase]bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	out := make(map[Phase]bool)
	first := true
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return out, err
		}
		if first {
			first = false
			continue
		}
		if len(row) == 0 {
			continue
		}
		if p, ok := phaseFromName(row[0]); ok {
			out[p] = true
		}
	}
	return out, nil
}

func tailLog(path string, n int) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	start := 0
	if len(lines) > n {
		start = len(lines) - n
	}
	for _, l := range lines[start:] {
		fmt.Println(l)
	}
}
