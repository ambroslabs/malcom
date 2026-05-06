// Package bootstrap is the `malcom bootstrap` subcommand: assemble a
// complete gaiad home directory from:
//   - an application.db produced by `malcom snapshot import`
//   - a cometbft RPC (for state.db + blockstore.db via offline state-sync)
//   - a chain genesis.json
//
// What it writes:
//
//	<out>/config/genesis.json     copy of the supplied genesis
//	<out>/data/application.db/    independent copy of <appdb>/application.db
//	<out>/data/state.db/          fresh, populated via cometbft offline state-sync
//	<out>/data/blockstore.db/     fresh, holds the seen commit at H
//	<out>/data/wasm-payloads/     copy of extensions/, for follow-up placement
//
// What it does NOT do:
//   - install wasm contract bytecode under data/wasm/ (gaia-specific layout —
//     keep them at wasm-payloads/ for inspection; -place-wasm handles this)
//   - run gaiad
package bootstrap

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/node"
)

// Run is the malcom subcommand entry point. Returns the process exit
// code (0 on success).
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom bootstrap", flag.ContinueOnError)
	appdb := fs.String("appdb", "", "directory containing application.db/ and extensions/ (output of `malcom snapshot import`)")
	genesis := fs.String("genesis", "", "path to chain genesis.json")
	rpcStr := fs.String("rpc", "", "comma-separated cometbft RPC URLs (light client requires >= 2)")
	height := fs.Int64("height", 0, "snapshot height (must match application.db)")
	out := fs.String("out", "", "output gaia root directory (will contain config/ and data/)")
	trustHeight := fs.Int64("trust-height", 0, "trust height for light client (defaults to -height)")
	trustHashHex := fs.String("trust-hash", "", "trust block hash (hex) at -trust-height; auto-fetched from RPC if empty")
	trustPeriod := fs.Duration("trust-period", 30*24*time.Hour, "trust period for light client")
	overwrite := fs.Bool("overwrite", false, "wipe <out>/data/ before bootstrapping")
	writeConfigs := fs.Bool("write-configs", false, "write minimal app.toml/config.toml/client.toml under <out>/config/")
	moniker := fs.String("moniker", "bootstrap-node", "moniker for the node (when -write-configs)")
	appDBBackend := fs.String("app-db-backend", "pebbledb", "db_backend value for app.toml (must match application.db format)")
	cmtDBBackend := fs.String("cmt-db-backend", "goleveldb", "db_backend value for config.toml (cometbft state.db/blockstore.db)")
	placeWasm := fs.Bool("place-wasm", true, "extract wasm payloads to <out>/wasm/state/wasm/ (cosmwasm) and <out>/data/08-light-client/state/wasm/ (08-wasm)")
	skipAppCopy := fs.Bool("skip-app-copy", false, "skip cloning <appdb>/application.db into <out>/data/application.db. Use when you've already written application.db directly to the destination")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *appdb == "" || *genesis == "" || *rpcStr == "" || *height == 0 || *out == "" {
		fs.Usage()
		return 2
	}

	rpcs := strings.Split(*rpcStr, ",")
	for i := range rpcs {
		rpcs[i] = strings.TrimSpace(rpcs[i])
	}
	if len(rpcs) == 1 {
		// cometbft's light client wants at least 2 (1 primary + 1 witness).
		// Duplicate the single URL — works in practice for our use case
		// where we trust the operator's RPC choice.
		rpcs = append(rpcs, rpcs[0])
		fmt.Fprintln(os.Stderr, "[bootstrap] note: only 1 RPC URL provided; duplicating for cometbft light-client (it requires >=2)")
	}

	if *trustHeight == 0 {
		*trustHeight = *height
	}

	configDir := filepath.Join(*out, "config")
	dataDir := filepath.Join(*out, "data")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir config: %v\n", err)
		return 1
	}
	if *overwrite {
		_ = os.RemoveAll(filepath.Join(dataDir, "state.db"))
		_ = os.RemoveAll(filepath.Join(dataDir, "blockstore.db"))
		_ = os.RemoveAll(filepath.Join(dataDir, "application.db"))
		_ = os.RemoveAll(filepath.Join(dataDir, "wasm-payloads"))
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir data: %v\n", err)
		return 1
	}

	// 1. Copy genesis.json into <out>/config/.
	gPath := filepath.Join(configDir, "genesis.json")
	fmt.Printf("[bootstrap] genesis  %s -> %s\n", *genesis, gPath)
	srcAbs, _ := filepath.Abs(*genesis)
	dstAbs, _ := filepath.Abs(gPath)
	if srcAbs != dstAbs {
		if err := copyFile(*genesis, gPath); err != nil {
			fmt.Fprintf(os.Stderr, "copy genesis: %v\n", err)
			return 1
		}
	} else {
		fmt.Printf("[bootstrap] genesis: src == dst, skipping copy\n")
	}

	// 2. Resolve trust hash if missing.
	if *trustHashHex == "" {
		fmt.Printf("[bootstrap] fetching trust hash from %s at height %d\n", rpcs[0], *trustHeight)
		bh, err := fetchBlockHash(rpcs[0], *trustHeight)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fetch trust hash: %v\n", err)
			return 1
		}
		*trustHashHex = bh
	}
	fmt.Printf("[bootstrap] trust    height=%d hash=%s\n", *trustHeight, *trustHashHex)

	// 3. Fetch the appHash for safety: state after block H is in block H+1's
	//    AppHash field.
	appHashHex, err := fetchAppHash(rpcs[0], *height+1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "fetch app hash: %v\n", err)
		return 1
	}
	appHash, err := hex.DecodeString(appHashHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "decode app hash: %v\n", err)
		return 1
	}
	fmt.Printf("[bootstrap] AppHash  %s (state after block %d)\n", appHashHex, *height)

	// 4. Build cometbft config rooted at <out>.
	c := cfg.DefaultConfig()
	c.SetRoot(*out)
	c.DBBackend = "goleveldb"
	c.Genesis = "config/genesis.json"
	c.StateSync.RPCServers = rpcs
	c.StateSync.TrustHeight = *trustHeight
	c.StateSync.TrustHash = *trustHashHex
	c.StateSync.TrustPeriod = *trustPeriod
	c.StateSync.Enable = false

	fmt.Printf("[bootstrap] running cometbft offline state-sync bootstrap (height=%d)...\n", *height)
	t0 := time.Now()
	if err := node.BootstrapState(context.Background(), c, cfg.DefaultDBProvider, uint64(*height), appHash); err != nil {
		fmt.Fprintf(os.Stderr, "BootstrapState: %v\n", err)
		return 1
	}
	fmt.Printf("[bootstrap] cometbft bootstrap done in %s\n", time.Since(t0).Truncate(time.Second))

	// 5. Copy application.db into the gaia data dir as an independent
	// tree so the source stays untouched across reruns. See cloneTree
	// for why we don't hardlink.
	srcApp := filepath.Join(*appdb, "application.db")
	dstApp := filepath.Join(dataDir, "application.db")
	if *skipAppCopy {
		fmt.Printf("[bootstrap] application.db: -skip-app-copy set; expecting it at %s\n", dstApp)
		if _, err := os.Stat(dstApp); err != nil {
			fmt.Fprintf(os.Stderr, "skip-app-copy: %s missing: %v\n", dstApp, err)
			return 1
		}
	} else {
		fmt.Printf("[bootstrap] application.db %s -> %s\n", srcApp, dstApp)
		t0 = time.Now()
		if err := cloneTree(srcApp, dstApp); err != nil {
			fmt.Fprintf(os.Stderr, "place application.db: %v\n", err)
			return 1
		}
		fmt.Printf("[bootstrap] application.db placed in %s\n", time.Since(t0).Truncate(time.Millisecond))
	}

	// 6. Place wasm extension payloads.
	srcExt := filepath.Join(*appdb, "extensions")
	if _, err := os.Stat(srcExt); err == nil {
		dstExt := filepath.Join(dataDir, "wasm-payloads")
		fmt.Printf("[bootstrap] extensions  %s -> %s\n", srcExt, dstExt)
		if err := cloneTree(srcExt, dstExt); err != nil {
			fmt.Fprintf(os.Stderr, "place extensions: %v\n", err)
			return 1
		}
		if *placeWasm {
			if err := placeWasmPayloads(srcExt, *out); err != nil {
				fmt.Fprintf(os.Stderr, "place wasm payloads: %v\n", err)
				return 1
			}
		}
	}

	// 7. Optionally write minimal config files.
	if *writeConfigs {
		if err := writeConfigFiles(*out, *moniker, *appDBBackend, *cmtDBBackend); err != nil {
			fmt.Fprintf(os.Stderr, "write configs: %v\n", err)
			return 1
		}
		fmt.Printf("[bootstrap] wrote %s/config/{app,config,client}.toml\n", *out)
	}

	fmt.Println()
	fmt.Println("[bootstrap] done. layout:")
	fmt.Printf("  %s/config/genesis.json\n", *out)
	fmt.Printf("  %s/data/application.db/   (from %s)\n", *out, srcApp)
	fmt.Printf("  %s/data/state.db/         (fresh, height=%d)\n", *out, *height)
	fmt.Printf("  %s/data/blockstore.db/    (seen commit at %d, offline-sync height set)\n", *out, *height)
	fmt.Printf("  %s/data/wasm-payloads/    (parking — install under data/wasm/ when you wire up gaiad)\n", *out)
	return 0
}

// ─── RPC helpers ─────────────────────────────────────────────────────────

func fetchAppHash(rpcBase string, height int64) (string, error) {
	res, err := rpcGet(rpcBase, "/commit", map[string]string{"height": fmt.Sprintf("%d", height)})
	if err != nil {
		return "", err
	}
	v, _ := drill(res, "result", "signed_header", "header", "app_hash")
	s, _ := v.(string)
	if s == "" {
		return "", fmt.Errorf("no app_hash in /commit?height=%d", height)
	}
	return s, nil
}

func fetchBlockHash(rpcBase string, height int64) (string, error) {
	res, err := rpcGet(rpcBase, "/commit", map[string]string{"height": fmt.Sprintf("%d", height)})
	if err != nil {
		return "", err
	}
	v, _ := drill(res, "result", "signed_header", "commit", "block_id", "hash")
	s, _ := v.(string)
	if s == "" {
		return "", fmt.Errorf("no block hash in /commit?height=%d", height)
	}
	return s, nil
}

func rpcGet(base, path string, params map[string]string) (map[string]interface{}, error) {
	q := ""
	first := true
	for k, v := range params {
		if first {
			q = "?"
			first = false
		} else {
			q += "&"
		}
		q += k + "=" + v
	}
	url := strings.TrimRight(base, "/") + path + q
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}
	var raw map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func drill(m map[string]interface{}, keys ...string) (interface{}, bool) {
	var cur interface{} = m
	for _, k := range keys {
		mm, ok := cur.(map[string]interface{})
		if !ok {
			return nil, false
		}
		cur, ok = mm[k]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// ─── filesystem helpers ──────────────────────────────────────────────────

func copyFile(src, dst string) error {
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

func sha256sum(b []byte) []byte {
	h := sha256.Sum256(b)
	return h[:]
}

// placeWasmPayloads ungzips each snapshot extension payload, sha256s the raw
// wasm bytes to get the checksum, and writes the bytecode at the path
// wasmvm expects:
//
//	<root>/wasm/state/wasm/<hex_checksum>                  (cosmwasm — wasmd's BaseDir is <homePath>/wasm)
//	<root>/data/08-light-client/state/wasm/<hex_checksum>  (IBC 08-wasm)
//
// wasmvm compiles on first invocation, so we only place raw bytecode.
func placeWasmPayloads(extDir, gaiaRoot string) error {
	if err := placeOneExt(filepath.Join(extDir, "wasm"), filepath.Join(gaiaRoot, "wasm", "state", "wasm")); err != nil {
		return fmt.Errorf("wasm extension: %w", err)
	}
	if _, err := os.Stat(filepath.Join(extDir, "08-wasm")); err == nil {
		dst := filepath.Join(gaiaRoot, "data", "08-light-client", "state", "wasm")
		if err := placeOneExt(filepath.Join(extDir, "08-wasm"), dst); err != nil {
			return fmt.Errorf("08-wasm extension: %w", err)
		}
	}
	return nil
}

func placeOneExt(srcDir, dstDir string) error {
	if _, err := os.Stat(srcDir); err != nil {
		return nil
	}
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return err
	}
	count := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "payload-") {
			continue
		}
		gz, err := os.ReadFile(filepath.Join(srcDir, e.Name()))
		if err != nil {
			return err
		}
		raw, err := gunzip(gz)
		if err != nil {
			return fmt.Errorf("gunzip %s: %w", e.Name(), err)
		}
		sum := sha256sum(raw)
		dstPath := filepath.Join(dstDir, hex.EncodeToString(sum))
		if err := os.WriteFile(dstPath, raw, 0o644); err != nil {
			return err
		}
		count++
	}
	fmt.Printf("[bootstrap] placed %d wasm bytecode files in %s\n", count, dstDir)
	return nil
}

func gunzip(in []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// cloneTree mirrors srcDir to dstDir as an independent on-disk copy.
//
// We don't hardlink, even on the same filesystem. Hardlinking is faster
// (instant) and would otherwise be the obvious choice for a write-once
// store like pebble, but it has two real downsides:
//
//   - flock aliases. Pebble's LOCK file would be a single inode shared
//     between src and dst, so gaiad's flock at runtime blocks any
//     tooling running against the source dir (and vice versa).
//
//   - lifetime entanglement. gaiad's pebble obsoletes files via
//     unlink, which only decrements link count; the source dir's
//     hardlinked names keep them alive. So src stays valid in
//     practice, but its files are owned by the dst's runtime — if dst
//     ever unlinks the last surviving name, src loses data. Awkward
//     for a "pristine reference copy" intended to be reused.
//
// Cost on local-attached storage is ~1.5 min for a 14 GB pebble dir,
// which is small relative to the rest of the snapshot→gaiad pipeline.
func cloneTree(srcDir, dstDir string) error {
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		dst := filepath.Join(dstDir, rel)
		if info.IsDir() {
			return os.MkdirAll(dst, info.Mode())
		}
		return copyFile(path, dst)
	})
}
