// Package bootstrap is the `malcom bootstrap` subcommand: assemble a
// complete gaiad home directory from:
//   - an application.db produced by `malcom snapshot import`
//   - a cometbft RPC (for state.db + blockstore.db via offline state-sync)
//   - a chain genesis.json
//
// Output: <-out>/gaia_<chain>_<height>/{config,data}/. Default -out is
// the current working directory.
//
// Tuning (trust period, wasm placement, db backends, moniker) lives
// in the [chains.<id>.bootstrap] section of config.toml.
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
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/node"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
)

// Run is the malcom subcommand entry point. Returns the process exit
// code (0 on success).
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom bootstrap", flag.ContinueOnError)
	chain := fs.String("chain", "", "chain id (required; must have been added with `malcom add <chain-id>`)")
	appdb := fs.String("appdb", "", "directory containing application.db/ and extensions/ (output of `malcom snapshot import`)")
	height := fs.Int64("height", 0, "snapshot height (must match application.db)")
	out := fs.String("out", ".", "parent dir for the gaia home (subdir gaia_<chain>_<height>/ created inside)")
	trustHeight := fs.Int64("trust-height", 0, "trust height for light client (defaults to -height)")
	trustHashHex := fs.String("trust-hash", "", "trust block hash (hex) at -trust-height; auto-fetched from RPC if empty")
	overwrite := fs.Bool("overwrite", false, "wipe gaia home's data/ before bootstrapping")
	skipAppCopy := fs.Bool("skip-app-copy", false, "skip cloning <appdb>/application.db into the gaia home; assume it's already there")
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	debug := fs.Bool("debug", false, "verbose logging")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return 2
	}
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := malcomlog.New(malcomlog.Options{
		Writer: os.Stderr, Mode: mode, Level: level,
	}).With("module", "bootstrap")

	if *chain == "" {
		log.Error("required: -chain <id>")
		return 2
	}
	cfgFile, err := config.Load()
	if err != nil {
		log.Error("config load", "err", err)
		return 1
	}
	ch, err := cfgFile.Resolve(*chain)
	if err != nil {
		log.Error("resolve chain", "err", err, "chain", *chain)
		return 1
	}

	if *appdb == "" || *height == 0 {
		log.Error("required: -appdb -height")
		return 2
	}
	genesis, err := resolveGenesis(ch, log)
	if err != nil {
		log.Error("resolve genesis",
			"err", err, "chain", ch.ChainID, "config", cfgFile.Path(),
			"hint", fmt.Sprintf("set chains.%s.genesis in %s", ch.ChainID, cfgFile.Path()))
		return 1
	}
	if len(ch.RPCs) == 0 {
		log.Error("chains.<id>.rpcs is empty", "chain", ch.ChainID, "config", cfgFile.Path())
		return 1
	}

	rpcs := append([]string(nil), ch.RPCs...)
	if len(rpcs) == 1 {
		// cometbft's light client wants at least 2 (1 primary + 1 witness).
		// Duplicate the single URL — works in practice for our use case
		// where we trust the operator's RPC choice.
		rpcs = append(rpcs, rpcs[0])
		log.Warn("only 1 RPC URL configured; duplicating for cometbft light-client (requires >=2)")
	}

	outRoot := filepath.Join(*out, fmt.Sprintf("gaia_%s_%d", ch.ChainID, *height))
	trustPeriod := ch.Bootstrap.TrustPeriod.Duration()
	moniker := ch.Bootstrap.Moniker
	appDBBackend := ch.Bootstrap.AppDBBackend
	cmtDBBackend := ch.Bootstrap.CmtDBBackend
	placeWasmFlag := ch.Bootstrap.PlaceWasm
	writeConfigsFlag := ch.Bootstrap.WriteConfigs

	log.Info("starting",
		"config", cfgFile.Path(),
		"chain", ch.ChainID,
		"out", outRoot,
		"appdb", *appdb,
		"height", *height)

	if *trustHeight == 0 {
		*trustHeight = *height
	}

	configDir := filepath.Join(outRoot, "config")
	dataDir := filepath.Join(outRoot, "data")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		log.Error("mkdir config failed", "err", err)
		return 1
	}
	if *overwrite {
		_ = os.RemoveAll(filepath.Join(dataDir, "state.db"))
		_ = os.RemoveAll(filepath.Join(dataDir, "blockstore.db"))
		_ = os.RemoveAll(filepath.Join(dataDir, "application.db"))
		_ = os.RemoveAll(filepath.Join(dataDir, "wasm-payloads"))
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Error("mkdir data failed", "err", err)
		return 1
	}

	// 1. Copy genesis.json into <out>/config/.
	gPath := filepath.Join(configDir, "genesis.json")
	log.Info("genesis copy", "src", genesis, "dst", gPath)
	srcAbs, _ := filepath.Abs(genesis)
	dstAbs, _ := filepath.Abs(gPath)
	if srcAbs != dstAbs {
		if err := copyFile(genesis, gPath); err != nil {
			log.Error("copy genesis failed", "err", err)
			return 1
		}
	} else {
		log.Info("genesis: src == dst, skipping copy")
	}

	// 2. Resolve trust hash if missing.
	if *trustHashHex == "" {
		log.Info("fetching trust hash", "rpc", rpcs[0], "height", *trustHeight)
		bh, err := fetchBlockHash(rpcs[0], *trustHeight)
		if err != nil {
			log.Error("fetch trust hash failed", "err", err)
			return 1
		}
		*trustHashHex = bh
	}
	log.Info("trust", "height", *trustHeight, "hash", *trustHashHex)

	// 3. Fetch the appHash for safety: state after block H is in block H+1's
	//    AppHash field.
	appHashHex, err := fetchAppHash(rpcs[0], *height+1)
	if err != nil {
		log.Error("fetch app hash failed", "err", err)
		return 1
	}
	appHash, err := hex.DecodeString(appHashHex)
	if err != nil {
		log.Error("decode app hash failed", "err", err)
		return 1
	}
	log.Info("apphash", "apphash", appHashHex, "after_block", *height)

	// 4. Build cometbft config rooted at <out>.
	c := cfg.DefaultConfig()
	c.SetRoot(outRoot)
	c.DBBackend = "goleveldb"
	c.Genesis = "config/genesis.json"
	c.StateSync.RPCServers = rpcs
	c.StateSync.TrustHeight = *trustHeight
	c.StateSync.TrustHash = *trustHashHex
	c.StateSync.TrustPeriod = trustPeriod
	c.StateSync.Enable = false

	log.Info("running cometbft offline state-sync bootstrap", "height", *height)
	t0 := time.Now()
	if err := node.BootstrapState(context.Background(), c, cfg.DefaultDBProvider, uint64(*height), appHash); err != nil {
		log.Error("BootstrapState failed", "err", err)
		return 1
	}
	log.Info("cometbft bootstrap done", "elapsed", time.Since(t0).Truncate(time.Second))

	// 5. Copy application.db into the gaia data dir as an independent
	// tree so the source stays untouched across reruns. See cloneTree
	// for why we don't hardlink.
	srcApp := filepath.Join(*appdb, "application.db")
	dstApp := filepath.Join(dataDir, "application.db")
	if *skipAppCopy {
		log.Info("application.db: -skip-app-copy set; expecting it in place", "path", dstApp)
		if _, err := os.Stat(dstApp); err != nil {
			log.Error("skip-app-copy target missing", "path", dstApp, "err", err)
			return 1
		}
	} else {
		log.Info("application.db copy", "src", srcApp, "dst", dstApp)
		t0 = time.Now()
		if err := cloneTree(srcApp, dstApp); err != nil {
			log.Error("place application.db failed", "err", err)
			return 1
		}
		log.Info("application.db placed", "elapsed", time.Since(t0).Truncate(time.Millisecond))
	}

	// 6. Place wasm extension payloads.
	srcExt := filepath.Join(*appdb, "extensions")
	if _, err := os.Stat(srcExt); err == nil {
		dstExt := filepath.Join(dataDir, "wasm-payloads")
		log.Info("extensions copy", "src", srcExt, "dst", dstExt)
		if err := cloneTree(srcExt, dstExt); err != nil {
			log.Error("place extensions failed", "err", err)
			return 1
		}
		if placeWasmFlag {
			if err := placeWasmPayloads(srcExt, outRoot, log); err != nil {
				log.Error("place wasm payloads failed", "err", err)
				return 1
			}
		}
	}

	// 7. Optionally write minimal config files.
	if writeConfigsFlag {
		if err := writeConfigFiles(outRoot, moniker, appDBBackend, cmtDBBackend); err != nil {
			log.Error("write configs failed", "err", err)
			return 1
		}
		log.Info("config files written", "dir", filepath.Join(outRoot, "config"))
	}

	log.Info("done",
		"genesis", filepath.Join(outRoot, "config/genesis.json"),
		"appdb", filepath.Join(outRoot, "data/application.db"),
		"state_db", filepath.Join(outRoot, "data/state.db"),
		"blockstore_db", filepath.Join(outRoot, "data/blockstore.db"),
		"wasm_payloads", filepath.Join(outRoot, "data/wasm-payloads"),
		"height", *height)
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
func placeWasmPayloads(extDir, gaiaRoot string, log *slog.Logger) error {
	if err := placeOneExt(filepath.Join(extDir, "wasm"), filepath.Join(gaiaRoot, "wasm", "state", "wasm"), log); err != nil {
		return fmt.Errorf("wasm extension: %w", err)
	}
	if _, err := os.Stat(filepath.Join(extDir, "08-wasm")); err == nil {
		dst := filepath.Join(gaiaRoot, "data", "08-light-client", "state", "wasm")
		if err := placeOneExt(filepath.Join(extDir, "08-wasm"), dst, log); err != nil {
			return fmt.Errorf("08-wasm extension: %w", err)
		}
	}
	return nil
}

func placeOneExt(srcDir, dstDir string, log *slog.Logger) error {
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
	log.Info("placed wasm bytecode files", "count", count, "dir", dstDir)
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
