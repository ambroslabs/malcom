// cosmos-bootstrap-gaia assembles a complete gaia data directory from:
//   - an application.db produced by cosmos-snapshot-to-appdb
//   - a cometbft RPC (for state.db + blockstore.db via light-client bootstrap)
//   - a chain genesis.json
//
// What it writes:
//
//	<out>/config/genesis.json     copy of the supplied genesis
//	<out>/data/application.db/    hardlink/copy from <appdb>/application.db
//	<out>/data/state.db/          fresh, populated via cometbft's offline state-sync
//	<out>/data/blockstore.db/     fresh, holds the seen commit at H
//	<out>/data/wasm-payloads/     copy of extensions/, for follow-up placement
//
// What it does NOT do:
//   - install wasm contract bytecode under data/wasm/ (gaia-specific layout —
//     keep them at wasm-payloads/ for inspection; placement is a follow-up)
//   - write app.toml / config.toml (you set these yourself; see notes)
//   - run gaiad
//
// Usage:
//
//	cosmos-bootstrap-gaia \
//	  -appdb   /mnt/data/cosmos-archive/appdb-out/30936000 \
//	  -genesis /home/$USER/genesis.cosmoshub-4.json \
//	  -rpc     "https://cosmos-rpc.polkachu.com,https://rpc-cosmoshub.blockapsis.com" \
//	  -height  30936000 \
//	  -out     /mnt/data/cosmos-archive/gaia-bootstrap/30936000
package main

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
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/node"
)

func gzipNewReader(in []byte) (io.ReadCloser, error) {
	return gzip.NewReader(bytes.NewReader(in))
}

func sha256sum(b []byte) []byte {
	h := sha256.Sum256(b)
	return h[:]
}

func main() {
	appdb := flag.String("appdb", "", "directory containing application.db/ and extensions/ (output of cosmos-snapshot-to-appdb)")
	genesis := flag.String("genesis", "", "path to chain genesis.json")
	rpcStr := flag.String("rpc", "", "comma-separated cometbft RPC URLs (light client requires >= 2)")
	height := flag.Int64("height", 0, "snapshot height (must match application.db)")
	out := flag.String("out", "", "output gaia root directory (will contain config/ and data/)")
	trustHeight := flag.Int64("trust-height", 0, "trust height for light client (defaults to -height)")
	trustHashHex := flag.String("trust-hash", "", "trust block hash (hex) at -trust-height; auto-fetched from RPC if empty")
	trustPeriod := flag.Duration("trust-period", 30*24*time.Hour, "trust period for light client")
	overwrite := flag.Bool("overwrite", false, "wipe <out>/data/ before bootstrapping")
	writeConfigs := flag.Bool("write-configs", false, "write minimal app.toml/config.toml/client.toml under <out>/config/")
	moniker := flag.String("moniker", "bootstrap-node", "moniker for the node (when -write-configs)")
	appDBBackend := flag.String("app-db-backend", "pebbledb", "db_backend value for app.toml (must match application.db format)")
	cmtDBBackend := flag.String("cmt-db-backend", "goleveldb", "db_backend value for config.toml (cometbft state.db/blockstore.db)")
	placeWasm := flag.Bool("place-wasm", true, "extract wasm payloads to <out>/wasm/state/wasm/ (cosmwasm) and <out>/data/08-light-client/state/wasm/ (08-wasm)")
	flag.Parse()

	if *appdb == "" || *genesis == "" || *rpcStr == "" || *height == 0 || *out == "" {
		flag.Usage()
		os.Exit(2)
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

	// Lay out the gaia root.
	configDir := filepath.Join(*out, "config")
	dataDir := filepath.Join(*out, "data")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		log.Fatalf("mkdir config: %v", err)
	}
	if *overwrite {
		_ = os.RemoveAll(filepath.Join(dataDir, "state.db"))
		_ = os.RemoveAll(filepath.Join(dataDir, "blockstore.db"))
		_ = os.RemoveAll(filepath.Join(dataDir, "application.db"))
		_ = os.RemoveAll(filepath.Join(dataDir, "wasm-payloads"))
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Fatalf("mkdir data: %v", err)
	}

	// 1. Copy genesis.json into <out>/config/.
	gPath := filepath.Join(configDir, "genesis.json")
	fmt.Printf("[bootstrap] genesis  %s -> %s\n", *genesis, gPath)
	srcAbs, _ := filepath.Abs(*genesis)
	dstAbs, _ := filepath.Abs(gPath)
	if srcAbs != dstAbs {
		if err := copyFile(*genesis, gPath); err != nil {
			log.Fatalf("copy genesis: %v", err)
		}
	} else {
		fmt.Printf("[bootstrap] genesis: src == dst, skipping copy\n")
	}

	// 2. Resolve trust hash if missing.
	if *trustHashHex == "" {
		fmt.Printf("[bootstrap] fetching trust hash from %s at height %d\n", rpcs[0], *trustHeight)
		bh, err := fetchBlockHash(rpcs[0], *trustHeight)
		if err != nil {
			log.Fatalf("fetch trust hash: %v", err)
		}
		*trustHashHex = bh
	}
	fmt.Printf("[bootstrap] trust    height=%d hash=%s\n", *trustHeight, *trustHashHex)

	// 3. Fetch the appHash for safety: state after block H is in block H+1's
	//    AppHash field.
	appHashHex, err := fetchAppHash(rpcs[0], *height+1)
	if err != nil {
		log.Fatalf("fetch app hash: %v", err)
	}
	appHash, err := hex.DecodeString(appHashHex)
	if err != nil {
		log.Fatalf("decode app hash: %v", err)
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
	// We're not actually enabling state-sync at runtime; we're just using
	// node.BootstrapState which reads StateSync for trust point + RPCs.
	c.StateSync.Enable = false
	// Storage: keep ABCI responses (default). DiscardABCIResponses=true breaks
	// historical queries but is fine for sync nodes.

	fmt.Printf("[bootstrap] running cometbft offline state-sync bootstrap (height=%d)...\n", *height)
	t0 := time.Now()
	if err := node.BootstrapState(context.Background(), c, cfg.DefaultDBProvider, uint64(*height), appHash); err != nil {
		log.Fatalf("BootstrapState: %v", err)
	}
	fmt.Printf("[bootstrap] cometbft bootstrap done in %s\n", time.Since(t0).Truncate(time.Second))

	// 5. Place application.db (hardlink-tree if same fs, copy otherwise).
	srcApp := filepath.Join(*appdb, "application.db")
	dstApp := filepath.Join(dataDir, "application.db")
	fmt.Printf("[bootstrap] application.db %s -> %s\n", srcApp, dstApp)
	t0 = time.Now()
	if err := cloneTree(srcApp, dstApp); err != nil {
		log.Fatalf("place application.db: %v", err)
	}
	fmt.Printf("[bootstrap] application.db placed in %s\n", time.Since(t0).Truncate(time.Millisecond))

	// 6. Place wasm extension payloads.
	srcExt := filepath.Join(*appdb, "extensions")
	if _, err := os.Stat(srcExt); err == nil {
		// Park the raw payloads for transparency.
		dstExt := filepath.Join(dataDir, "wasm-payloads")
		fmt.Printf("[bootstrap] extensions  %s -> %s\n", srcExt, dstExt)
		if err := cloneTree(srcExt, dstExt); err != nil {
			log.Fatalf("place extensions: %v", err)
		}
		if *placeWasm {
			// Extract to wasmvm's expected layout.
			if err := placeWasmPayloads(srcExt, *out); err != nil {
				log.Fatalf("place wasm payloads: %v", err)
			}
		}
	}

	// 7. Optionally write minimal config files.
	if *writeConfigs {
		if err := writeConfigFiles(*out, *moniker, *appDBBackend, *cmtDBBackend); err != nil {
			log.Fatalf("write configs: %v", err)
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

// placeWasmPayloads ungzips each snapshot extension payload, sha256s the raw
// wasm bytes to get the checksum, and writes the bytecode at the path
// wasmvm expects:
//
//	<root>/wasm/state/wasm/<hex_checksum>                  (cosmwasm — wasmd's BaseDir is <homePath>/wasm)
//	<root>/data/08-light-client/state/wasm/<hex_checksum>  (IBC 08-wasm)
//
// wasmvm compiles on first invocation, so we only place raw bytecode.
func placeWasmPayloads(extDir, gaiaRoot string) error {
	// cosmwasm "wasm" extension → BaseDir = <gaiaRoot>/wasm
	if err := placeOneExt(filepath.Join(extDir, "wasm"), filepath.Join(gaiaRoot, "wasm", "state", "wasm")); err != nil {
		return fmt.Errorf("wasm extension: %w", err)
	}
	// IBC "08-wasm" extension → BaseDir = <gaiaRoot>/data/08-light-client
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
		return nil // no payloads
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
	r, err := gzipNewReader(in)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// writeConfigFiles drops the minimum config files gaiad needs to start: a
// cometbft config.toml, a cosmos-sdk app.toml, and a client.toml. Values
// are tuned for an offline-state-sync bootstrap scenario (state-sync
// disabled, block-sync enabled, sensible mempool/peer defaults).
func writeConfigFiles(root, moniker, appDB, cmtDB string) error {
	cfgDir := filepath.Join(root, "config")
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(cfgDir, "config.toml"),
		[]byte(renderConfigTOML(moniker, cmtDB)), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(cfgDir, "app.toml"),
		[]byte(renderAppTOML(appDB)), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(cfgDir, "client.toml"),
		[]byte(renderClientTOML()), 0o644); err != nil {
		return err
	}
	return nil
}

func renderConfigTOML(moniker, dbBackend string) string {
	return fmt.Sprintf(`# Generated by cosmos-bootstrap-gaia. Minimal cometbft config.
proxy_app = "tcp://127.0.0.1:26658"
moniker = %q
db_backend = %q
db_dir = "data"
log_level = "info"
log_format = "plain"
genesis_file = "config/genesis.json"
priv_validator_key_file = "config/priv_validator_key.json"
priv_validator_state_file = "data/priv_validator_state.json"
priv_validator_laddr = ""
node_key_file = "config/node_key.json"
abci = "socket"
filter_peers = false

[rpc]
laddr = "tcp://127.0.0.1:26657"
cors_allowed_origins = []
cors_allowed_methods = ["HEAD", "GET", "POST"]
cors_allowed_headers = ["Origin", "Accept", "Content-Type", "X-Requested-With", "X-Server-Time"]
grpc_laddr = ""
grpc_max_open_connections = 900
unsafe = false
max_open_connections = 900
max_subscription_clients = 100
max_subscriptions_per_client = 5
experimental_subscription_buffer_size = 200
experimental_websocket_write_buffer_size = 200
experimental_close_on_slow_client = false
timeout_broadcast_tx_commit = "10s"
max_request_batch_size = 10
max_body_bytes = 1000000
max_header_bytes = 1048576
tls_cert_file = ""
tls_key_file = ""
pprof_laddr = ""

[p2p]
laddr = "tcp://0.0.0.0:26656"
external_address = ""
seeds = ""
persistent_peers = ""
addr_book_file = "config/addrbook.json"
addr_book_strict = true
max_num_inbound_peers = 40
max_num_outbound_peers = 10
unconditional_peer_ids = ""
persistent_peers_max_dial_period = "0s"
flush_throttle_timeout = "100ms"
max_packet_msg_payload_size = 1024
send_rate = 5120000
recv_rate = 5120000
pex = true
seed_mode = false
private_peer_ids = ""
allow_duplicate_ip = false
handshake_timeout = "20s"
dial_timeout = "3s"

[mempool]
type = "flood"
recheck = true
recheck_timeout = "1s"
broadcast = true
wal_dir = ""
size = 5000
max_txs_bytes = 1073741824
cache_size = 10000
keep-invalid-txs-in-cache = false
max_tx_bytes = 1048576
max_batch_bytes = 0
experimental_max_gossip_connections_to_persistent_peers = 0
experimental_max_gossip_connections_to_non_persistent_peers = 0

[statesync]
enable = false
rpc_servers = ""
trust_height = 0
trust_hash = ""
trust_period = "168h0m0s"
discovery_time = "15s"
temp_dir = ""
chunk_request_timeout = "10s"
chunk_fetchers = "4"

[blocksync]
version = "v0"

[consensus]
wal_file = "data/cs.wal/wal"
timeout_propose = "3s"
timeout_propose_delta = "500ms"
timeout_prevote = "1s"
timeout_prevote_delta = "500ms"
timeout_precommit = "1s"
timeout_precommit_delta = "500ms"
timeout_commit = "5s"
double_sign_check_height = 0
skip_timeout_commit = false
create_empty_blocks = true
create_empty_blocks_interval = "0s"
peer_gossip_sleep_duration = "100ms"
peer_query_maj23_sleep_duration = "2s"

[storage]
discard_abci_responses = false

[tx_index]
indexer = "null"

[instrumentation]
prometheus = false
prometheus_listen_addr = ":26660"
max_open_connections = 3
namespace = "cometbft"
`, moniker, dbBackend)
}

func renderAppTOML(dbBackend string) string {
	return fmt.Sprintf(`# Generated by cosmos-bootstrap-gaia. Minimal cosmos-sdk app config.

###############################################################################
###                           Base Configuration                            ###
###############################################################################

minimum-gas-prices = "0.0025uatom"
pruning = "default"
pruning-keep-recent = "0"
pruning-interval = "0"
halt-height = 0
halt-time = 0
min-retain-blocks = 0
inter-block-cache = true
index-events = []
iavl-cache-size = 781250
iavl-disable-fastnode = false
app-db-backend = %q

###############################################################################
###                         Telemetry Configuration                         ###
###############################################################################

[telemetry]
service-name = ""
enabled = false
enable-hostname = false
enable-hostname-label = false
enable-service-label = false
prometheus-retention-time = 0
global-labels = []

###############################################################################
###                           API Configuration                             ###
###############################################################################

[api]
enable = false
swagger = false
address = "tcp://localhost:1317"
max-open-connections = 1000
rpc-read-timeout = 10
rpc-write-timeout = 0
rpc-max-body-bytes = 1000000
enabled-unsafe-cors = false

###############################################################################
###                           gRPC Configuration                            ###
###############################################################################

[grpc]
enable = true
address = "localhost:9090"
max-recv-msg-size = "10485760"
max-send-msg-size = "2147483647"

[grpc-web]
enable = true

###############################################################################
###                         State Sync Configuration                        ###
###############################################################################

[state-sync]
snapshot-interval = 0
snapshot-keep-recent = 2

###############################################################################
###                              Wasm                                       ###
###############################################################################

[wasm]
simulation_gas_limit = ""
query_gas_limit = 3000000
memory_cache_size = 100
contract_debug_mode = false
`, dbBackend)
}

func renderClientTOML() string {
	return `# Generated by cosmos-bootstrap-gaia. Minimal client config.
chain-id = ""
keyring-backend = "test"
output = "text"
node = "tcp://localhost:26657"
broadcast-mode = "sync"
`
}

// cloneTree mirrors srcDir to dstDir, hardlinking files when the source and
// destination share a filesystem (instant + zero extra disk), and falling
// back to a real copy across filesystem boundaries.
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
		// Try hardlink first.
		if err := os.Link(path, dst); err == nil {
			return nil
		}
		// Fall back to copy.
		return copyFile(path, dst)
	})
}
