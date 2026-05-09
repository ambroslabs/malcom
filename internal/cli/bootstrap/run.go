// Package bootstrap is the `malcom bootstrap` subcommand: assemble a
// runnable chain home directory from:
//   - an application.db produced by `malcom snapshot import`
//   - a chain binary (operator-supplied, used for `init` and
//     `tendermint/comet bootstrap-state`)
//   - a cometbft RPC (for the trust hash + state.db population)
//
// Bootstrap orchestrates subprocess invocations of the chain binary
// rather than re-implementing chain-specific logic in malcom — that
// keeps the subcommand chain-agnostic. The hand-rolled config
// templates that used to live here are gone; the binary writes its
// own config.toml/app.toml/client.toml via `init`, and malcom only
// patches a small set of fields after the fact.
//
// Output: <-out>/home_<chain>_<height>/{config,data}/. Default -out
// is the current working directory.
package bootstrap

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/registry"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotimport"
)

// Run is the malcom subcommand entry point. Returns the process exit
// code (0 on success).
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom bootstrap", flag.ContinueOnError)
	chain := fs.String("chain", "", "chain id (override; required if appdb meta.json is missing or omits chain_id)")
	appdb := fs.String("appdb", "", "directory containing application.db/ and extensions/ (output of `malcom snapshot import`)")
	height := fs.Int64("height", 0, "height override (required if appdb meta.json is missing or omits height)")
	out := fs.String("out", ".", "parent dir for the chain home (subdir home_<chain>_<height>/ created inside)")
	binary := fs.String("binary", "", "path to chain binary; default = $PATH lookup of daemon_name from chain-registry")
	appStrategy := fs.String("app-strategy", "copy", "how to place application.db: copy|move (move is rename(2), same-fs only)")
	moniker := fs.String("moniker", "", "moniker passed to <chain-exe> init (default = [chains.<id>.bootstrap].moniker, then 'malcom-bootstrap')")
	minGasPrices := fs.String("minimum-gas-prices", "", "value for app.toml minimum-gas-prices (default = chain-registry fees.fee_tokens[0]); cosmos-sdk daemons refuse to start without one")
	forwardPeers := fs.Bool("forward-peers", true, "write fetch's served peers into config.toml's persistent_peers")
	doCopyAddrbook := fs.Bool("copy-addrbook", true, "copy fetch's addrbook into the chain home's config/addrbook.json")
	skipInit := fs.Bool("skip-init", false, "skip <chain-exe> init (assumes the operator pre-init'd -out)")
	overwrite := fs.Bool("overwrite", false, "wipe the chain home before bootstrapping (mutually exclusive with -skip-init)")
	trustHeight := fs.Int64("trust-height", 0, "trust height for light client (defaults to -height)")
	trustHashHex := fs.String("trust-hash", "", "trust block hash (hex) at -trust-height; auto-fetched from the chain's first RPC if empty")
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	debug := fs.Bool("debug", false, "verbose logging")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	extraInitArgs := fs.Args() // anything after `--` becomes extra `<chain-exe> init` args

	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return 2
	}

	if *appdb == "" {
		fmt.Fprintln(os.Stderr, "required: -appdb <dir>")
		return 2
	}
	if *skipInit && *overwrite {
		fmt.Fprintln(os.Stderr, "-skip-init and -overwrite are mutually exclusive")
		return 2
	}
	strategy, err := ParseAppStrategy(*appStrategy)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	// Read appdb meta.json. chain_id, height, and db_backend are
	// drawn from there so the operator doesn't have to repeat them;
	// flags are required overrides only when meta.json is missing.
	appdbMeta, metaErr := snapshotimport.ReadAppDBMeta(*appdb)
	switch {
	case metaErr != nil && !os.IsNotExist(metaErr):
		fmt.Fprintf(os.Stderr, "read appdb meta: %v\n", metaErr)
		return 1
	case metaErr != nil:
		if *chain == "" || *height == 0 {
			fmt.Fprintf(os.Stderr, "no meta.json in %s; pass -chain and -height to override\n", *appdb)
			return 2
		}
	default:
		if appdbMeta.ChainID != "" && *chain != "" && appdbMeta.ChainID != *chain {
			fmt.Fprintf(os.Stderr, "meta.json chain_id %q does not match -chain %q\n", appdbMeta.ChainID, *chain)
			return 1
		}
		if appdbMeta.Height != 0 && *height != 0 && appdbMeta.Height != *height {
			fmt.Fprintf(os.Stderr, "meta.json height %d does not match -height %d\n", appdbMeta.Height, *height)
			return 1
		}
		if *chain == "" {
			if appdbMeta.ChainID == "" {
				fmt.Fprintln(os.Stderr, "meta.json has no chain_id; pass -chain to override")
				return 2
			}
			*chain = appdbMeta.ChainID
		}
		if *height == 0 {
			if appdbMeta.Height == 0 {
				fmt.Fprintln(os.Stderr, "meta.json has no height; pass -height to override")
				return 2
			}
			*height = appdbMeta.Height
		}
	}

	// db_backend defaults to pebble for any meta.json that pre-dates
	// the field (only one backend has ever been written).
	dbBackend := appdbMeta.DBBackend
	if dbBackend == "" {
		dbBackend = snapshotimport.DBBackendPebble
	}

	cfgFile, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	ch, err := cfgFile.Resolve(*chain)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	logOpts, err := malcomlog.BuildOptions(malcomlog.Tuning{
		Level:   ch.Log.Level,
		Modules: ch.Log.Modules,
	}, mode, *debug, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log config: %v\n", err)
		return 2
	}
	log := malcomlog.New(logOpts).With("module", "bootstrap")

	// chain-registry lookup gives us daemon_name (PATH fallback) and
	// recommended_version (informational). Missing registry entry is
	// fine when the operator passes -binary explicitly.
	var regInfo *registry.ChainInfo
	if cacheDir, err := config.RegistryCacheDir(); err == nil {
		if r, err := registry.Lookup(cacheDir, ch.ChainID); err == nil {
			regInfo = r
		} else {
			log.Debug("chain-registry lookup miss", "chain", ch.ChainID, "err", err)
		}
	}
	daemonName := ""
	if regInfo != nil {
		daemonName = regInfo.DaemonName
	}
	binPath, err := findBinary(*binary, daemonName)
	if err != nil {
		log.Error("locate chain binary", "err", err)
		return 1
	}

	// RPCs are needed for the trust-hash lookup and for the
	// [statesync] block we patch into config.toml. Bail early if
	// missing — later steps will fail anyway.
	if len(ch.RPCs) == 0 {
		log.Error("chains.<id>.rpcs is empty", "chain", ch.ChainID, "config", cfgFile.Path())
		return 1
	}
	rpcs := append([]string(nil), ch.RPCs...)
	if len(rpcs) == 1 {
		// cometbft's light client wants at least 2 (1 primary + 1 witness).
		rpcs = append(rpcs, rpcs[0])
		log.Warn("only 1 RPC URL configured; duplicating for cometbft light-client (requires >=2)")
	}

	if *trustHeight == 0 {
		*trustHeight = *height
	}
	chosenMoniker := *moniker
	if chosenMoniker == "" {
		chosenMoniker = ch.Bootstrap.Moniker
	}
	if chosenMoniker == "" {
		chosenMoniker = "malcom-bootstrap"
	}

	outRoot := filepath.Join(*out, fmt.Sprintf("home_%s_%d", ch.ChainID, *height))
	configDir := filepath.Join(outRoot, "config")
	dataDir := filepath.Join(outRoot, "data")

	log.Info("starting",
		"config", cfgFile.Path(),
		"chain", ch.ChainID,
		"height", *height,
		"out", outRoot,
		"binary", binPath,
		"app_strategy", string(strategy),
		"db_backend", dbBackend)
	if regInfo != nil && regInfo.RecommendedVersion != "" {
		log.Info("chain-registry recommended version", "version", regInfo.RecommendedVersion)
	}

	if *overwrite {
		log.Info("overwrite: wiping out dir", "dir", outRoot)
		if err := os.RemoveAll(outRoot); err != nil {
			log.Error("remove out dir", "err", err)
			return 1
		}
	}

	ctx := context.Background()

	// 1. <binary> init <moniker> --chain-id <id> --home <outRoot> [extras]
	if !*skipInit {
		if _, err := os.Stat(filepath.Join(configDir, "config.toml")); err == nil {
			log.Error("chain home already initialized; pass -skip-init or -overwrite", "dir", outRoot)
			return 1
		}
		if err := os.MkdirAll(outRoot, 0o755); err != nil {
			log.Error("mkdir out failed", "err", err)
			return 1
		}
		initArgs := append([]string{"init", chosenMoniker, "--chain-id", ch.ChainID, "--home", outRoot}, extraInitArgs...)
		if err := runDaemon(ctx, log, binPath, initArgs...); err != nil {
			log.Error("daemon init failed", "err", err)
			return 1
		}
	} else {
		if _, err := os.Stat(filepath.Join(configDir, "config.toml")); err != nil {
			log.Error("-skip-init set but config.toml missing", "path", filepath.Join(configDir, "config.toml"))
			return 1
		}
		log.Info("skip-init: using existing chain home", "dir", outRoot)
	}

	// 2. Overlay real genesis.json from chain config (or chain-registry).
	if err := overlayGenesis(ch, regInfo, configDir, log); err != nil {
		log.Error("overlay genesis", "err", err)
		return 1
	}

	// 3. Resolve trust hash (auto-fetch from RPC if not provided).
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

	// 4. Patch app.toml + config.toml with the values bootstrap-state
	//    and the runtime daemon will read.
	appTOML := filepath.Join(configDir, "app.toml")
	cfgTOML := filepath.Join(configDir, "config.toml")
	if err := setTOMLString(appTOML, "", "app-db-backend", dbBackend); err != nil {
		log.Error("patch app.toml app-db-backend", "err", err)
		return 1
	}
	log.Info("app.toml patched", "app-db-backend", dbBackend)

	chosenMinGasPrices := *minGasPrices
	if chosenMinGasPrices == "" && regInfo != nil {
		chosenMinGasPrices = regInfo.MinGasPrice
	}
	if chosenMinGasPrices != "" {
		if err := setTOMLString(appTOML, "", "minimum-gas-prices", chosenMinGasPrices); err != nil {
			log.Error("patch app.toml minimum-gas-prices", "err", err)
			return 1
		}
		log.Info("app.toml minimum-gas-prices patched", "value", chosenMinGasPrices)
	} else {
		log.Warn("no minimum-gas-prices source — daemon will refuse to start until you set it",
			"hint", "pass -minimum-gas-prices, or set it in app.toml after bootstrap")
	}

	trustPeriod := ch.Bootstrap.TrustPeriod.Duration().String()
	rpcServersCSV := strings.Join(rpcs, ",")
	if err := setTOMLString(cfgTOML, "statesync", "rpc_servers", rpcServersCSV); err != nil {
		log.Error("patch config.toml statesync.rpc_servers", "err", err)
		return 1
	}
	if err := setTOMLInt64(cfgTOML, "statesync", "trust_height", *trustHeight); err != nil {
		log.Error("patch config.toml statesync.trust_height", "err", err)
		return 1
	}
	if err := setTOMLString(cfgTOML, "statesync", "trust_hash", *trustHashHex); err != nil {
		log.Error("patch config.toml statesync.trust_hash", "err", err)
		return 1
	}
	if err := setTOMLString(cfgTOML, "statesync", "trust_period", trustPeriod); err != nil {
		log.Error("patch config.toml statesync.trust_period", "err", err)
		return 1
	}
	log.Info("config.toml [statesync] patched",
		"rpc_servers", rpcServersCSV, "trust_height", *trustHeight, "trust_period", trustPeriod)

	// 5. Forward fetch-validated served peers + the chain config's
	//    bootstrap_peers (chain-registry's seeds + persistent_peers,
	//    merged at `malcom add` time) into [p2p].persistent_peers.
	//    served.json comes first because those nodes proved they could
	//    serve us during fetch; the registry list backfills with
	//    well-known stable peers in case served entries are pruned or
	//    don't run blocksync.
	if *forwardPeers {
		served, err := readServedPeers(ch.Served, defaultPersistentPeerCount)
		if err != nil {
			log.Warn("read served peers (continuing without)", "err", err, "path", ch.Served)
		}
		merged := mergePeers(served, ch.Fetch.BootstrapPeers)
		if len(merged) == 0 {
			log.Info("no peers to forward (served.json empty and no [fetch].bootstrap_peers)")
		} else {
			joined := joinPersistentPeers(merged)
			if err := setTOMLString(cfgTOML, "p2p", "persistent_peers", joined); err != nil {
				log.Error("patch config.toml p2p.persistent_peers", "err", err)
				return 1
			}
			log.Info("config.toml [p2p].persistent_peers patched",
				"served", len(served),
				"registry", len(ch.Fetch.BootstrapPeers),
				"merged", len(merged))
		}
	}

	// 6. Copy fetch's addrbook into <home>/config/addrbook.json if asked.
	if *doCopyAddrbook {
		if err := copyAddrbook(ch.AddrBook, outRoot, log); err != nil {
			log.Error("copy addrbook", "err", err)
			return 1
		}
	}

	// 7. Place application.db.
	srcAppDB := filepath.Join(*appdb, "application.db")
	dstAppDB := filepath.Join(dataDir, "application.db")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		log.Error("mkdir data", "err", err)
		return 1
	}
	if err := placeAppDB(srcAppDB, dstAppDB, strategy, log); err != nil {
		log.Error("place application.db", "err", err)
		return 1
	}

	// 8. Place wasm extension payloads (if any).
	srcExt := filepath.Join(*appdb, "extensions")
	if _, err := os.Stat(srcExt); err == nil {
		if err := placeWasmPayloads(srcExt, outRoot, ch.ChainID, log); err != nil {
			log.Error("place wasm payloads", "err", err)
			return 1
		}
	}

	// 9. Probe for the right statesync subcommand path and invoke
	//    `<binary> {tendermint,comet} bootstrap-state --height <h>`.
	sub, err := probeStatesyncSubcommand(ctx, binPath)
	if err != nil {
		log.Error("probe bootstrap-state subcommand", "err", err)
		return 1
	}
	log.Info("bootstrap-state subcommand probed", "path", sub)

	bsArgs := []string{sub, "bootstrap-state", "--home", outRoot, "--height", fmt.Sprintf("%d", *height)}
	if err := runDaemon(ctx, log, binPath, bsArgs...); err != nil {
		log.Error("bootstrap-state failed", "err", err)
		return 1
	}

	log.Info("done",
		"home", outRoot,
		"genesis", filepath.Join(configDir, "genesis.json"),
		"appdb", dstAppDB,
		"state_db", filepath.Join(dataDir, "state.db"),
		"blockstore_db", filepath.Join(dataDir, "blockstore.db"),
		"height", *height,
		"start_command", fmt.Sprintf("%s start --home %s", binPath, outRoot))
	return 0
}

// overlayGenesis copies the chain's real genesis.json into <configDir>.
// Source priority:
//  1. chains/<id>.toml's `genesis` field (resolved to a local file).
//  2. chain-registry's codebase.genesis.genesis_url (downloaded into
//     malcom's data dir, then copied).
//
// Returns nil if neither source is available — `<chain-exe> init` has
// already written a placeholder genesis, so the operator has at least
// a starting point. Logged as a warning in that case.
func overlayGenesis(ch config.Chain, regInfo *registry.ChainInfo, configDir string, log *slog.Logger) error {
	dst := filepath.Join(configDir, "genesis.json")

	if ch.Genesis != "" {
		src, err := resolveGenesis(ch, log)
		if err != nil {
			return err
		}
		log.Info("genesis copy", "src", src, "dst", dst)
		return copyFile(src, dst)
	}
	if regInfo != nil && regInfo.GenesisURL != "" {
		dataDir, err := config.DataDir()
		if err != nil {
			return err
		}
		cached := filepath.Join(dataDir, ch.ChainID, "genesis.json")
		log.Info("genesis from chain-registry", "url", regInfo.GenesisURL)
		if err := registry.DownloadGenesis(regInfo.GenesisURL, cached); err != nil {
			return err
		}
		log.Info("genesis copy", "src", cached, "dst", dst)
		return copyFile(cached, dst)
	}
	log.Warn("no genesis source configured; using daemon's init placeholder",
		"chain", ch.ChainID,
		"hint", "set chains.<id>.genesis or run `malcom registry refresh`")
	return nil
}

// fetchBlockHash retrieves the block hash at height from a cometbft
// RPC's /commit endpoint. Used to populate config.toml's
// [statesync].trust_hash.
func fetchBlockHash(rpcBase string, height int64) (string, error) {
	url := fmt.Sprintf("%s/commit?height=%d", strings.TrimRight(rpcBase, "/"), height)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
	}
	var raw map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return "", err
	}
	result, _ := raw["result"].(map[string]interface{})
	signed, _ := result["signed_header"].(map[string]interface{})
	commit, _ := signed["commit"].(map[string]interface{})
	blockID, _ := commit["block_id"].(map[string]interface{})
	hash, _ := blockID["hash"].(string)
	if hash == "" {
		return "", fmt.Errorf("no block hash in /commit?height=%d", height)
	}
	return hash, nil
}

