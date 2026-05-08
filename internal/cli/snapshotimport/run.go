// Package snapshotimport is the `malcom snapshot import` subcommand:
// take a downloaded snapshot directory and produce gaiad-compatible
// artefacts.
//
// Output: <-out>/appdb_<chain>_<height>/{application.db,extensions}/.
// Default -out is the current working directory.
//
// Pebble bulk-load tuning lives in the [chains.<id>.import] section
// of config.toml.
package snapshotimport

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/zrbecker/cosmos-p2p/internal/config"
	malcomlog "github.com/zrbecker/cosmos-p2p/internal/log"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotdiff"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotimport"
)

type metaJSON struct {
	ChainID string `json:"chain_id"`
	Height  uint64 `json:"height"`
	HashHex string `json:"hash_hex"`
}

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom snapshot import", flag.ContinueOnError)
	chain := fs.String("chain", "", "chain id (override; required if snapshot meta.json is missing or omits chain_id)")
	snapshotDir := fs.String("snapshot", "", "snapshot directory to import (with chunk_*.bin + meta.json)")
	out := fs.String("out", ".", "parent dir for the output (subdir appdb_<chain>_<height>/ created inside)")
	height := fs.Int64("height", 0, "height override (required if snapshot meta.json is missing or omits height)")
	noExt := fs.Bool("no-extensions", false, "skip writing extension payloads")
	logMode := fs.String("log", "", "log output: auto (default), pretty, text, json")
	debug := fs.Bool("debug", false, "verbose logging")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *snapshotDir == "" {
		fs.Usage()
		return 2
	}

	mode, ok := malcomlog.ParseMode(*logMode)
	if !ok {
		fmt.Fprintf(os.Stderr, "invalid -log %q (want auto/pretty/text/json)\n", *logMode)
		return 2
	}
	// Load config so the logger picks up [log] / [log.modules]. Fall
	// back to defaults if config is missing (the import path doesn't
	// strictly require a config — chain id can come from meta.json).
	var logTuning malcomlog.Tuning
	if cfg, err := config.Load(); err == nil {
		logTuning = malcomlog.Tuning{Level: cfg.Log.Level, Modules: cfg.Log.Modules}
	}
	logOpts, err := malcomlog.BuildOptions(logTuning, mode, *debug, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log config: %v\n", err)
		return 2
	}
	logger := malcomlog.New(logOpts)
	log := logger.With("module", "import-cli")

	// Read meta.json if present. The chain id and height are normally
	// drawn from here so the user doesn't have to repeat themselves;
	// the -chain / -height flags only kick in when meta.json is
	// missing or pre-dates the field, in which case they're required
	// overrides. Any flag value that's set must match meta.json.
	metaPath := filepath.Join(*snapshotDir, "meta.json")
	meta, metaErr := readMeta(metaPath)
	switch {
	case metaErr != nil && !os.IsNotExist(metaErr):
		log.Error("read meta.json", "path", metaPath, "err", metaErr)
		return 1
	case metaErr != nil:
		// missing meta.json — flags must fully specify chain + height.
		if *chain == "" || *height == 0 {
			log.Error("no meta.json; pass -chain and -height to override", "path", *snapshotDir)
			return 1
		}
	default:
		if meta.ChainID != "" && *chain != "" && meta.ChainID != *chain {
			log.Error("meta.json chain_id does not match -chain", "meta", meta.ChainID, "flag", *chain)
			return 1
		}
		if meta.Height != 0 && *height != 0 && uint64(*height) != meta.Height {
			log.Error("meta.json height does not match -height", "meta", meta.Height, "flag", *height)
			return 1
		}
		if *chain == "" {
			if meta.ChainID == "" {
				log.Error("meta.json has no chain_id; pass -chain to override")
				return 1
			}
			*chain = meta.ChainID
		}
		if *height == 0 {
			if meta.Height == 0 {
				log.Error("meta.json has no height; pass -height to override")
				return 1
			}
			*height = int64(meta.Height)
		}
	}

	cfg, err := config.Load()
	if err != nil {
		log.Error("config load", "err", err)
		return 1
	}
	ch, err := cfg.Resolve(*chain)
	if err != nil {
		log.Error("resolve chain", "err", err, "chain", *chain)
		return 1
	}

	outDir := filepath.Join(*out, fmt.Sprintf("appdb_%s_%d", ch.ChainID, *height))
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Error("mkdir out failed", "err", err, "dir", outDir)
		return 1
	}

	if ch.Import.MinFreeGB > 0 {
		if err := snapshotdiff.CheckFreeSpace(outDir, uint64(ch.Import.MinFreeGB)<<30); err != nil {
			log.Error("disk check failed", "err", err)
			return 1
		}
	}

	log.Info("starting",
		"config", cfg.Path(),
		"chain", ch.ChainID,
		"snapshot", *snapshotDir,
		"out", outDir,
		"height", *height,
		"memtable_mb", ch.Import.MemtableMB,
		"cache_mb", ch.Import.CacheMB,
		"max_compact", ch.Import.MaxConcurrentCompactions,
		"extensions", !*noExt)

	stats, err := snapshotimport.Import(snapshotimport.Options{
		SnapshotDir:              *snapshotDir,
		OutDir:                   outDir,
		Height:                   *height,
		NoExtensions:             *noExt,
		MemtableMB:               ch.Import.MemtableMB,
		CacheMB:                  ch.Import.CacheMB,
		MaxConcurrentCompactions: ch.Import.MaxConcurrentCompactions,
		Log:                      logger,
	})
	if err != nil {
		log.Error("import failed", "err", err)
		return 1
	}

	finalDB := filepath.Join(outDir, "application.db")
	dbBytes := uint64(0)
	if size, err := dirSize(finalDB); err == nil {
		dbBytes = uint64(size)
	}
	log.Info("complete",
		"elapsed", stats.Elapsed,
		"stores", len(stats.Stores),
		"items", stats.Items,
		"extensions", stats.Extensions,
		"ext_payloads", stats.ExtensionPayloads,
		"stream_elapsed", stats.StreamElapsed,
		"final_compact_elapsed", stats.FinalCompactElapsed,
		"cleanup_elapsed", stats.CleanupElapsed,
		"appdb", finalDB,
		"appdb_bytes", dbBytes)
	return 0
}

func readMeta(path string) (metaJSON, error) {
	var m metaJSON
	f, err := os.Open(path)
	if err != nil {
		return m, err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return m, err
	}
	return m, nil
}

func dirSize(dir string) (int64, error) {
	var total int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

