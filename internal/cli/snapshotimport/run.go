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
	"github.com/zrbecker/cosmos-p2p/internal/humanbytes"
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
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *snapshotDir == "" {
		fs.Usage()
		return 2
	}

	// Read meta.json if present. The chain id and height are normally
	// drawn from here so the user doesn't have to repeat themselves;
	// the -chain / -height flags only kick in when meta.json is
	// missing or pre-dates the field, in which case they're required
	// overrides. Any flag value that's set must match meta.json.
	metaPath := filepath.Join(*snapshotDir, "meta.json")
	meta, metaErr := readMeta(metaPath)
	switch {
	case metaErr != nil && !os.IsNotExist(metaErr):
		fmt.Fprintf(os.Stderr, "read %s: %v\n", metaPath, metaErr)
		return 1
	case metaErr != nil:
		// missing meta.json — flags must fully specify chain + height.
		if *chain == "" || *height == 0 {
			fmt.Fprintf(os.Stderr, "snapshot %s has no meta.json; pass -chain and -height to override\n", *snapshotDir)
			return 1
		}
	default:
		if meta.ChainID != "" && *chain != "" && meta.ChainID != *chain {
			fmt.Fprintf(os.Stderr, "meta.json chain_id %q does not match -chain %q\n", meta.ChainID, *chain)
			return 1
		}
		if meta.Height != 0 && *height != 0 && uint64(*height) != meta.Height {
			fmt.Fprintf(os.Stderr, "meta.json height %d does not match -height %d\n", meta.Height, *height)
			return 1
		}
		if *chain == "" {
			if meta.ChainID == "" {
				fmt.Fprintln(os.Stderr, "snapshot meta.json has no chain_id; pass -chain to override")
				return 1
			}
			*chain = meta.ChainID
		}
		if *height == 0 {
			if meta.Height == 0 {
				fmt.Fprintln(os.Stderr, "snapshot meta.json has no height; pass -height to override")
				return 1
			}
			*height = int64(meta.Height)
		}
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	ch, err := cfg.Resolve(*chain)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	outDir := filepath.Join(*out, fmt.Sprintf("appdb_%s_%d", ch.ChainID, *height))
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir out: %v\n", err)
		return 1
	}

	if ch.Import.MinFreeGB > 0 {
		if err := snapshotdiff.CheckFreeSpace(outDir, uint64(ch.Import.MinFreeGB)<<30); err != nil {
			fmt.Fprintf(os.Stderr, "disk check: %v\n", err)
			return 1
		}
	}

	fmt.Printf("[import] config:       %s\n", cfg.Path())
	fmt.Printf("[import] chain:        %s\n", ch.ChainID)
	fmt.Printf("[import] snapshot:     %s\n", *snapshotDir)
	fmt.Printf("[import] out:          %s\n", outDir)
	fmt.Printf("[import] height:       %d\n", *height)
	fmt.Printf("[import] memtable:     %d MiB\n", ch.Import.MemtableMB)
	fmt.Printf("[import] cache:        %d MiB\n", ch.Import.CacheMB)
	fmt.Printf("[import] max-compact:  %d\n", ch.Import.MaxConcurrentCompactions)
	fmt.Printf("[import] extensions:   %v\n", !*noExt)
	fmt.Println()

	stats, err := snapshotimport.Import(snapshotimport.Options{
		SnapshotDir:              *snapshotDir,
		OutDir:                   outDir,
		Height:                   *height,
		NoExtensions:             *noExt,
		MemtableMB:               ch.Import.MemtableMB,
		CacheMB:                  ch.Import.CacheMB,
		MaxConcurrentCompactions: ch.Import.MaxConcurrentCompactions,
		Log:                      os.Stdout,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "import: %v\n", err)
		return 1
	}

	finalDB := filepath.Join(outDir, "application.db")
	fmt.Println()
	fmt.Printf("[import] complete in %s\n", stats.Elapsed)
	fmt.Printf("  stores written:     %d\n", len(stats.Stores))
	fmt.Printf("  IAVL items:         %d\n", stats.Items)
	fmt.Printf("  extensions:         %d\n", stats.Extensions)
	fmt.Printf("  ext payloads:       %d\n", stats.ExtensionPayloads)
	fmt.Printf("  stream elapsed:     %s\n", stats.StreamElapsed)
	fmt.Printf("  final compact:      %s\n", stats.FinalCompactElapsed)
	fmt.Printf("  cleanup pass:       %s\n", stats.CleanupElapsed)
	if size, err := dirSize(finalDB); err == nil {
		fmt.Printf("  application.db:     %s  (%s)\n", finalDB, humanbytes.Format(uint64(size)))
	}
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

