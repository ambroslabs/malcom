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
	"github.com/zrbecker/cosmos-p2p/internal/snapshotdiff"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotimport"
)

type metaJSON struct {
	Height  uint64 `json:"height"`
	HashHex string `json:"hash_hex"`
}

// Run is the malcom subcommand entry point.
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom snapshot import", flag.ContinueOnError)
	chain := fs.String("chain", "", fmt.Sprintf("chain id (default %q; override in config.default_chain)", config.DefaultChainID))
	snapshotDir := fs.String("snapshot", "", "snapshot directory to import (with chunk_*.bin + meta.json)")
	out := fs.String("out", ".", "parent dir for the output (subdir appdb_<chain>_<height>/ created inside)")
	height := fs.Int64("height", 0, "height to import (default: read from snapshot meta.json)")
	noExt := fs.Bool("no-extensions", false, "skip writing extension payloads")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *snapshotDir == "" {
		fs.Usage()
		return 2
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	chainName := *chain
	if chainName == "" {
		chainName = cfg.DefaultChain
	}
	ch, err := cfg.Resolve(chainName)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	if *height == 0 {
		m, err := readMeta(filepath.Join(*snapshotDir, "meta.json"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "read snapshot meta.json: %v (specify -height explicitly to skip)\n", err)
			return 1
		}
		*height = int64(m.Height)
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
		fmt.Printf("  application.db:     %s  (%s)\n", finalDB, humanBytes(uint64(size)))
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

func humanBytes(n uint64) string {
	const (
		k = 1024
		m = k * 1024
		g = m * 1024
	)
	switch {
	case n >= g:
		return fmt.Sprintf("%.2f GB", float64(n)/float64(g))
	case n >= m:
		return fmt.Sprintf("%.1f MB", float64(n)/float64(m))
	case n >= k:
		return fmt.Sprintf("%.1f KB", float64(n)/float64(k))
	default:
		return fmt.Sprintf("%d B", n)
	}
}
