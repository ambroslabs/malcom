// Package snapshotimport is the `malcom snapshot import` subcommand:
// take a downloaded cosmos-sdk snapshot directory and produce the
// gaiad-compatible artefacts cosmos-bootstrap-gaia consumes:
//
//	<out>/application.db/      pebble DB (gaiad reads with db_backend = "pebbledb")
//	<out>/extensions/<name>/   wasm bytecode payloads (cosmwasm + 08-light-client)
//
// Atop internal/snapshotimport, which is the single-goroutine
// stack-based importer (no iavl dependency, ~10× memory reduction vs
// the deleted wave-parallel path).
package snapshotimport

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/zrbecker/cosmos-p2p/internal/snapshotdiff"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotimport"
)

type metaJSON struct {
	Height  uint64 `json:"height"`
	HashHex string `json:"hash_hex"`
}

// Run is the malcom subcommand entry point. Returns the process exit
// code (0 on success).
func Run(args []string) int {
	fs := flag.NewFlagSet("malcom snapshot import", flag.ContinueOnError)
	var (
		snapshotDir = fs.String("snapshot", "", "snapshot directory (with chunk_*.bin + meta.json)")
		outDir      = fs.String("out", "", "output directory (will contain application.db/ and extensions/)")
		height      = fs.Int64("height", 0, "height to import (default: read from snapshot meta.json)")
		noExt       = fs.Bool("no-extensions", false, "skip writing extension payloads")
		minFreeGB   = fs.Int64("min-free-gb", 30, "abort if -out's filesystem has less than this many GB free; 0 to skip")
		memtableMB  = fs.Int("memtable-mb", 256, "pebble memtable size (per memtable; 2 slots are kept)")
		cacheMB     = fs.Int("cache-mb", 16, "pebble block cache size (small for write-only workload)")
		maxCompact  = fs.Int("max-concurrent-compactions", 4, "max concurrent pebble compactions")
	)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *snapshotDir == "" || *outDir == "" {
		fs.Usage()
		return 2
	}

	if *height == 0 {
		m, err := readMeta(filepath.Join(*snapshotDir, "meta.json"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "read snapshot meta.json: %v (specify -height explicitly to skip)\n", err)
			return 1
		}
		*height = int64(m.Height)
	}

	if *minFreeGB > 0 {
		if err := snapshotdiff.CheckFreeSpace(*outDir, uint64(*minFreeGB)<<30); err != nil {
			fmt.Fprintf(os.Stderr, "disk check: %v\n", err)
			return 1
		}
	}

	fmt.Printf("[import] snapshot      %s\n", *snapshotDir)
	fmt.Printf("[import] out           %s\n", *outDir)
	fmt.Printf("[import] height        %d\n", *height)
	fmt.Printf("[import] memtable      %d MiB\n", *memtableMB)
	fmt.Printf("[import] cache         %d MiB\n", *cacheMB)
	fmt.Printf("[import] max-compact   %d\n", *maxCompact)
	fmt.Printf("[import] extensions    %v\n", !*noExt)
	fmt.Println()

	stats, err := snapshotimport.Import(snapshotimport.Options{
		SnapshotDir:              *snapshotDir,
		OutDir:                   *outDir,
		Height:                   *height,
		NoExtensions:             *noExt,
		MemtableMB:               *memtableMB,
		CacheMB:                  *cacheMB,
		MaxConcurrentCompactions: *maxCompact,
		Log:                      os.Stdout,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "import: %v\n", err)
		return 1
	}

	finalDB := filepath.Join(*outDir, "application.db")
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
