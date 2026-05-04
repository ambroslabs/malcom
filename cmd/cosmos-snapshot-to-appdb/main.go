// cosmos-snapshot-to-appdb imports a cosmos-sdk format-3 snapshot
// directory into a fresh application.db that gaiad can read.
//
// Usage:
//
//	cosmos-snapshot-to-appdb -snapshot <dir> -out <dir> -height <H> [flags]
//
// Output layout:
//
//	<out>/application.db/      goleveldb directory
//	<out>/extensions/<name>/   extension payloads (wasm bytecode etc.)
//
// To boot a gaiad node from this output:
//   - move/copy <out>/application.db to ~/.gaia/data/application.db
//   - install extension payloads (wasm contracts) under ~/.gaia/data/wasm/
//   - populate state.db + minimal blockstore.db from cometbft RPC (separate
//     tool — not yet built)
//   - gaiad start  → block-syncs from height+1
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/snapshotappdb"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotdiff"
)

type metaJSON struct {
	Height  uint64 `json:"height"`
	HashHex string `json:"hash_hex"`
}

func main() {
	snapshot := flag.String("snapshot", "", "snapshot directory (with chunk_*.bin + meta.json)")
	out := flag.String("out", "", "output directory (will contain application.db/ and extensions/)")
	height := flag.Int64("height", 0, "height to import (default: read from snapshot meta.json)")
	backendStr := flag.String("backend", "goleveldb", "application.db backend: goleveldb")
	noExt := flag.Bool("no-extensions", false, "skip writing extension payloads")
	minFreeGB := flag.Int64("min-free-gb", 30, "abort if -out's filesystem has less than this many GB free")
	concurrency := flag.Int("concurrency", 0, "max stores with work in flight at once (commit pipelined behind reader); 0=auto (min(NumCPU,8)), 1=serial")
	flag.Parse()

	if *snapshot == "" || *out == "" {
		flag.Usage()
		os.Exit(2)
	}

	// Default height from snapshot's meta.json.
	if *height == 0 {
		m, err := readMeta(filepath.Join(*snapshot, "meta.json"))
		if err != nil {
			log.Fatalf("read snapshot meta.json: %v (specify -height explicitly to skip)", err)
		}
		*height = int64(m.Height)
	}

	if err := snapshotdiff.CheckFreeSpace(*out, uint64(*minFreeGB)<<30); err != nil {
		log.Fatalf("disk check: %v", err)
	}

	extDir := ""
	if !*noExt {
		extDir = filepath.Join(*out, "extensions")
	}

	fmt.Printf("[appdb] snapshot    %s\n", *snapshot)
	fmt.Printf("[appdb] out         %s\n", *out)
	fmt.Printf("[appdb] height      %d\n", *height)
	fmt.Printf("[appdb] backend     %s\n", *backendStr)
	fmt.Printf("[appdb] concurrency %d (0=auto)\n", *concurrency)
	if extDir != "" {
		fmt.Printf("[appdb] ext dir     %s\n", extDir)
	}
	fmt.Println()

	t0 := time.Now()
	stats, err := snapshotappdb.Import(
		*snapshot,
		filepath.Join(*out, "appdb-staging"),
		*height,
		snapshotappdb.Backend(*backendStr),
		extDir,
		*concurrency,
	)
	if err != nil {
		log.Fatalf("import: %v", err)
	}

	// Move the staging dir to its final name. The goleveldb backend
	// produces <staging>/application.db (since we passed name="application").
	stagingDB := filepath.Join(*out, "appdb-staging", "application.db")
	finalDB := filepath.Join(*out, "application.db")
	if _, err := os.Stat(stagingDB); err == nil {
		_ = os.RemoveAll(finalDB)
		if err := os.Rename(stagingDB, finalDB); err != nil {
			log.Fatalf("rename to final application.db: %v", err)
		}
		_ = os.Remove(filepath.Join(*out, "appdb-staging"))
	}

	// Pebble's in-process Compact during Import queues obsolete L0
	// files for deletion via the cleanup manager, but those deletions
	// don't always drain before our Close. The result is ~8-10 GB of
	// orphaned SSTs left on disk for cosmoshub-4. Reopening with
	// default options forces pebble's recovery + cleanup path to run,
	// reclaiming the slack. Skip for goleveldb (the slack issue is
	// pebble-specific).
	if snapshotappdb.Backend(*backendStr) == snapshotappdb.BackendPebble {
		fmt.Printf("[appdb] cleanup compaction pass...\n")
		t := time.Now()
		if err := snapshotappdb.PebbleCleanupCompact(finalDB); err != nil {
			log.Fatalf("cleanup compact: %v", err)
		}
		fmt.Printf("[appdb] cleanup pass done in %s\n", time.Since(t).Truncate(time.Second))
	}

	elapsed := time.Since(t0).Truncate(time.Millisecond)
	fmt.Printf("[appdb] complete in %s\n\n", elapsed)
	fmt.Printf("  stores written:     %d\n", stats.Stores)
	fmt.Printf("  IAVL nodes imported:%d\n", stats.Items)
	fmt.Printf("  extensions:         %d\n", stats.Extensions)
	fmt.Printf("  ext payloads:       %d\n", stats.ExtensionPayloads)
	fmt.Printf("  raw bytes:          %s\n", snapshotappdb.HumanBytes(stats.BytesUncompressed))
	if fi, err := dirSize(finalDB); err == nil {
		fmt.Printf("  application.db:     %s  (%s)\n", finalDB, snapshotappdb.HumanBytes(uint64(fi)))
	}
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
