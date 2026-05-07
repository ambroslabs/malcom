// cosmos-snapshot-diff produces a diff between two snapshots.
//
// Two modes:
//
//	cosmos-snapshot-diff leaves     -base <dir> -target <dir> -o <out.diff>
//	cosmos-snapshot-diff state-sync -base <dir> -target <dir> -o <out.diff> [-tmp-dir <dir>] [-keep-tmp]
//
// "leaves" is the compact archival format (only IAVL leaf changes).
// "state-sync" produces a content-addressed diff over the full snapshot
// stream — bigger, but apply produces a snapshot byte-equivalent to a
// real cosmos-sdk producer at target_height (state-sync feedable).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/snapshotdiff"
)

type metaJSON struct {
	Height  uint64 `json:"height"`
	HashHex string `json:"hash_hex"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "leaves":
		runLeaves(os.Args[2:])
	case "state-sync":
		runStateSync(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		// Legacy: no-subcommand defaults to leaves mode for back-compat.
		runLeaves(os.Args[1:])
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `cosmos-snapshot-diff — diff two snapshots.

Usage:
  cosmos-snapshot-diff leaves     -base <dir> -target <dir> -o <out.diff>
  cosmos-snapshot-diff state-sync -base <dir> -target <dir> -o <out.diff> [-tmp-dir <dir>] [-keep-tmp]
`)
}

func runLeaves(args []string) {
	fs := flag.NewFlagSet("leaves", flag.ExitOnError)
	base := fs.String("base", "", "base snapshot directory (older)")
	target := fs.String("target", "", "target snapshot directory (newer)")
	out := fs.String("o", "", "output diff file path")
	_ = fs.Parse(args)
	if *base == "" || *target == "" || *out == "" {
		fs.Usage()
		os.Exit(2)
	}

	bm, err := readMeta(filepath.Join(*base, "meta.json"))
	if err != nil {
		log.Fatalf("read base meta.json: %v", err)
	}
	tm, err := readMeta(filepath.Join(*target, "meta.json"))
	if err != nil {
		log.Fatalf("read target meta.json: %v", err)
	}
	fmt.Printf("[diff] mode=leaves\n")
	fmt.Printf("[diff] base   height=%d hash=%s\n", bm.Height, bm.HashHex[:16])
	fmt.Printf("[diff] target height=%d hash=%s\n", tm.Height, tm.HashHex[:16])
	fmt.Printf("[diff] out    %s\n\n", *out)

	t0 := time.Now()
	stats, err := snapshotdiff.Compute(*base, *target, *out, bm.HashHex, tm.HashHex, bm.Height, tm.Height)
	if err != nil {
		log.Fatalf("compute: %v", err)
	}
	elapsed := time.Since(t0).Truncate(time.Millisecond)

	fmt.Printf("[diff] complete in %s\n\n", elapsed)
	fmt.Printf("  totals\n")
	fmt.Printf("    inserts:        %d\n", stats.Inserts)
	fmt.Printf("    updates:        %d\n", stats.Updates)
	fmt.Printf("    deletes:        %d\n", stats.Deletes)
	fmt.Printf("    ext adds:       %d\n", stats.ExtAdds)
	fmt.Printf("    ext removes:    %d\n", stats.ExtRemoves)
	fmt.Printf("    body raw:       %s\n", snapshotdiff.HumanBytes(stats.BodyBytesRaw))
	fmt.Printf("    body compressed:%s\n", snapshotdiff.HumanBytes(stats.BodyBytesGz))
	if fi, err := os.Stat(*out); err == nil {
		fmt.Printf("    file on disk:   %s  (%s)\n", *out, snapshotdiff.HumanBytes(uint64(fi.Size())))
	}

	type row struct {
		name string
		s    snapshotdiff.StoreStats
		ops  uint64
	}
	rows := []row{}
	for n, s := range stats.PerStore {
		rows = append(rows, row{n, s, s.Inserts + s.Updates + s.Deletes})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ops > rows[j].ops })
	if len(rows) > 0 {
		fmt.Printf("\n  per-store (sorted by changes desc)\n")
		fmt.Printf("    %-22s %12s %12s %10s %10s %10s\n", "name", "base_items", "target_items", "ins", "upd", "del")
		fmt.Printf("    %-22s %12s %12s %10s %10s %10s\n", "----", "----------", "------------", "---", "---", "---")
		for _, r := range rows {
			fmt.Printf("    %-22s %12d %12d %10d %10d %10d\n",
				r.name, r.s.BaseItems, r.s.TargetItems, r.s.Inserts, r.s.Updates, r.s.Deletes)
		}
	}
}

func runStateSync(args []string) {
	fs := flag.NewFlagSet("state-sync", flag.ExitOnError)
	base := fs.String("base", "", "base snapshot directory (older)")
	target := fs.String("target", "", "target snapshot directory (newer)")
	out := fs.String("o", "", "output diff file path")
	tmpDirRoot := fs.String("tmp-dir", "/mnt/data/cosmos-archive/diff-tmp", "root for tmp Pebble base index")
	keepTmp := fs.Bool("keep-tmp", false, "do not delete tmp dir on success (for debugging)")
	minFreeGB := fs.Int64("min-free-gb", 20, "abort if -tmp-dir's filesystem has less than this many GB free")
	_ = fs.Parse(args)
	if *base == "" || *target == "" || *out == "" {
		fs.Usage()
		os.Exit(2)
	}

	bm, err := readMeta(filepath.Join(*base, "meta.json"))
	if err != nil {
		log.Fatalf("read base meta.json: %v", err)
	}
	tm, err := readMeta(filepath.Join(*target, "meta.json"))
	if err != nil {
		log.Fatalf("read target meta.json: %v", err)
	}

	if err := snapshotdiff.CheckFreeSpace(*tmpDirRoot, uint64(*minFreeGB)<<30); err != nil {
		log.Fatalf("disk check: %v", err)
	}
	tmpDir, cleanup, err := snapshotdiff.MakeTmpDir(*tmpDirRoot, *keepTmp)
	if err != nil {
		log.Fatalf("tmp dir: %v", err)
	}
	defer cleanup()

	fmt.Printf("[diff] mode=state-sync\n")
	fmt.Printf("[diff] base   height=%d hash=%s\n", bm.Height, bm.HashHex[:16])
	fmt.Printf("[diff] target height=%d hash=%s\n", tm.Height, tm.HashHex[:16])
	fmt.Printf("[diff] out    %s\n", *out)
	fmt.Printf("[diff] tmp    %s (kept on error; %s)\n\n", tmpDir,
		map[bool]string{true: "kept on success", false: "removed on success"}[*keepTmp])

	t0 := time.Now()
	stats, err := snapshotdiff.ComputeStateSync(*base, *target, *out, tmpDir, bm.HashHex, tm.HashHex, bm.Height, tm.Height)
	if err != nil {
		log.Fatalf("compute state-sync: %v", err)
	}
	elapsed := time.Since(t0).Truncate(time.Millisecond)

	fmt.Printf("[diff] complete in %s\n\n", elapsed)
	fmt.Printf("  base items:       %d\n", stats.BaseItems)
	fmt.Printf("  target items:     %d\n", stats.TargetItems)
	fmt.Printf("  refs (matches):   %d (%.1f%%)\n", stats.Refs,
		100*float64(stats.Refs)/float64(stats.TargetItems))
	fmt.Printf("  literals (new):   %d (%.1f%%)\n", stats.Literals,
		100*float64(stats.Literals)/float64(stats.TargetItems))
	fmt.Printf("  literal bytes:    %s\n", snapshotdiff.HumanBytes(stats.LiteralBytes))
	fmt.Printf("  body raw:         %s\n", snapshotdiff.HumanBytes(stats.BodyBytesRaw))
	fmt.Printf("  body compressed:  %s\n", snapshotdiff.HumanBytes(stats.BodyBytesGz))
	if fi, err := os.Stat(*out); err == nil {
		fmt.Printf("  file on disk:     %s  (%s)\n", *out, snapshotdiff.HumanBytes(uint64(fi.Size())))
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
