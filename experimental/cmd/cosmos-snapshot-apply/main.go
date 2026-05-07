// cosmos-snapshot-apply applies a diff produced by cosmos-snapshot-diff
// to a base snapshot, producing a target snapshot directory.
//
// Usage:
//
//	cosmos-snapshot-apply apply      -base <dir> -diff <file> -out <dir>     (CSDF leaf-only diff)
//	cosmos-snapshot-apply state-sync -base <dir> -diff <file> -out <dir>     (CSDS state-sync diff)
//	cosmos-snapshot-apply verify     -a <dir> -b <dir>                        (logical equivalence check)
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/zrbecker/cosmos-p2p/internal/snapshotdiff"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "apply":
		runApply(os.Args[2:])
	case "state-sync":
		runStateSyncApply(os.Args[2:])
	case "verify":
		runVerify(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `cosmos-snapshot-apply — apply a diff to a base snapshot, or verify two snapshots.

Usage:
  cosmos-snapshot-apply apply      -base <dir> -diff <file> -out <dir>      (CSDF leaf-only diff)
  cosmos-snapshot-apply state-sync -base <dir> -diff <file> -out <dir> [-tmp-dir <dir>] [-keep-tmp]
  cosmos-snapshot-apply verify     -a <dir> -b <dir>
`)
}

func runStateSyncApply(args []string) {
	fs := flag.NewFlagSet("state-sync", flag.ExitOnError)
	base := fs.String("base", "", "base snapshot directory")
	diffPath := fs.String("diff", "", "CSDS diff file to apply")
	out := fs.String("out", "", "output directory for reconstructed snapshot")
	tmpDirRoot := fs.String("tmp-dir", "/mnt/data/cosmos-archive/diff-tmp", "root for tmp Pebble base index")
	keepTmp := fs.Bool("keep-tmp", false, "do not delete tmp dir on success")
	minFreeGB := fs.Int64("min-free-gb", 30, "abort if -tmp-dir's filesystem has less than this many GB free")
	_ = fs.Parse(args)
	if *base == "" || *diffPath == "" || *out == "" {
		fs.Usage()
		os.Exit(2)
	}

	if err := snapshotdiff.CheckFreeSpace(*tmpDirRoot, uint64(*minFreeGB)<<30); err != nil {
		log.Fatalf("disk check: %v", err)
	}
	tmpDir, cleanup, err := snapshotdiff.MakeTmpDir(*tmpDirRoot, *keepTmp)
	if err != nil {
		log.Fatalf("tmp dir: %v", err)
	}
	defer cleanup()

	fmt.Printf("[apply] mode=state-sync\n")
	fmt.Printf("[apply] base %s\n", *base)
	fmt.Printf("[apply] diff %s\n", *diffPath)
	fmt.Printf("[apply] out  %s\n", *out)
	fmt.Printf("[apply] tmp  %s\n\n", tmpDir)

	t0 := time.Now()
	stats, err := snapshotdiff.ApplyStateSync(*base, *diffPath, *out, tmpDir)
	if err != nil {
		log.Fatalf("apply: %v", err)
	}
	elapsed := time.Since(t0).Truncate(time.Millisecond)

	fmt.Printf("[apply] complete in %s\n\n", elapsed)
	fmt.Printf("  base items indexed: %d\n", stats.BaseItems)
	fmt.Printf("  target items:       %d\n", stats.TargetItems)
	fmt.Printf("  refs:               %d\n", stats.Refs)
	fmt.Printf("  literals:           %d\n", stats.Literals)
	fmt.Printf("  chunks:             %d\n", stats.Chunks)
	fmt.Printf("  raw stream bytes:   %s\n", snapshotdiff.HumanBytes(stats.BytesUncompressed))
	fmt.Printf("  on-disk chunk bytes:%s\n", snapshotdiff.HumanBytes(stats.BytesCompressed))
	fmt.Printf("  metadata.bin:       %d bytes\n", stats.MetadataBytes)
	if mp := filepath.Join(*out, "metadata.bin"); fileExists(mp) {
		fmt.Printf("  written:            %s\n", mp)
	}
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

func runApply(args []string) {
	fs := flag.NewFlagSet("apply", flag.ExitOnError)
	base := fs.String("base", "", "base snapshot directory")
	diffPath := fs.String("diff", "", "diff file to apply")
	out := fs.String("out", "", "output directory for reconstructed snapshot")
	_ = fs.Parse(args)
	if *base == "" || *diffPath == "" || *out == "" {
		fs.Usage()
		os.Exit(2)
	}

	fmt.Printf("[apply] base %s\n", *base)
	fmt.Printf("[apply] diff %s\n", *diffPath)
	fmt.Printf("[apply] out  %s\n\n", *out)

	t0 := time.Now()
	stats, err := snapshotdiff.Apply(*base, *diffPath, *out)
	if err != nil {
		log.Fatalf("apply: %v", err)
	}
	elapsed := time.Since(t0).Truncate(time.Millisecond)

	fmt.Printf("[apply] complete in %s\n\n", elapsed)
	fmt.Printf("  stores written:    %d\n", stats.Stores)
	fmt.Printf("  extensions:        %d\n", stats.Extensions)
	fmt.Printf("  items emitted:     %d\n", stats.ItemsEmitted)
	fmt.Printf("  payloads emitted:  %d\n", stats.PayloadsEmitted)
	fmt.Printf("  chunks:            %d\n", stats.Chunks)
	fmt.Printf("  raw stream bytes:  %s\n", snapshotdiff.HumanBytes(stats.BytesUncompressed))
	fmt.Printf("  on-disk bytes:     %s\n", snapshotdiff.HumanBytes(stats.BytesCompressed))
}

func runVerify(args []string) {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	a := fs.String("a", "", "snapshot A directory")
	b := fs.String("b", "", "snapshot B directory")
	_ = fs.Parse(args)
	if *a == "" || *b == "" {
		fs.Usage()
		os.Exit(2)
	}

	fmt.Printf("[verify] a %s\n", *a)
	fmt.Printf("[verify] b %s\n\n", *b)

	t0 := time.Now()
	res, err := snapshotdiff.VerifyEquivalent(*a, *b)
	if err != nil {
		log.Fatalf("verify: %v", err)
	}
	elapsed := time.Since(t0).Truncate(time.Millisecond)

	totalDiff := res.ExtraInA + res.ExtraInB + res.ValueDiffs + uint64(res.ExtensionDiff)

	fmt.Printf("[verify] complete in %s\n\n", elapsed)
	fmt.Printf("  stores compared:   %d\n", res.Stores)
	fmt.Printf("  items compared:    %d\n", res.ItemsCompared)
	fmt.Printf("  extra in A:        %d\n", res.ExtraInA)
	fmt.Printf("  extra in B:        %d\n", res.ExtraInB)
	fmt.Printf("  value diffs:       %d\n", res.ValueDiffs)
	fmt.Printf("  extension diffs:   %d\n", res.ExtensionDiff)
	fmt.Printf("  TOTAL DIFFERENCES: %d\n", totalDiff)

	if totalDiff > 0 {
		type row struct {
			name string
			s    snapshotdiff.VerifyStoreStats
		}
		var rows []row
		for n, s := range res.PerStore {
			if s.ExtraInA+s.ExtraInB+s.ValueDiffs > 0 {
				rows = append(rows, row{n, s})
			}
		}
		sort.Slice(rows, func(i, j int) bool {
			return rows[i].s.ExtraInA+rows[i].s.ExtraInB+rows[i].s.ValueDiffs >
				rows[j].s.ExtraInA+rows[j].s.ExtraInB+rows[j].s.ValueDiffs
		})
		fmt.Printf("\n  stores with differences:\n")
		fmt.Printf("    %-22s %12s %12s %10s %10s %10s\n", "name", "items_a", "items_b", "extra_a", "extra_b", "val_diff")
		fmt.Printf("    %-22s %12s %12s %10s %10s %10s\n", "----", "-------", "-------", "-------", "-------", "--------")
		for _, r := range rows {
			fmt.Printf("    %-22s %12d %12d %10d %10d %10d\n",
				r.name, r.s.ItemsA, r.s.ItemsB, r.s.ExtraInA, r.s.ExtraInB, r.s.ValueDiffs)
		}
		os.Exit(1)
	}

	fmt.Printf("\n  snapshots are logically equivalent\n")
}
