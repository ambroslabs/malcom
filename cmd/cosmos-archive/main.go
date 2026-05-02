// cosmos-archive is the multi-tool for the on-disk block archive.
//
//	cosmos-archive ranges  -archive <dir>            list contiguous-have runs
//	cosmos-archive missing -archive <dir> -lo H -hi H  list gap runs in [lo,hi]
//	cosmos-archive stats   -archive <dir>            shard-by-shard summary
//	cosmos-archive download -archive <dir> ...       (fetcher; see -h)
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/zrbecker/cosmos-p2p/internal/archive"
)

func usage() {
	fmt.Fprintf(os.Stderr, `cosmos-archive — manage the on-disk block archive

Usage:
  cosmos-archive <subcommand> [flags]

Subcommands:
  ranges     show contiguous-present height ranges
  missing    show height ranges we don't have within [lo, hi]
  stats      per-shard counts and the global summary
  download   fetch missing blocks from archive peers (long-running)

Run any subcommand with -h for its flags.
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "ranges":
		runRanges(os.Args[2:])
	case "missing":
		runMissing(os.Args[2:])
	case "stats":
		runStats(os.Args[2:])
	case "download":
		runDownload(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func openStore(dir string) *archive.Store {
	if dir == "" {
		fmt.Fprintln(os.Stderr, "error: -archive required")
		os.Exit(2)
	}
	st, err := archive.New(dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open archive %s: %v\n", dir, err)
		os.Exit(1)
	}
	return st
}

// commafmt prints a uint64 with thousands separators.
func commafmt(n uint64) string {
	s := fmt.Sprintf("%d", n)
	out := make([]byte, 0, len(s)+len(s)/3)
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, byte(c))
	}
	return string(out)
}

func runRanges(args []string) {
	fs := flag.NewFlagSet("ranges", flag.ExitOnError)
	dir := fs.String("archive", "/mnt/data/cosmos-archive/cosmoshub-4", "archive root directory")
	_ = fs.Parse(args)

	st := openStore(*dir)
	defer st.Close()

	ranges, total, err := st.Ranges()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ranges: %v\n", err)
		os.Exit(1)
	}
	if len(ranges) == 0 {
		fmt.Println("(no blocks present)")
		return
	}
	fmt.Println("have:")
	for _, r := range ranges {
		fmt.Printf("  %14s .. %14s  (%14s blocks)\n",
			commafmt(r.Lo), commafmt(r.Hi), commafmt(r.Count()))
	}
	fmt.Printf("  %s\n", "─────────────────────────────────────────────────────────")
	fmt.Printf("  total: %s blocks across %d range(s)\n", commafmt(total), len(ranges))
}

func runMissing(args []string) {
	fs := flag.NewFlagSet("missing", flag.ExitOnError)
	dir := fs.String("archive", "/mnt/data/cosmos-archive/cosmoshub-4", "archive root directory")
	lo := fs.Uint64("lo", 0, "lowest height of the query window (inclusive). 0 ⇒ first present height (or 1)")
	hi := fs.Uint64("hi", 0, "highest height of the query window (inclusive). 0 ⇒ last present height")
	_ = fs.Parse(args)

	st := openStore(*dir)
	defer st.Close()

	have, _, err := st.Ranges()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ranges: %v\n", err)
		os.Exit(1)
	}
	if *lo == 0 {
		if len(have) > 0 {
			*lo = have[0].Lo
		} else {
			*lo = 1
		}
	}
	if *hi == 0 {
		if len(have) > 0 {
			*hi = have[len(have)-1].Hi
		} else {
			*hi = *lo
		}
	}

	gaps, missing, err := st.Missing(*lo, *hi)
	if err != nil {
		fmt.Fprintf(os.Stderr, "missing: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("query: [%s .. %s]\n", commafmt(*lo), commafmt(*hi))
	if len(gaps) == 0 {
		fmt.Println("no missing blocks in window")
		return
	}
	fmt.Println("missing:")
	for _, r := range gaps {
		fmt.Printf("  %14s .. %14s  (%14s blocks)\n",
			commafmt(r.Lo), commafmt(r.Hi), commafmt(r.Count()))
	}
	fmt.Printf("  %s\n", "─────────────────────────────────────────────────────────")
	fmt.Printf("  total: %s blocks across %d gap(s)\n", commafmt(missing), len(gaps))
}

func runStats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	dir := fs.String("archive", "/mnt/data/cosmos-archive/cosmoshub-4", "archive root directory")
	_ = fs.Parse(args)

	st := openStore(*dir)
	defer st.Close()

	bases, err := st.ShardBases()
	if err != nil {
		fmt.Fprintf(os.Stderr, "list shards: %v\n", err)
		os.Exit(1)
	}
	if len(bases) == 0 {
		fmt.Println("(no shards)")
		return
	}
	var totalCount uint64
	fmt.Printf("%-14s  %-14s  %-14s  %-10s\n", "shard_base", "min_height", "max_height", "count")
	for _, base := range bases {
		// Have to open shard to inspect; openStore caches.
		_, _, _ = st.Ranges() // ensures all shards opened
		// Use Has + AllEntries for counts via Ranges machinery would re-walk.
		// Cheaper: just read shard, call PresentRange.
		// Open the shard ourselves:
		// (We don't have a public accessor; use Has on first/last via ranges already.)
		// Simpler: re-implement via a fresh open — the Store keeps it cached.
		// For each shard fetch PresentRange via a tiny indirection:
		//
		// We'll just use the existing scanned ranges per-shard by computing
		// the intersection — but that's an O(R) loop where R = # ranges.
		// For 257 shards × small R, fine.
		_ = base
	}
	// Walk shards via a fresh per-shard scan for simplicity — re-use Ranges().
	allRanges, total, err := st.Ranges()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ranges: %v\n", err)
		os.Exit(1)
	}
	for _, base := range bases {
		shardLo := base
		shardHi := base + archive.ChunkSize - 1
		var minH, maxH uint64
		var cnt uint64
		for _, r := range allRanges {
			if r.Hi < shardLo || r.Lo > shardHi {
				continue
			}
			lo, hi := r.Lo, r.Hi
			if lo < shardLo {
				lo = shardLo
			}
			if hi > shardHi {
				hi = shardHi
			}
			if cnt == 0 {
				minH = lo
			}
			maxH = hi
			cnt += hi - lo + 1
		}
		if cnt == 0 {
			fmt.Printf("%14s  %-14s  %-14s  %10s\n", commafmt(base), "—", "—", "0")
		} else {
			fmt.Printf("%14s  %14s  %14s  %10s\n",
				commafmt(base), commafmt(minH), commafmt(maxH), commafmt(cnt))
		}
		totalCount += cnt
	}
	fmt.Printf("─────────────────────────────────────────────────────────────\n")
	fmt.Printf("shards=%d  blocks=%s  ranges=%d\n", len(bases), commafmt(total), len(allRanges))
	_ = totalCount
}

func runDownload(args []string) {
	fmt.Fprintln(os.Stderr, "download: not implemented yet (next commit). Use 'ranges' / 'missing' / 'stats' for now.")
	os.Exit(2)
}
