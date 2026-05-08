// cosmos-snapshot-inspect decodes a downloaded state-sync snapshot
// (zstd-compressed chunks + metadata.bin) and rewrites the directory's
// meta.json with structural details: stores, items per store, extension
// modules, decoded chunk_hashes.
//
// Standalone form is useful for re-running on already-downloaded snapshots
// or after future changes to the inspector. cosmos-snapshot-fetch also
// invokes this logic automatically after a successful download.
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

	"github.com/zrbecker/cosmos-p2p/internal/humanbytes"
	"github.com/zrbecker/cosmos-p2p/internal/snapshotinspect"
)

// MetaJSON mirrors the format written by cosmos-snapshot-fetch + the
// inspector's enrichment fields. Round-trips so existing fields are
// preserved across re-runs.
type MetaJSON struct {
	// Original fetch fields
	Height          uint64    `json:"height"`
	Format          uint32    `json:"format"`
	Chunks          uint32    `json:"chunks"`
	HashHex         string    `json:"hash_hex"`
	MetadataLen     int       `json:"metadata_len"`
	GoodPeers       []string  `json:"good_peers,omitempty"`
	OfferedBy       []string  `json:"offered_by,omitempty"`
	DownloadedAt    time.Time `json:"downloaded_at"`
	BytesTotal      uint64    `json:"bytes_total"`
	BytesTotalHuman string    `json:"bytes_total_human"`

	// Inspector fields (populated by this tool)
	InspectedAt            time.Time                       `json:"inspected_at,omitempty"`
	DecompressedBytes      uint64                          `json:"decompressed_bytes,omitempty"`
	DecompressedBytesHuman string                          `json:"decompressed_bytes_human,omitempty"`
	TotalItems             uint64                          `json:"total_items,omitempty"`
	Stores                 []snapshotinspect.StoreInfo     `json:"stores,omitempty"`
	Extensions             []snapshotinspect.ExtensionInfo `json:"extensions,omitempty"`
	UnknownItemTags        map[uint8]uint64                `json:"unknown_item_tags,omitempty"`
	ChunkHashesHex         []string                        `json:"chunk_hashes_hex,omitempty"`
}

func main() {
	dir := flag.String("dir", "", "snapshot directory (containing chunks + metadata.bin + meta.json)")
	verbose := flag.Bool("verbose", true, "print summary table to stdout")
	flag.Parse()
	if *dir == "" {
		log.Fatalf("-dir is required")
	}

	if err := Run(*dir, *verbose); err != nil {
		log.Fatalf("inspect: %v", err)
	}
}

// Run decodes the snapshot at dir, enriches dir/meta.json with structural
// info, and (if verbose) prints a summary to stdout.
func Run(dir string, verbose bool) error {
	metaPath := filepath.Join(dir, "meta.json")
	mdPath := filepath.Join(dir, "metadata.bin")

	meta, err := readMeta(metaPath)
	if err != nil {
		return fmt.Errorf("read meta.json: %w", err)
	}
	// Compute bytes_total_human in case the original write predated it.
	if meta.BytesTotalHuman == "" {
		meta.BytesTotalHuman = humanbytes.Format(meta.BytesTotal)
	}

	// Decode chunk_hashes from metadata.bin.
	mdBytes, err := os.ReadFile(mdPath)
	if err != nil {
		return fmt.Errorf("read metadata.bin: %w", err)
	}
	hashes, err := snapshotinspect.ParseChunkHashes(mdBytes)
	if err != nil {
		return fmt.Errorf("parse chunk_hashes: %w", err)
	}
	meta.ChunkHashesHex = hashes

	// Stream-parse the snapshot itself.
	if verbose {
		fmt.Printf("[inspect] decoding %d chunks in %s ...\n", meta.Chunks, dir)
	}
	t0 := time.Now()
	res, err := snapshotinspect.Inspect(dir)
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}
	elapsed := time.Since(t0)

	meta.InspectedAt = time.Now().UTC()
	meta.DecompressedBytes = res.DecompressedBytes
	meta.DecompressedBytesHuman = humanbytes.Format(res.DecompressedBytes)
	meta.TotalItems = res.TotalItems
	meta.Stores = res.Stores
	meta.Extensions = res.Extensions
	meta.UnknownItemTags = res.UnknownItemTags

	if err := writeMeta(metaPath, meta); err != nil {
		return fmt.Errorf("write meta.json: %w", err)
	}

	if verbose {
		printSummary(meta, elapsed)
	}
	return nil
}

func readMeta(path string) (MetaJSON, error) {
	var m MetaJSON
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

func writeMeta(path string, m MetaJSON) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(m)
}

func printSummary(m MetaJSON, elapsed time.Duration) {
	fmt.Printf("\n[inspect] height=%d format=%d  parsed %d items  decompressed=%s in %s\n",
		m.Height, m.Format, m.TotalItems, m.DecompressedBytesHuman, elapsed.Truncate(time.Millisecond))
	if len(m.Stores) > 0 {
		// Sort a copy by bytes desc for the printout.
		sorted := make([]snapshotinspect.StoreInfo, len(m.Stores))
		copy(sorted, m.Stores)
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].BytesUncompressed > sorted[j].BytesUncompressed
		})
		fmt.Printf("\n  STORES (%d):\n", len(sorted))
		fmt.Printf("    %-22s %12s %12s\n", "name", "items", "bytes")
		fmt.Printf("    %-22s %12s %12s\n", "----", "-----", "-----")
		for _, s := range sorted {
			fmt.Printf("    %-22s %12d %12s\n", s.Name, s.Items, s.BytesHuman)
		}
	}
	if len(m.Extensions) > 0 {
		fmt.Printf("\n  EXTENSIONS (%d):\n", len(m.Extensions))
		fmt.Printf("    %-22s %6s %12s %12s\n", "name", "format", "payloads", "bytes")
		fmt.Printf("    %-22s %6s %12s %12s\n", "----", "------", "--------", "-----")
		for _, e := range m.Extensions {
			fmt.Printf("    %-22s %6d %12d %12s\n", e.Name, e.Format, e.Payloads, e.BytesHuman)
		}
	}
	if len(m.UnknownItemTags) > 0 {
		fmt.Printf("\n  UNKNOWN ITEM TAGS: %v\n", m.UnknownItemTags)
	}
	fmt.Printf("\n  meta.json updated at %s\n", filepath.Join("(snapshot dir)", "meta.json"))
}
