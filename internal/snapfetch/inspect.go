package snapfetch

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	cmtlog "github.com/cometbft/cometbft/libs/log"

	"github.com/zrbecker/cosmos-p2p/internal/snapshotinspect"
)

// InspectAndEnrich runs the snapshotinspect package on a completed
// snapshot directory and rewrites meta.json with structural details.
// Optional post-process — RunFetch doesn't call it (the next pipeline
// step parses the snapshot anyway). logger may be nil for silent
// operation.
func InspectAndEnrich(dir string, logger cmtlog.Logger) error {
	if logger != nil {
		logger.Info("inspecting", "dir", dir)
	}
	t0 := time.Now()
	res, err := snapshotinspect.Inspect(dir)
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}
	mdBytes, err := os.ReadFile(filepath.Join(dir, "metadata.bin"))
	if err != nil {
		return fmt.Errorf("read metadata.bin: %w", err)
	}
	hashes, err := snapshotinspect.ParseChunkHashes(mdBytes)
	if err != nil {
		return fmt.Errorf("parse chunk_hashes: %w", err)
	}

	mp := filepath.Join(dir, "meta.json")
	raw, err := os.ReadFile(mp)
	if err != nil {
		return fmt.Errorf("read meta.json: %w", err)
	}
	var current map[string]interface{}
	if err := json.Unmarshal(raw, &current); err != nil {
		return fmt.Errorf("parse meta.json: %w", err)
	}
	current["inspected_at"] = time.Now().UTC()
	current["decompressed_bytes"] = res.DecompressedBytes
	current["decompressed_bytes_human"] = snapshotinspect.HumanBytes(res.DecompressedBytes)
	current["total_items"] = res.TotalItems
	current["stores"] = res.Stores
	current["extensions"] = res.Extensions
	if len(res.UnknownItemTags) > 0 {
		current["unknown_item_tags"] = res.UnknownItemTags
	}
	current["chunk_hashes_hex"] = hashes
	enriched, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(mp, append(enriched, '\n'), 0o644); err != nil {
		return fmt.Errorf("write meta.json: %w", err)
	}
	if err := fsyncDir(dir); err != nil {
		return fmt.Errorf("fsync snapshot dir: %w", err)
	}
	if logger != nil {
		logger.Info("inspect complete",
			"items", res.TotalItems,
			"decompressed", snapshotinspect.HumanBytes(res.DecompressedBytes),
			"stores", len(res.Stores),
			"extensions", len(res.Extensions),
			"elapsed", time.Since(t0).Truncate(time.Millisecond))
	}
	return nil
}
