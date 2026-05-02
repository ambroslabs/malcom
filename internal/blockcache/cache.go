// Package blockcache stores a sliding window of raw cometbft Block proto
// bytes — keyed by height, capacity ~1000 — so we can serve BlockRequest /
// StatusRequest from peers without re-marshaling.
//
// Why raw bytes:
// - A BlockResponse is byte-for-byte what we received over the wire. Storing
//   the decoded *types.Block and re-marshaling on serve is not guaranteed to
//   produce the same bytes (gogoproto canonicalization caveats), and we don't
//   want to risk subtle hash mismatches at the consumer.
// - It's also faster on the serve path: we just blit the cached bytes into
//   the wire envelope.
//
// Storage layout on disk: data/blocks/<height>.bin = raw cmtproto.Block bytes.
// At process start we walk the directory and load anything contiguous near the
// tip into memory; older entries are pruned to keep the disk footprint bounded.
package blockcache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cosmos/gogoproto/proto"
)

const (
	// DefaultCapacity is the sliding-window size in blocks.
	DefaultCapacity = 1000
)

// Cache is a height-indexed ring of raw block bytes plus matching parsed
// protos for cheap inspection. All methods are safe for concurrent use.
type Cache struct {
	mu      sync.RWMutex
	dir     string
	cap     int
	raw     map[int64][]byte        // height → raw cmtproto.Block bytes
	parsed  map[int64]*cmtproto.Block
	tip     int64
	base    int64
}

// New creates an empty cache and prepares the on-disk directory.
// If existing files are present in dir, they are loaded so we resume across
// restarts.
func New(dir string, capacity int) (*Cache, error) {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	c := &Cache{
		dir:    dir,
		cap:    capacity,
		raw:    make(map[int64][]byte, capacity),
		parsed: make(map[int64]*cmtproto.Block, capacity),
	}
	if err := c.loadFromDisk(); err != nil {
		return nil, err
	}
	return c, nil
}

// Capacity returns the configured window size.
func (c *Cache) Capacity() int { return c.cap }

// Range returns the current (base, tip). When the cache is empty both are 0.
func (c *Cache) Range() (base, tip int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.base, c.tip
}

// Has reports whether height H is currently cached.
func (c *Cache) Has(h int64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.raw[h]
	return ok
}

// GetRaw returns the raw cmtproto.Block bytes for height h, or nil if absent.
func (c *Cache) GetRaw(h int64) []byte {
	c.mu.RLock()
	defer c.mu.RUnlock()
	b := c.raw[h]
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// GetParsed returns the decoded cmtproto.Block at h, or nil.
func (c *Cache) GetParsed(h int64) *cmtproto.Block {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.parsed[h]
}

// Put inserts (or overwrites) the block at the given height. The raw bytes
// are persisted to disk before the in-memory entry becomes visible. If the
// resulting tip-base spread exceeds capacity, oldest heights are evicted.
//
// Returns an error if the bytes don't decode as a cmtproto.Block, or if the
// embedded Header.Height disagrees with h.
func (c *Cache) Put(h int64, raw []byte) error {
	if h <= 0 {
		return fmt.Errorf("invalid height %d", h)
	}
	var pb cmtproto.Block
	if err := proto.Unmarshal(raw, &pb); err != nil {
		return fmt.Errorf("decode block at %d: %w", h, err)
	}
	if pb.Header.Height != h {
		return fmt.Errorf("height mismatch: caller said %d, header says %d", h, pb.Header.Height)
	}

	// Write through to disk first so we never lose a record we acked in RAM.
	if err := c.writeFile(h, raw); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.raw[h] = raw
	c.parsed[h] = &pb
	if h > c.tip {
		c.tip = h
	}
	c.evictLocked()
	return nil
}

// evictLocked drops heights below tip-cap+1 from RAM and disk, and recomputes
// base as the smallest height we still have. Must be called with mu held.
func (c *Cache) evictLocked() {
	if c.tip == 0 {
		c.base = 0
		return
	}
	floor := c.tip - int64(c.cap) + 1
	if floor < 1 {
		floor = 1
	}
	for h := range c.raw {
		if h < floor {
			delete(c.raw, h)
			delete(c.parsed, h)
			_ = os.Remove(c.filePath(h))
		}
	}
	c.base = c.minHeightLocked()
}

func (c *Cache) minHeightLocked() int64 {
	var min int64
	for h := range c.raw {
		if min == 0 || h < min {
			min = h
		}
	}
	return min
}

// Stats returns a snapshot of cache vitals.
type Stats struct {
	Count    int
	Base     int64
	Tip      int64
	Capacity int
	BytesRAM int64
}

func (c *Cache) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var bytes int64
	for _, b := range c.raw {
		bytes += int64(len(b))
	}
	return Stats{
		Count:    len(c.raw),
		Base:     c.base,
		Tip:      c.tip,
		Capacity: c.cap,
		BytesRAM: bytes,
	}
}

// MissingHeights returns the heights in [from, to] (inclusive on both ends)
// that are NOT currently cached, capped at limit. Useful for the sync loop.
func (c *Cache) MissingHeights(from, to int64, limit int) []int64 {
	if from > to || limit <= 0 {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]int64, 0, limit)
	for h := from; h <= to && len(out) < limit; h++ {
		if _, ok := c.raw[h]; !ok {
			out = append(out, h)
		}
	}
	return out
}

func (c *Cache) filePath(h int64) string {
	return filepath.Join(c.dir, fmt.Sprintf("%d.bin", h))
}

func (c *Cache) writeFile(h int64, raw []byte) error {
	tmp := c.filePath(h) + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := os.Rename(tmp, c.filePath(h)); err != nil {
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// loadFromDisk scans the cache dir for *.bin and loads any heights within
// the most recent capacity window. Heights outside the window are deleted.
func (c *Cache) loadFromDisk() error {
	ents, err := os.ReadDir(c.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	heights := make([]int64, 0, len(ents))
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, ".bin") {
			continue
		}
		h, err := strconv.ParseInt(strings.TrimSuffix(name, ".bin"), 10, 64)
		if err != nil {
			continue
		}
		heights = append(heights, h)
	}
	if len(heights) == 0 {
		return nil
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] > heights[j] })
	tip := heights[0]
	floor := tip - int64(c.cap) + 1
	if floor < 1 {
		floor = 1
	}
	for _, h := range heights {
		if h < floor {
			_ = os.Remove(c.filePath(h))
			continue
		}
		raw, err := os.ReadFile(c.filePath(h))
		if err != nil {
			continue
		}
		var pb cmtproto.Block
		if err := proto.Unmarshal(raw, &pb); err != nil {
			// Corrupt file; drop.
			_ = os.Remove(c.filePath(h))
			continue
		}
		c.raw[h] = raw
		c.parsed[h] = &pb
		if h > c.tip {
			c.tip = h
		}
	}
	c.base = c.minHeightLocked()
	return nil
}
