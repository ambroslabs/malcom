package archive

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Store is a directory of shards. Opens shards lazily on first access and
// keeps them open for fast subsequent operations.
type Store struct {
	root string

	mu     sync.Mutex
	shards map[uint64]*Shard
}

// New opens (or creates) a Store rooted at dir.
// Layout: <dir>/shards/<base>.blocks + .idx
func New(dir string) (*Store, error) {
	shardsDir := filepath.Join(dir, "shards")
	if err := os.MkdirAll(shardsDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir shards: %w", err)
	}
	return &Store{
		root:   dir,
		shards: make(map[uint64]*Shard),
	}, nil
}

// Root returns the on-disk directory.
func (s *Store) Root() string { return s.root }

// Close releases all open shards.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, sh := range s.shards {
		if err := sh.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	s.shards = nil
	return firstErr
}

// shard returns (and opens lazily) the shard for height h.
func (s *Store) shard(h uint64) (*Shard, error) {
	base := ShardBase(h)
	s.mu.Lock()
	defer s.mu.Unlock()
	if sh, ok := s.shards[base]; ok {
		return sh, nil
	}
	bp, ip := s.shardPaths(base)
	sh, err := Open(bp, ip, base)
	if err != nil {
		return nil, err
	}
	s.shards[base] = sh
	return sh, nil
}

func (s *Store) shardPaths(base uint64) (blocks, idx string) {
	name := fmt.Sprintf("%010d", base)
	return filepath.Join(s.root, "shards", name+".blocks"),
		filepath.Join(s.root, "shards", name+".idx")
}

// Has reports whether the given height is present.
func (s *Store) Has(h uint64) bool {
	sh, err := s.shard(h)
	if err != nil {
		return false
	}
	return sh.Has(h)
}

// Get fetches raw block bytes for a height. Returns ErrNotPresent if absent.
func (s *Store) Get(h uint64) ([]byte, error) {
	sh, err := s.shard(h)
	if err != nil {
		return nil, err
	}
	return sh.Get(h)
}

// Put writes block bytes for a height. Idempotent.
func (s *Store) Put(h uint64, blockBytes []byte) error {
	sh, err := s.shard(h)
	if err != nil {
		return err
	}
	return sh.Put(h, blockBytes)
}

// Sync fsyncs every open shard.
func (s *Store) Sync() error {
	s.mu.Lock()
	shards := make([]*Shard, 0, len(s.shards))
	for _, sh := range s.shards {
		shards = append(shards, sh)
	}
	s.mu.Unlock()
	for _, sh := range shards {
		if err := sh.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// ShardBases returns every shard base height present on disk, sorted asc.
// Reads the directory, parses the filenames; doesn't open the shards.
func (s *Store) ShardBases() ([]uint64, error) {
	ents, err := os.ReadDir(filepath.Join(s.root, "shards"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	seen := map[uint64]bool{}
	for _, e := range ents {
		name := e.Name()
		if !strings.HasSuffix(name, ".idx") {
			continue
		}
		base, err := strconv.ParseUint(strings.TrimSuffix(name, ".idx"), 10, 64)
		if err != nil {
			continue
		}
		seen[base] = true
	}
	out := make([]uint64, 0, len(seen))
	for b := range seen {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// Range is a contiguous run of present heights.
type Range struct {
	Lo uint64 // inclusive
	Hi uint64 // inclusive
}

// Count returns the number of heights in this range.
func (r Range) Count() uint64 { return r.Hi - r.Lo + 1 }

// Ranges scans every on-disk shard and returns the contiguous present
// runs of heights. The returned slice is sorted ascending.
//
// Implementation reads each shard's idx (1.6 MB) once. With 257 shards
// covering 25.7 M heights this is ~412 MB of sequential I/O, dominated by
// page cache after the first run.
func (s *Store) Ranges() ([]Range, uint64, error) {
	bases, err := s.ShardBases()
	if err != nil {
		return nil, 0, err
	}
	var (
		out   []Range
		total uint64
		cur   *Range
	)
	flush := func() {
		if cur != nil {
			out = append(out, *cur)
			cur = nil
		}
	}
	for _, base := range bases {
		sh, err := s.shard(base)
		if err != nil {
			return nil, 0, err
		}
		bm, err := sh.PresentBitmap()
		if err != nil {
			return nil, 0, err
		}
		// Walk this shard's bitmap, extending or starting runs.
		for i := 0; i < ChunkSize; i++ {
			present := bm[i/8]&(1<<(uint(i)%8)) != 0
			h := base + uint64(i)
			if present {
				total++
				if cur == nil {
					cur = &Range{Lo: h, Hi: h}
				} else if h == cur.Hi+1 {
					cur.Hi = h
				} else {
					flush()
					cur = &Range{Lo: h, Hi: h}
				}
			}
		}
	}
	flush()
	return out, total, nil
}

// FSCKAll runs a deep consistency check on every shard. Reports indexed
// by base height, in ascending order.
func (s *Store) FSCKAll(maxCRCPerShard int) (map[uint64]FSCKReport, error) {
	bases, err := s.ShardBases()
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]FSCKReport, len(bases))
	for _, b := range bases {
		sh, err := s.shard(b)
		if err != nil {
			return out, err
		}
		rep, err := sh.FSCK(maxCRCPerShard)
		if err != nil {
			return out, fmt.Errorf("fsck shard %d: %w", b, err)
		}
		out[b] = rep
	}
	return out, nil
}

// Missing returns the gap ranges between Lo and Hi (inclusive) that are
// not present on disk. Useful for the downloader's work-queue.
func (s *Store) Missing(lo, hi uint64) ([]Range, uint64, error) {
	if lo > hi {
		return nil, 0, fmt.Errorf("missing: lo > hi (%d > %d)", lo, hi)
	}
	have, _, err := s.Ranges()
	if err != nil {
		return nil, 0, err
	}
	var (
		out     []Range
		missing uint64
		cursor  = lo
	)
	for _, r := range have {
		if r.Hi < lo || r.Lo > hi {
			continue
		}
		// clip
		rl, rh := r.Lo, r.Hi
		if rl < lo {
			rl = lo
		}
		if rh > hi {
			rh = hi
		}
		if cursor < rl {
			out = append(out, Range{Lo: cursor, Hi: rl - 1})
			missing += rl - cursor
		}
		if rh+1 > cursor {
			cursor = rh + 1
		}
	}
	if cursor <= hi {
		out = append(out, Range{Lo: cursor, Hi: hi})
		missing += hi - cursor + 1
	}
	return out, missing, nil
}
