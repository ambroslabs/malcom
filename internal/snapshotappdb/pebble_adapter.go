package snapshotappdb

import (
	"errors"
	"fmt"
	"math"

	corestore "cosmossdk.io/core/store"
	"github.com/cockroachdb/pebble"
)

// pebbleDB wraps *pebble.DB so it satisfies iavl/db.DB and
// corestore.KVStoreWithBatch. Note: gaiad's "pebbledb" backend in
// cometbft-db uses the same pebble library and an analogous adapter,
// so a database we produce here is binary-compatible with what gaiad
// would read when configured with `db_backend = "pebbledb"`.
type pebbleDB struct {
	db *pebble.DB
}

// openPebbleDB opens an application.db in bulk-load mode: automatic
// compactions disabled, very high L0 thresholds (so writes never stall
// on L0 file count), large memtable. The final compaction is run
// explicitly in FinalCompact() after all writes are complete. This
// matches the design in issue #3 — see that issue for context.
func openPebbleDB(dir string) (*pebbleDB, error) {
	opts := &pebble.Options{
		// Disable L0+ compactions during bulk load — those are the
		// expensive ones we want to defer to the end. Memtable
		// flushes still run; pebble counts them as compactions for
		// MaxConcurrentCompactions purposes, so we keep that >0.
		DisableAutomaticCompactions: true,
		L0CompactionThreshold:       math.MaxInt32,
		L0StopWritesThreshold:       math.MaxInt32,

		// MaxConcurrentCompactions gates memtable→L0 flushes too, not
		// just L0+ compactions. Setting this to 0 blocks flushes and
		// causes the importer to stall against the in-flight
		// memtable cap. Give flushes plenty of budget.
		MaxConcurrentCompactions: func() int { return 4 },

		// Big memtable + many in-flight memtables so memtable flush
		// never blocks the importer either.
		MemTableSize:                1 << 30, // 1 GB
		MemTableStopWritesThreshold: 8,

		// Cache helps both the importer (for any reads it does
		// internally) and the final compaction phase.
		Cache:        pebble.NewCache(2 << 30), // 2 GB
		MaxOpenFiles: 4096,
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &pebbleDB{db: db}, nil
}

// FinalCompact flushes the memtable and runs a full-keyspace compaction.
// Call after bulk writes are complete and before Close. parallelize=true
// uses all available cores. Skips silently if the DB has no data.
func (p *pebbleDB) FinalCompact() error {
	if err := p.db.Flush(); err != nil {
		return fmt.Errorf("pebble flush: %w", err)
	}
	// Pebble's Compact is exclusive on end. Use the full byte range.
	start := []byte{0x00}
	end := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	if err := p.db.Compact(start, end, true); err != nil {
		return fmt.Errorf("pebble compact: %w", err)
	}
	return nil
}

func (p *pebbleDB) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("empty key")
	}
	val, closer, err := p.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(val))
	copy(out, val)
	closer.Close()
	return out, nil
}

func (p *pebbleDB) Has(key []byte) (bool, error) {
	if len(key) == 0 {
		return false, errors.New("empty key")
	}
	_, closer, err := p.db.Get(key)
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	closer.Close()
	return true, nil
}

func (p *pebbleDB) Set(key, value []byte) error {
	return p.db.Set(key, value, pebble.NoSync)
}

func (p *pebbleDB) SetSync(key, value []byte) error {
	return p.db.Set(key, value, pebble.Sync)
}

func (p *pebbleDB) Delete(key []byte) error {
	return p.db.Delete(key, pebble.NoSync)
}

func (p *pebbleDB) DeleteSync(key []byte) error {
	return p.db.Delete(key, pebble.Sync)
}

func (p *pebbleDB) Iterator(start, end []byte) (corestore.Iterator, error) {
	if start != nil && len(start) == 0 {
		return nil, errors.New("empty (non-nil) start")
	}
	if end != nil && len(end) == 0 {
		return nil, errors.New("empty (non-nil) end")
	}
	it, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return nil, err
	}
	it.First()
	return &pebbleIter{it: it, start: start, end: end, reverse: false}, nil
}

func (p *pebbleDB) ReverseIterator(start, end []byte) (corestore.Iterator, error) {
	if start != nil && len(start) == 0 {
		return nil, errors.New("empty (non-nil) start")
	}
	if end != nil && len(end) == 0 {
		return nil, errors.New("empty (non-nil) end")
	}
	it, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	if err != nil {
		return nil, err
	}
	it.Last()
	return &pebbleIter{it: it, start: start, end: end, reverse: true}, nil
}

func (p *pebbleDB) Close() error {
	return p.db.Close()
}

func (p *pebbleDB) NewBatch() corestore.Batch {
	return &pebbleBatch{db: p.db, batch: p.db.NewBatch()}
}

func (p *pebbleDB) NewBatchWithSize(size int) corestore.Batch {
	return &pebbleBatch{db: p.db, batch: p.db.NewBatchWithSize(size)}
}

// Print is a debug helper required by iavl/db.DB. We don't need it.
func (p *pebbleDB) Print() error { return nil }

// Stats is required by iavl/db.DB. Return empty map.
func (p *pebbleDB) Stats() map[string]string { return map[string]string{} }

// ─── iterator ────────────────────────────────────────────────────────────

type pebbleIter struct {
	it      *pebble.Iterator
	start   []byte
	end     []byte
	reverse bool
}

func (i *pebbleIter) Domain() (start, end []byte) { return i.start, i.end }
func (i *pebbleIter) Valid() bool                 { return i.it.Valid() }

func (i *pebbleIter) Next() {
	if i.reverse {
		i.it.Prev()
	} else {
		i.it.Next()
	}
}

func (i *pebbleIter) Key() []byte {
	if !i.it.Valid() {
		panic("invalid iterator")
	}
	k := i.it.Key()
	out := make([]byte, len(k))
	copy(out, k)
	return out
}

func (i *pebbleIter) Value() []byte {
	if !i.it.Valid() {
		panic("invalid iterator")
	}
	v := i.it.Value()
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

func (i *pebbleIter) Error() error { return i.it.Error() }

func (i *pebbleIter) Close() error { return i.it.Close() }

// ─── batch ────────────────────────────────────────────────────────────

type pebbleBatch struct {
	db    *pebble.DB
	batch *pebble.Batch
}

func (b *pebbleBatch) Set(key, value []byte) error {
	return b.batch.Set(key, value, nil)
}

func (b *pebbleBatch) Delete(key []byte) error {
	return b.batch.Delete(key, nil)
}

func (b *pebbleBatch) Write() error {
	if err := b.batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("pebble batch commit: %w", err)
	}
	return nil
}

func (b *pebbleBatch) WriteSync() error {
	if err := b.batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("pebble batch commit (sync): %w", err)
	}
	return nil
}

func (b *pebbleBatch) Close() error { return b.batch.Close() }

func (b *pebbleBatch) GetByteSize() (int, error) {
	return int(b.batch.Len()), nil
}
