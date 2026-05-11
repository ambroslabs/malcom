package snapserve

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// CatalogConfig wires a Catalog up to its filesystem source and the
// reactor it should swap stores onto.
type CatalogConfig struct {
	// RootDir is the parent directory the catalog scans for snapshot
	// subdirs. Required.
	RootDir string

	// ChainID, if non-empty, filters out subdirs whose meta.json
	// chain_id doesn't match. One root can hold multiple chains'
	// snapshots, served by per-chain processes.
	ChainID string

	// VerifyMode is the integrity check applied to each candidate dir
	// on every scan. Aggregate-hash by default in the CLI.
	VerifyMode VerifyMode

	// RescanInterval is how often the background goroutine re-walks
	// RootDir. Default 30s. Zero disables periodic rescan — Trigger()
	// is the only way to refresh.
	RescanInterval time.Duration

	// OnStore is invoked after every successful (and changed) scan
	// with the new Store. Typically wires to reactor.SetProvider so
	// the inbound serve path sees the new catalogue.
	//
	// Called from the catalog goroutine; must not block long. A nil
	// OnStore is allowed (Current() is then the only consumer).
	OnStore func(*Store)

	// Logger is optional; pass nil for silent operation.
	Logger *slog.Logger
}

func (c *CatalogConfig) defaults() {
	if c.RescanInterval == 0 {
		c.RescanInterval = 30 * time.Second
	}
}

// Catalog owns the dir-scan rescan loop. It periodically calls
// LoadStoreFromRoot, compares the result to its last-known catalogue
// fingerprint, and (when something changed) emits the new Store via
// the OnStore hook set with SetOnStore. Trigger() forces an immediate
// rescan from any goroutine — hook it to SIGHUP at the CLI layer.
//
// Concurrency: Current(), Trigger(), Stop(), and SetOnStore() are all
// safe to call from any goroutine. The loop runs as a single
// background goroutine.
type Catalog struct {
	cfg CatalogConfig
	log *slog.Logger

	// store is the most recently emitted catalogue. Held in an
	// atomic.Pointer so Current() reads are lock-free and align with
	// statesync.Reactor's provider-swap pattern.
	store atomic.Pointer[Store]

	// onStore is swapped lock-free so the CLI can wire the reactor
	// hand-off after NewCatalog returns without racing the loop.
	onStore atomic.Pointer[onStoreSlot]

	// rescanMu serialises Rescan calls (so two concurrent triggers
	// don't both walk the dir at once) and guards fingerp. Reads of
	// store don't take this lock — they use the atomic.Pointer above.
	rescanMu sync.Mutex
	fingerp  string // catalogue fingerprint for change detection

	trigger  chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

// onStoreSlot wraps the OnStore callback so atomic.Pointer can hold
// it (atomic.Pointer needs a concrete type).
type onStoreSlot struct{ fn func(*Store) }

// NewCatalog constructs a Catalog. It does not start the rescan loop
// — call Rescan once manually for the initial scan, then launch the
// loop via the unexported loop method (RunServe pattern), or do both
// in one shot with the typical scan + go pattern.
func NewCatalog(cfg CatalogConfig) *Catalog {
	cfg.defaults()
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	c := &Catalog{
		cfg:     cfg,
		log:     log,
		trigger: make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	if cfg.OnStore != nil {
		c.onStore.Store(&onStoreSlot{fn: cfg.OnStore})
	}
	return c
}

// SetOnStore installs (or replaces) the callback invoked after every
// successful, *changed* rescan. Safe to call before or after the loop
// goroutine has started; pass nil to clear.
//
// The CLI uses this to wire the reactor hand-off after RunServe has
// constructed both the catalog and the reactor — they have an init
// ordering that NewCatalog alone can't satisfy.
func (c *Catalog) SetOnStore(fn func(*Store)) {
	if fn == nil {
		c.onStore.Store(nil)
		return
	}
	c.onStore.Store(&onStoreSlot{fn: fn})
}

// Stop shuts down the rescan loop. Safe to call multiple times and
// from multiple goroutines — the close fires at most once.
func (c *Catalog) Stop() {
	c.stopOnce.Do(func() { close(c.done) })
}

// Trigger requests an immediate rescan. Non-blocking — if a scan is
// already pending the trigger coalesces. Safe from any goroutine.
func (c *Catalog) Trigger() {
	select {
	case c.trigger <- struct{}{}:
	default:
	}
}

// Current returns the latest Store. Returns nil only before the first
// Rescan completes; once Rescan has returned successfully, Current is
// always non-nil (an empty rootDir yields an empty store, not nil).
func (c *Catalog) Current() *Store {
	return c.store.Load()
}

// Rescan does one synchronous scan + diff + emit pass. Exposed for
// tests and for the initial sync at startup. Concurrent Rescan calls
// serialise through rescanMu; scans are quick (file I/O on a small
// set of dirs).
func (c *Catalog) Rescan(ctx context.Context) error {
	t0 := time.Now()
	store, err := LoadStoreFromRoot(c.cfg.RootDir, c.cfg.ChainID, c.cfg.VerifyMode, c.log)
	if err != nil {
		return err
	}
	fp := storeFingerprint(store)

	c.rescanMu.Lock()
	changed := fp != c.fingerp
	c.fingerp = fp
	c.rescanMu.Unlock()
	c.store.Store(store)

	if changed {
		c.log.Info("catalog updated",
			"snapshots", store.Len(),
			"summary", store.Describe(),
			"elapsed", time.Since(t0).Truncate(time.Millisecond))
		if slot := c.onStore.Load(); slot != nil {
			slot.fn(store)
		}
	} else {
		c.log.Debug("catalog unchanged",
			"snapshots", store.Len(),
			"elapsed", time.Since(t0).Truncate(time.Millisecond))
	}
	// Surface ctx cancellation if it happened mid-scan so the caller
	// can shut down cleanly. Scans themselves don't observe ctx
	// because LoadStoreFromRoot is mostly file I/O — not worth the
	// complexity for the size of snapshot dirs we see.
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (c *Catalog) loop(ctx context.Context) {
	var ticker *time.Ticker
	var tickC <-chan time.Time
	if c.cfg.RescanInterval > 0 {
		ticker = time.NewTicker(c.cfg.RescanInterval)
		defer ticker.Stop()
		tickC = ticker.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-tickC:
			if err := c.Rescan(ctx); err != nil {
				c.log.Error("periodic rescan failed; serving prior catalogue",
					"err", err, "root", c.cfg.RootDir)
			}
		case <-c.trigger:
			if err := c.Rescan(ctx); err != nil {
				c.log.Error("triggered rescan failed; serving prior catalogue",
					"err", err, "root", c.cfg.RootDir)
			}
		}
	}
}

// storeFingerprint returns a short, deterministic identifier of a
// store's catalogue. Used purely for change detection: two stores
// with the same fingerprint advertise the same (height, format, hash,
// chunks) set. We hash (height, format, hash) rather than dirs
// because moving the same snapshot to a different subdir shouldn't
// count as a change.
func storeFingerprint(s *Store) string {
	if s == nil || len(s.snapshots) == 0 {
		return "empty"
	}
	keys := make([]string, len(s.snapshots))
	for i, ls := range s.snapshots {
		// Caller already verified hash matches metadata; hash is
		// enough to identify the snapshot content uniquely.
		keys[i] = ls.ChainID + "/" +
			strconv.FormatUint(ls.Height, 10) + "/" +
			strconv.FormatUint(uint64(ls.Format), 10) + "/" +
			strconv.FormatUint(uint64(ls.Chunks), 10) + "/" +
			hex.EncodeToString(ls.Hash)
	}
	sort.Strings(keys)
	h := sha256.Sum256([]byte(strings.Join(keys, "|")))
	return hex.EncodeToString(h[:8])
}
