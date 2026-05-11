package snapserve

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"sort"
	"strings"
	"sync"
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
// OnStore. Trigger() forces an immediate rescan from any goroutine —
// hook it to SIGHUP at the CLI layer.
//
// Concurrency: Current() and Trigger() are safe to call from any
// goroutine. Start launches a single background goroutine; Stop ends
// it.
type Catalog struct {
	cfg CatalogConfig
	log *slog.Logger

	mu      sync.RWMutex
	store   *Store
	fingerp string // catalogue fingerprint for change detection

	trigger chan struct{}
	done    chan struct{}
}

// NewCatalog constructs a Catalog. It does not start the rescan loop
// — call Start, or call Rescan once manually if you don't want a
// background goroutine.
func NewCatalog(cfg CatalogConfig) *Catalog {
	cfg.defaults()
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	return &Catalog{
		cfg:     cfg,
		log:     log,
		trigger: make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
}

// Start runs the rescan loop on a new goroutine. Returns after firing
// an initial synchronous scan so the caller sees a populated Current()
// (or a hard error from the root dir) before serving begins.
func (c *Catalog) Start(ctx context.Context) error {
	// Initial scan is synchronous and fatal-on-error: a missing
	// rootDir is operator misconfig and we want the server to fail
	// loudly rather than serving zero snapshots.
	if err := c.Rescan(ctx); err != nil {
		return err
	}
	go c.loop(ctx)
	return nil
}

// Stop shuts down the rescan loop. Safe to call multiple times.
func (c *Catalog) Stop() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}

// Trigger requests an immediate rescan. Non-blocking — if a scan is
// already pending the trigger coalesces. Safe from any goroutine.
func (c *Catalog) Trigger() {
	select {
	case c.trigger <- struct{}{}:
	default:
	}
}

// Current returns the latest Store. Returns nil only before the
// initial Rescan; once Start has returned successfully, Current is
// always non-nil (an empty rootDir yields an empty store, not nil).
func (c *Catalog) Current() *Store {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.store
}

// Rescan does one synchronous scan + diff + emit pass. Exposed for
// tests and for the initial sync at startup. Concurrent Rescan calls
// serialize through the same mutex; that's fine, scans are quick.
func (c *Catalog) Rescan(ctx context.Context) error {
	t0 := time.Now()
	store, err := LoadStoreFromRoot(c.cfg.RootDir, c.cfg.ChainID, c.cfg.VerifyMode, c.log)
	if err != nil {
		return err
	}
	fp := storeFingerprint(store)

	c.mu.Lock()
	changed := fp != c.fingerp
	c.store = store
	c.fingerp = fp
	c.mu.Unlock()

	if changed {
		c.log.Info("catalog updated",
			"snapshots", store.Len(),
			"summary", store.Describe(),
			"elapsed", time.Since(t0).Truncate(time.Millisecond))
		if c.cfg.OnStore != nil {
			c.cfg.OnStore(store)
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
		keys[i] = ls.ChainID + "/" + uintToString(ls.Height) + "/" +
			uintToString(uint64(ls.Format)) + "/" + uintToString(uint64(ls.Chunks)) +
			"/" + hex.EncodeToString(ls.Hash)
	}
	sort.Strings(keys)
	h := sha256.Sum256([]byte(strings.Join(keys, "|")))
	return hex.EncodeToString(h[:8])
}

func uintToString(v uint64) string {
	// Tiny zero-alloc itoa-ish helper for fingerprint hashing.
	// strconv.FormatUint is fine too; this exists to avoid pulling
	// strconv into the import list for one call site.
	if v == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for v > 0 {
		pos--
		b[pos] = byte('0' + v%10)
		v /= 10
	}
	return string(b[pos:])
}
