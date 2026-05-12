// Pipeline orchestration for `malcom snapshot fetch --import`. The
// orchestrator wires snapfetch's OnDownloadReady + OnChunkReady
// callbacks to a TailingChunkSource, which the parallel importer
// consumes as its decompressed-stream source. Fetch keeps
// persisting chunks to disk as before; the importer just reads them
// out earlier than it would in the two-step path.
//
// Why disk-persisted (vs. an in-memory hand-off): the chunk ring's
// natural backpressure would propagate up to fetch, but fetch is
// not a pull-based reader. Chunks are pushed by peers; pausing
// would mean disconnecting peers and re-establishing them, which is
// expensive and disrupts the carefully-built peer set. Disk
// decouples the rates entirely, at the cost of snapshot disk
// space the user can `rm -rf` after.

package snapshotfetch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ambroslabs/malcom/internal/config"
	"github.com/ambroslabs/malcom/internal/snapshotimport"
)

// pipelineState owns the import goroutine + tailing source for a
// single `fetch --import` run.
type pipelineState struct {
	chain          config.Chain
	importOutDir   string
	noExtensions   bool
	parallelOpts   pipelineImportTuning
	logger         *slog.Logger
	cancelFetchCtx context.CancelFunc

	mu         sync.Mutex
	tailing    *snapshotimport.TailingChunkSource
	importDone chan importResult
	started    bool

	// appdbOut + appdbHeight are populated by onDownloadReady so the
	// caller can chain into post-import verify without re-deriving the
	// path. Empty until OnDownloadReady fires.
	appdbOut    string
	appdbHeight int64
}

// pipelineImportTuning carries the knobs we expose for the
// pipelined import. Kept narrow on purpose — operators wanting deep
// import tuning (cpuprofile, mem-stats, fast-ingest A/B) should run
// the two-step path. The pipelined path always uses ImportParallel
// (the chunk-ring streaming architecture is what makes pipelining
// possible at sub-chunk granularity).
type pipelineImportTuning struct {
	Workers      int
	ChunkMB      int
	WaveParallel bool
	FastIngest   bool
}

type importResult struct {
	stats *snapshotimport.Stats
	err   error
}

// onDownloadReady is wired to snapfetch.Config.OnDownloadReady. It
// stands up the tailing source + import goroutine after fetch has
// chosen an offer and prepared the snapshot dir. height is the
// offer's actual height — which can be ≥ --target-height because
// walk's match is range-based, not exact.
func (p *pipelineState) onDownloadReady(snapDir string, height uint64, totalChunks uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return errors.New("pipeline: onDownloadReady called twice")
	}
	p.started = true

	p.tailing = snapshotimport.NewTailingChunkSource(snapDir, totalChunks)
	p.importDone = make(chan importResult, 1)

	importLog := p.logger.With("module", "import-cli")

	importHeight := int64(height)
	outDir := fmt.Sprintf("%s/appdb_%s_%d", p.importOutDir, p.chain.ChainID, importHeight)
	p.appdbOut = outDir
	p.appdbHeight = importHeight
	importLog.Info("pipeline: starting import",
		"snapshot", snapDir,
		"appdb", outDir,
		"height", importHeight,
		"chunks", totalChunks)

	opts := snapshotimport.ParallelOptions{
		Options: snapshotimport.Options{
			SnapshotDir:              snapDir,
			OutDir:                   outDir,
			Height:                   importHeight,
			NoExtensions:             p.noExtensions,
			MemtableMB:               p.chain.Import.MemtableMB,
			CacheMB:                  p.chain.Import.CacheMB,
			MaxConcurrentCompactions: p.chain.Compact.MaxConcurrentCompactions,
			FlushSplitMB:             p.chain.Import.FlushSplitMB,
			CompactDuringImport:      p.chain.Import.CompactDuringImport,
			Log:                      p.logger,
		},
		Workers:            p.parallelOpts.Workers,
		ChunkMB:            p.parallelOpts.ChunkMB,
		WaveParallel:       p.parallelOpts.WaveParallel,
		FastIngest:         p.parallelOpts.FastIngest,
		DecompressedSource: p.tailing,
		AllowIncomplete:    true,
	}

	go func() {
		stats, err := snapshotimport.ImportParallel(opts)
		p.importDone <- importResult{stats: stats, err: err}
		close(p.importDone)

		// If the import failed, cancel the fetch — there's no point
		// continuing to download chunks for an import we can't finish.
		if err != nil && p.cancelFetchCtx != nil {
			importLog.Error("pipeline: import failed, cancelling fetch", "err", err)
			p.cancelFetchCtx()
		}
	}()

	return nil
}

// onChunkReady is wired to snapfetch.Config.OnChunkReady. Forwards
// to the tailing source so it can advance through the chunk stream.
func (p *pipelineState) onChunkReady(idx uint32) {
	p.mu.Lock()
	t := p.tailing
	p.mu.Unlock()
	if t != nil {
		t.MarkReady(idx)
	}
}

// finalize collects the import result. fetchErr is the error (if
// any) returned by snapfetch.RunFetch. Returns the import error if
// import failed, or fetchErr otherwise. Always waits for the import
// goroutine to exit so we don't leak it.
func (p *pipelineState) finalize(fetchErr error) (*snapshotimport.Stats, error) {
	p.mu.Lock()
	t := p.tailing
	doneCh := p.importDone
	p.mu.Unlock()

	if !p.started {
		// Fetch failed before OnDownloadReady fired (walk failure,
		// config error). Nothing to wait on.
		return nil, fetchErr
	}

	if fetchErr != nil {
		// Wake the import side so it doesn't hang on a missing chunk.
		t.Fail(fmt.Errorf("fetch failed: %w", fetchErr))
	}

	res := <-doneCh

	switch {
	case res.err != nil:
		// Prefer the import error: when the import goroutine fails it
		// cancels the fetch context, so fetchErr will be
		// context.Canceled — which masks the real root cause if we
		// surfaced it instead.
		return res.stats, res.err
	case fetchErr != nil:
		return res.stats, fetchErr
	default:
		return res.stats, nil
	}
}
