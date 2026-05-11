package snapfetch

import "errors"

// Sentinel errors for failure-mode classification. RunFetch wraps
// returned errors with one of these so orchestrators (and the CLI's
// exit-code mapper) can branch on the category via errors.Is — for
// example, "no peers available" is worth retrying with a different
// bootstrap list, while "disk error" is not.
//
// Errors that don't fit any category are left uncategorized; the CLI
// reports them as the generic exit code.
var (
	// ErrNoPeers — the configured bootstrap_peers list and persisted
	// addrbook produced an empty pool. Nothing to dial.
	ErrNoPeers = errors.New("no peers available")

	// ErrWalkFailed — the walk phase exhausted every candidate height
	// without finding a peer that served chunk-0, or the configured
	// [min,max]/interval window contained no targets at all.
	ErrWalkFailed = errors.New("walk failed: no servable snapshot")

	// ErrDownloadFailed — chunk-fetch phase failed mid-stream: every
	// peer was banned before completion, parent ctx was cancelled, etc.
	ErrDownloadFailed = errors.New("download failed")

	// ErrDiskFailed — a filesystem operation against the snapshot
	// output directory failed (mkdir/fsync/write/read). Distinct from
	// ErrDownloadFailed because the network was fine; the local disk
	// (full, read-only, permissions) is the problem.
	ErrDiskFailed = errors.New("disk error")
)
