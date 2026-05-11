package snapserve

import (
	"context"
	"log/slog"
	"time"
)

// runPersistLoop ticks at interval and calls saveBook + saveBans to
// flush peer-state to disk. Cancelled via ctx; the clean-shutdown
// defer in RunServe runs its own final save regardless, so the only
// state this loop captures is what would otherwise be lost in a
// crash, OOM, or SIGKILL between scheduled saves.
//
// Both savers are expected to be safe to call concurrently with the
// PEX gossip / dial-failure recording goroutines (the cometbft
// AddrBook serialises its on-disk file write under an internal mutex;
// banlist.Set uses its own). The CLI wires them up via small closures
// over book.Save and bans.Save.
//
// Saves are unconditional — addrbook serialise is small (KB-range)
// and the loop runs on a minutes-scale tick, so we don't bother
// tracking "did anything change since last save". An interval of 0
// is a no-op (returns immediately) — callers who want persistence
// off can set 0, callers who want it on use the default 5m.
func runPersistLoop(
	ctx context.Context,
	interval time.Duration,
	saveBook func(),
	saveBans func() error,
	log *slog.Logger,
) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			saveBook()
			if err := saveBans(); err != nil {
				log.Error("periodic banlist save failed", "err", err)
				continue
			}
			log.Debug("periodic state persisted")
		}
	}
}
