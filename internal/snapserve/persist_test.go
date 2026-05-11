package snapserve

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestPersistLoop_TicksAndSaves(t *testing.T) {
	var bookSaves, bansSaves atomic.Int32
	saveBook := func() { bookSaves.Add(1) }
	saveBans := func() error { bansSaves.Add(1); return nil }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		runPersistLoop(ctx, 20*time.Millisecond, saveBook, saveBans, discardLogger())
		close(done)
	}()

	// Wait until both savers have run at least twice — confirms the
	// loop is actually ticking, not just running once. 200ms gives
	// the 20ms ticker comfortable room without making the test slow.
	deadline := time.After(500 * time.Millisecond)
	for {
		if bookSaves.Load() >= 2 && bansSaves.Load() >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected ≥2 ticks; got book=%d bans=%d", bookSaves.Load(), bansSaves.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runPersistLoop did not exit within 1s of ctx cancel")
	}
}

func TestPersistLoop_ZeroIntervalIsNoOp(t *testing.T) {
	var bookSaves atomic.Int32
	saveBook := func() { bookSaves.Add(1) }
	saveBans := func() error { return nil }

	// interval=0 should return immediately without ever firing the
	// savers. We let the goroutine run for a bit, then cancel and
	// assert nothing happened. The point is: callers passing 0
	// deliberately mean "don't persist," not "persist as fast as
	// possible" — which is what a misfired ticker would imply.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		runPersistLoop(ctx, 0, saveBook, saveBans, discardLogger())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("interval=0 should return immediately")
	}
	if got := bookSaves.Load(); got != 0 {
		t.Fatalf("interval=0 should never call saveBook; got %d", got)
	}
}

func TestPersistLoop_BanlistSaveErrorIsLoggedAndContinues(t *testing.T) {
	// A failed banlist save shouldn't kill the loop — the
	// addrbook save is still useful, and the next tick will try
	// the banlist again (in case the failure was transient: disk
	// full briefly, fsync hiccup).
	var bookSaves, bansAttempts atomic.Int32
	saveBook := func() { bookSaves.Add(1) }
	saveBans := func() error {
		bansAttempts.Add(1)
		return errSentinel
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		runPersistLoop(ctx, 10*time.Millisecond, saveBook, saveBans, discardLogger())
		close(done)
	}()

	deadline := time.After(300 * time.Millisecond)
	for {
		if bookSaves.Load() >= 3 && bansAttempts.Load() >= 3 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("loop should keep ticking past a banlist save error; book=%d bans=%d",
				bookSaves.Load(), bansAttempts.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	<-done
}

type sentinelErr struct{}

func (sentinelErr) Error() string { return "banlist save failed (test)" }

var errSentinel = sentinelErr{}
