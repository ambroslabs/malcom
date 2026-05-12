package snapshotimport

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
)

// TestProcessStoreSegmentDoesNotLeakOnMalformedStream is the regression
// test for issue #108, finding C1: in wave-parallel mode,
// processStoreSegment's error-return paths bail without closing writeQ
// or tearing down the per-store parState. The writer goroutine, the
// dispatcher, and N hash workers (≈ NumCPU) end up blocked on cond.Wait
// / range chan forever — a real goroutine + ring-memory leak that fires
// on any malformed snapshot.
//
// The test feeds the wave-parallel path a minimal SnapshotItem stream
// that's well-formed at the wire level but malformed semantically: a
// StoreItem followed by an IAVL inner node with no children on the
// stack. addInnerPar's closesPairPar check rejects this with a "does
// not close a sibling pair" error, which exercises one of the leaking
// returns at parallel.go:887-889. The other return paths in the wpLoop
// share the same shape, so a fix that defers teardown will cover all
// of them.
//
// The test fails on a code base where the teardown is not deferred,
// and passes once the deferred close(writeQ) + s.par.closed=true
// recommended in the issue is in place.
func TestProcessStoreSegmentDoesNotLeakOnMalformedStream(t *testing.T) {
	dbDir := t.TempDir()
	db, err := pebble.Open(dbDir, &pebble.Options{
		DisableWAL: true,
	})
	if err != nil {
		t.Fatalf("pebble.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ring := newChunkRing(4<<20, 16<<20)
	stream := buildMalformedStoreThenStrayInner(t, "foo")
	if _, err := ring.Write(stream); err != nil {
		t.Fatalf("ring.Write: %v", err)
	}
	ring.Close(nil)

	ing := newFastIngester(db, t.TempDir(), nil)
	t.Cleanup(ing.cleanup)

	store := StoreEntry{
		Name:              "foo",
		DecompressedStart: 0,
		DecompressedEnd:   int64(len(stream)),
	}

	// The signatures we care about. The three function names are
	// stable identifiers in the stack-frame view: the wave-parallel
	// writer + decoder are anonymous funcs inside processStoreSegment
	// so they show up as processStoreSegment.func1 / .func2.
	leakSigs := []string{
		"snapshotimport.(*storeImporter).parDispatcher",
		"snapshotimport.(*storeImporter).parWorker",
		"snapshotimport.processStoreSegment.func",
	}

	// Pebble opened above spawns a stable set of background goroutines
	// (compaction, flush, table cache). Snapshot now so any pre-existing
	// matches are baselined out — though under normal pebble the leak
	// signatures shouldn't match anyway.
	runtime.GC()
	baseline := countLeakSignatures(allStacks(), leakSigs)

	_, _, _, _, err = processStoreSegment(
		ring, store, db, /*height*/ 1, ing,
		slog.New(slog.DiscardHandler),
		/*waveParallel*/ true,
		/*fastIngest*/ false,
	)
	if err == nil {
		t.Fatal("processStoreSegment returned nil on a malformed stream; expected sibling-pair error")
	}
	if !strings.Contains(err.Error(), "sibling pair") {
		t.Fatalf("expected sibling-pair error, got: %v", err)
	}

	// Poll until the leak signatures return to baseline, or the
	// deadline expires. On a correctly-fixed implementation the
	// teardown completes in microseconds, so the first iteration
	// succeeds. On buggy code the leaked goroutines are blocked on
	// cond.Wait / range chan with no code path that could wake them,
	// so we hit the deadline and fail with the diagnostic below.
	//
	// 5s gives enormous margin over the real teardown cost without
	// holding back the success path — success returns on the first
	// poll, not at the deadline.
	deadline := time.Now().Add(5 * time.Second)
	var stacks string
	var got map[string]int
	for {
		runtime.GC()
		stacks = allStacks()
		got = countLeakSignatures(stacks, leakSigs)
		if !exceedsBaseline(got, baseline, leakSigs) {
			return
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	counts := make([]string, len(leakSigs))
	for i, sig := range leakSigs {
		counts[i] = fmt.Sprintf("  %s: baseline=%d after=%d", sig, baseline[sig], got[sig])
	}
	t.Fatalf("processStoreSegment leaked goroutines after error-return:\n"+
		"%s\n\nerror: %v\n\nleaked stack frames:\n%s",
		strings.Join(counts, "\n"), err,
		extractMatchingFrames(stacks, leakSigs))
}

// ─── stream-builder + stack helpers ─────────────────────────────────────

// buildMalformedStoreThenStrayInner returns a SnapshotItem byte stream
// that decodes cleanly through snapReader and looks like:
//
//	StoreItem{name: storeName}
//	IAVLItem{height: 1, version: 1, key: "k"}
//
// The inner node arrives with nothing on the wave-parallel stack, so
// processStoreSegment's addNode → addInnerPar returns the
// "does not close a sibling pair" error.
func buildMalformedStoreThenStrayInner(t *testing.T, storeName string) []byte {
	t.Helper()

	storeItemMsg := append([]byte{0x0A, byte(len(storeName))}, storeName...)
	storeEnvBody := append([]byte{0x0A, byte(len(storeItemMsg))}, storeItemMsg...)

	iavlInner := []byte{
		0x0A, 0x01, 'k', // field 1 (key) length-delimited: "k"
		0x18, 0x01, // field 3 (version) varint: 1
		0x20, 0x01, // field 4 (height) varint: 1
	}
	iavlEnvBody := append([]byte{0x12, byte(len(iavlInner))}, iavlInner...)

	var out []byte
	out = appendUvarint(out, uint64(len(storeEnvBody)))
	out = append(out, storeEnvBody...)
	out = appendUvarint(out, uint64(len(iavlEnvBody)))
	out = append(out, iavlEnvBody...)
	return out
}

func appendUvarint(b []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(b, tmp[:n]...)
}

func allStacks() string {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return string(buf[:n])
		}
		buf = make([]byte, 2*len(buf))
	}
}

func countLeakSignatures(stacks string, sigs []string) map[string]int {
	out := make(map[string]int, len(sigs))
	for _, frame := range strings.Split(stacks, "\n\n") {
		for _, sig := range sigs {
			if strings.Contains(frame, sig) {
				out[sig]++
			}
		}
	}
	return out
}

func exceedsBaseline(got, baseline map[string]int, sigs []string) bool {
	for _, sig := range sigs {
		if got[sig] > baseline[sig] {
			return true
		}
	}
	return false
}

func extractMatchingFrames(stacks string, sigs []string) string {
	var matches []string
	for _, frame := range strings.Split(stacks, "\n\n") {
		for _, sig := range sigs {
			if strings.Contains(frame, sig) {
				matches = append(matches, frame)
				break
			}
		}
	}
	return strings.Join(matches, "\n\n")
}
