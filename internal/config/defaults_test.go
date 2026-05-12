package config

import "testing"

// TestImportMemtableMBDefault pins the import memtable size default
// at 256 MiB. The previous 1024 default OOM-killed 8 GiB hosts —
// pebble keeps up to MemTableStopWritesThreshold=4 memtables alive
// simultaneously, and on CGO_ENABLED=0 builds the arenas live in
// the Go heap, so peak resident ≈ MemtableMB × 4. See #103 for the
// failure mode.
//
// Anyone raising this back toward 1024 has to update the test —
// forcing the trade-off back into review.
func TestImportMemtableMBDefault(t *testing.T) {
	var im ImportTuning
	applyImportDefaults(&im)
	const want = 256
	if im.MemtableMB != want {
		t.Fatalf("MemtableMB default = %d, want %d (lowering would OOM 8 GiB hosts; see #103)",
			im.MemtableMB, want)
	}
}
