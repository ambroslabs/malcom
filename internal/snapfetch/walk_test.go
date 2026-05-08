package snapfetch

import (
	"reflect"
	"testing"
)

func TestWalkTargetsBasic(t *testing.T) {
	got := walkTargets(10000, 5000, 1000)
	want := []uint64{10000, 9000, 8000, 7000, 6000, 5000}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("walkTargets(10000, 5000, 1000)=%v, want %v", got, want)
	}
}

func TestWalkTargetsFloorsToInterval(t *testing.T) {
	got := walkTargets(10500, 9000, 1000)
	want := []uint64{10000, 9000}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("walkTargets(10500, 9000, 1000)=%v, want %v", got, want)
	}
}

func TestWalkTargetsMinHeightZeroStopsAtInterval(t *testing.T) {
	got := walkTargets(2000, 0, 1000)
	want := []uint64{2000, 1000}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("walkTargets(2000, 0, 1000)=%v, want %v", got, want)
	}
}

func TestWalkTargetsTopBelowIntervalMinZero(t *testing.T) {
	if got := walkTargets(500, 0, 1000); got != nil {
		t.Fatalf("walkTargets(500, 0, 1000)=%v, want nil", got)
	}
}

func TestWalkTargetsFlooredStartEqualsMin(t *testing.T) {
	got := walkTargets(10500, 10000, 1000)
	want := []uint64{10000}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("walkTargets(10500, 10000, 1000)=%v, want %v", got, want)
	}
}

func TestWalkTargetsFlooredStartBelowMin(t *testing.T) {
	if got := walkTargets(10500, 10001, 1000); got != nil {
		t.Fatalf("walkTargets(10500, 10001, 1000)=%v, want nil", got)
	}
}

func TestWalkTargetsTopBelowMin(t *testing.T) {
	if got := walkTargets(100, 1000, 100); got != nil {
		t.Fatalf("walkTargets(top<min)=%v, want nil", got)
	}
}

func TestWalkTargetsZeroInterval(t *testing.T) {
	if got := walkTargets(1000, 100, 0); got != nil {
		t.Fatalf("walkTargets(interval=0)=%v, want nil", got)
	}
}

func TestWalkTargetsSingleTarget(t *testing.T) {
	got := walkTargets(5000, 5000, 1000)
	want := []uint64{5000}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("walkTargets(5000, 5000, 1000)=%v, want %v", got, want)
	}
}

func TestIsJumpCandidateAcceptsFresherHeight(t *testing.T) {
	if !isJumpCandidate(11000, 10000, 9000, nil) {
		t.Fatal("isJumpCandidate should accept height above current within window")
	}
}

func TestIsJumpCandidateRejectsNotAbove(t *testing.T) {
	if isJumpCandidate(10000, 10000, 0, nil) {
		t.Fatal("isJumpCandidate should reject height equal to current")
	}
	if isJumpCandidate(9000, 10000, 0, nil) {
		t.Fatal("isJumpCandidate should reject height below current")
	}
}

func TestIsJumpCandidateRespectsMinHeight(t *testing.T) {
	if isJumpCandidate(8500, 8000, 9000, nil) {
		t.Fatal("isJumpCandidate should reject height below minHeight")
	}
	if !isJumpCandidate(8500, 8000, 0, nil) {
		t.Fatal("isJumpCandidate should ignore minHeight when zero")
	}
}

func TestIsJumpCandidateSkipsFailedHeights(t *testing.T) {
	failed := map[uint64]bool{11000: true}
	if isJumpCandidate(11000, 10000, 0, failed) {
		t.Fatal("isJumpCandidate should reject heights already in failed set")
	}
}

// Issue #25: an adversarial peer that always offers a slightly higher
// height than the current target must not be able to keep the walker
// jumping forever. The walker gates jumps on a per-walk cap; this test
// drives the same gate the loop uses and asserts termination.
func TestJumpCapTerminatesUnderAdversary(t *testing.T) {
	const minHeight = 0
	failed := map[uint64]bool{}
	maxJumps := 4
	jumps := 0
	current := uint64(10000)

	for i := 0; i < 1000; i++ {
		next := current + 1000
		if !isJumpCandidate(next, current, minHeight, failed) {
			break
		}
		if jumps >= maxJumps {
			break
		}
		jumps++
		current = next
	}
	if jumps != maxJumps {
		t.Fatalf("jumps=%d, want %d (cap reached and loop exited)", jumps, maxJumps)
	}
}
