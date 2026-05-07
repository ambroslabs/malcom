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
