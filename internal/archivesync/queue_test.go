package archivesync

import "testing"

func TestQueueRoundTrip(t *testing.T) {
	q := NewQueue()
	if n := q.AddRange(100, 109); n != 10 {
		t.Fatalf("AddRange got %d, want 10", n)
	}
	if q.Size() != 10 {
		t.Fatalf("Size=%d", q.Size())
	}
	if !q.Has(105) {
		t.Fatalf("Has(105)=false")
	}
	q.Remove(105)
	if q.Has(105) {
		t.Fatalf("Has(105)=true after Remove")
	}
	// Iterate everything once via Next.
	got := make(map[int64]bool)
	for i := 0; i < q.Size(); i++ {
		h, ok := q.Next()
		if !ok {
			t.Fatalf("Next returned !ok early")
		}
		got[h] = true
	}
	if len(got) != 9 {
		t.Fatalf("collected %d unique, want 9", len(got))
	}
	if got[105] {
		t.Fatalf("Next returned a removed height")
	}
}

func TestQueueEmpty(t *testing.T) {
	q := NewQueue()
	if _, ok := q.Next(); ok {
		t.Fatalf("Next on empty returned ok")
	}
	q.Add(7)
	q.Remove(7)
	if _, ok := q.Next(); ok {
		t.Fatalf("Next after add+remove returned ok")
	}
}

func TestQueueDedup(t *testing.T) {
	q := NewQueue()
	q.Add(42)
	if q.Add(42) {
		t.Fatalf("Add returned new=true on duplicate")
	}
	if q.Size() != 1 {
		t.Fatalf("Size=%d", q.Size())
	}
}
