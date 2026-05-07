package snapfetch

import "testing"

func TestPeerStatRecordFailureBansAtThreshold(t *testing.T) {
	st := &peerStat{}
	if banned := st.recordFailure(3); banned {
		t.Fatalf("recordFailure(3) at fail=1 returned true, want false")
	}
	if banned := st.recordFailure(3); banned {
		t.Fatalf("recordFailure(3) at fail=2 returned true, want false")
	}
	if banned := st.recordFailure(3); !banned {
		t.Fatalf("recordFailure(3) at fail=3 returned false, want true")
	}
	if !st.banned {
		t.Fatalf("banned not set after threshold")
	}
}

func TestPeerStatRecordFailureStaysBanned(t *testing.T) {
	st := &peerStat{}
	for i := 0; i < 5; i++ {
		st.recordFailure(2)
	}
	if !st.banned {
		t.Fatalf("not banned after 5 calls with limit 2")
	}
	// Calling again past threshold should still report true (still banned).
	if banned := st.recordFailure(2); !banned {
		t.Fatalf("recordFailure past threshold returned false")
	}
}

func TestPeerStatFailureLimitProvenVsProvisional(t *testing.T) {
	proven := &peerStat{}
	if got := proven.failureLimit(3, 1); got != 3 {
		t.Fatalf("proven.failureLimit=%d, want 3", got)
	}
	provisional := &peerStat{provisional: true}
	if got := provisional.failureLimit(3, 1); got != 1 {
		t.Fatalf("provisional.failureLimit=%d, want 1", got)
	}
}

func TestPeerStatPromoteIdempotent(t *testing.T) {
	st := &peerStat{provisional: true}
	if !st.promote() {
		t.Fatalf("first promote() returned false")
	}
	if st.provisional {
		t.Fatalf("provisional still true after promote")
	}
	if st.promote() {
		t.Fatalf("second promote() returned true (should be no-op)")
	}
}

func TestPeerStatBenchIfProvisional(t *testing.T) {
	prov := &peerStat{provisional: true}
	if !prov.benchIfProvisional() {
		t.Fatalf("benchIfProvisional on provisional returned false")
	}
	if !prov.banned {
		t.Fatalf("provisional not banned after benchIfProvisional")
	}

	proven := &peerStat{}
	if proven.benchIfProvisional() {
		t.Fatalf("benchIfProvisional on proven returned true")
	}
	if proven.banned {
		t.Fatalf("proven peer was banned by benchIfProvisional")
	}
}

func TestPeerStatRemoveInflightFloor(t *testing.T) {
	st := &peerStat{}
	st.removeInflight() // already 0
	if st.inflight != 0 {
		t.Fatalf("inflight=%d, want 0 (floor)", st.inflight)
	}
	st.addInflight()
	st.addInflight()
	st.removeInflight()
	if st.inflight != 1 {
		t.Fatalf("inflight=%d, want 1", st.inflight)
	}
	st.removeInflight()
	st.removeInflight() // would go to -1 without floor
	if st.inflight != 0 {
		t.Fatalf("inflight=%d, want 0 (floor)", st.inflight)
	}
}

func TestPeerStatClearInflight(t *testing.T) {
	st := &peerStat{}
	st.addInflight()
	st.addInflight()
	st.addInflight()
	st.clearInflight()
	if st.inflight != 0 {
		t.Fatalf("inflight=%d after clear, want 0", st.inflight)
	}
}
