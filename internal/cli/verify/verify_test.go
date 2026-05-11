package verify

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testLog returns a slog.Logger that drops everything — keeps test
// output clean and dodges readCommitInfo's nil-logger deref (it does
// log.With() without a nil guard since CheckAppHash is the documented
// entry point and that one defensively assigns slog.Default()).
func testLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// osmosisAppDB is the e2e fixture appdb already on disk; CheckAppHash
// happy path needs a real CommitInfo. The known-good consensus
// AppHash at height+1 was captured against a live RPC at the time the
// fixture was produced — we mint a fake RPC that returns that exact
// hex so the test doesn't need network.
const (
	osmosisFixtureAppDB   = "../../../e2e-test-osmosis/appdb_osmosis-1_61314000"
	osmosisFixtureHeight  = int64(61314000)
	osmosisFixtureChainID = "osmosis-1"
)

func skipIfNoFixture(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(osmosisFixtureAppDB, "application.db")); err != nil {
		t.Skipf("fixture %s not present: %v", osmosisFixtureAppDB, err)
	}
}

// fakeRPC returns a /commit handler that responds with the given
// app_hash hex. RPC test pattern: bind an httptest.Server, hand the
// URL to CheckAppHash via rpcs[0].
func fakeRPC(t *testing.T, appHashHex string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/commit", func(w http.ResponseWriter, r *http.Request) {
		resp := map[string]interface{}{
			"result": map[string]interface{}{
				"signed_header": map[string]interface{}{
					"header": map[string]interface{}{
						"app_hash": appHashHex,
					},
				},
			},
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	return httptest.NewServer(mux)
}

// localHashFromFixture computes the local AppHash from the fixture
// appdb without making any network calls. Useful for two things: (a)
// asserting the success path returns matching hashes when the RPC
// echoes that hex, (b) constructing a different-but-validly-formatted
// hash for the mismatch test by flipping one byte.
func localHashFromFixture(t *testing.T) []byte {
	t.Helper()
	infos, err := readCommitInfo(osmosisFixtureAppDB, osmosisFixtureHeight, testLog())
	if err != nil {
		t.Fatalf("readCommitInfo: %v", err)
	}
	return computeAppHash(infos)
}

func TestCheckAppHash_Match(t *testing.T) {
	skipIfNoFixture(t)
	local := localHashFromFixture(t)
	srv := fakeRPC(t, fmt.Sprintf("%X", local))
	defer srv.Close()

	res, err := CheckAppHash(osmosisFixtureAppDB, osmosisFixtureHeight, []string{srv.URL}, nil)
	if err != nil {
		t.Fatalf("CheckAppHash: %v", err)
	}
	if !res.Match() {
		t.Fatal("expected Match()=true when consensus hash equals local")
	}
	if res.Height != osmosisFixtureHeight {
		t.Fatalf("Height=%d, want %d", res.Height, osmosisFixtureHeight)
	}
	if res.UsedRPC != srv.URL {
		t.Fatalf("UsedRPC=%q, want %q", res.UsedRPC, srv.URL)
	}
}

func TestCheckAppHash_Mismatch(t *testing.T) {
	skipIfNoFixture(t)
	local := localHashFromFixture(t)
	// Flip the last byte so the RPC reports a different-but-valid hash.
	tampered := append([]byte(nil), local...)
	tampered[len(tampered)-1] ^= 0xFF
	srv := fakeRPC(t, fmt.Sprintf("%X", tampered))
	defer srv.Close()

	res, err := CheckAppHash(osmosisFixtureAppDB, osmosisFixtureHeight, []string{srv.URL}, nil)
	if !errors.Is(err, ErrMismatch) {
		t.Fatalf("err = %v, want ErrMismatch", err)
	}
	if res.Match() {
		t.Fatal("expected Match()=false on tampered RPC response")
	}
	// Result is still populated on mismatch so the CLI layer can log
	// the local-vs-consensus comparison.
	if len(res.LocalHash) == 0 || len(res.ConsensusHash) == 0 {
		t.Fatal("Result.LocalHash / ConsensusHash should be populated on mismatch")
	}
}

func TestCheckAppHash_FailsOverToNextRPC(t *testing.T) {
	skipIfNoFixture(t)
	local := localHashFromFixture(t)

	// First RPC: refuses every connection. The httptest.Server
	// produces a working URL but we'll point at a closed port instead.
	deadSrv := httptest.NewServer(http.NewServeMux())
	deadURL := deadSrv.URL
	deadSrv.Close() // socket now closed; CheckAppHash should fail-fast

	goodSrv := fakeRPC(t, fmt.Sprintf("%X", local))
	defer goodSrv.Close()

	rpcs := []string{deadURL, goodSrv.URL}
	res, err := CheckAppHash(osmosisFixtureAppDB, osmosisFixtureHeight, rpcs, nil)
	if err != nil {
		t.Fatalf("CheckAppHash should have failed over to the second RPC; err=%v", err)
	}
	if res.UsedRPC != goodSrv.URL {
		t.Fatalf("expected fail-over to second RPC %q, used %q", goodSrv.URL, res.UsedRPC)
	}
}

func TestCheckAppHash_AllRPCsUnreachable(t *testing.T) {
	skipIfNoFixture(t)

	dead1 := httptest.NewServer(http.NewServeMux())
	dead1URL := dead1.URL
	dead1.Close()
	dead2 := httptest.NewServer(http.NewServeMux())
	dead2URL := dead2.URL
	dead2.Close()

	_, err := CheckAppHash(osmosisFixtureAppDB, osmosisFixtureHeight, []string{dead1URL, dead2URL}, nil)
	if !errors.Is(err, ErrNoUsableRPC) {
		t.Fatalf("err = %v, want ErrNoUsableRPC", err)
	}
	if !strings.Contains(err.Error(), "tried 2") {
		t.Fatalf("err message should report the number of RPCs tried, got: %v", err)
	}
}

func TestCheckAppHash_EmptyRPCList(t *testing.T) {
	if _, err := CheckAppHash(osmosisFixtureAppDB, osmosisFixtureHeight, nil, nil); err == nil {
		t.Fatal("expected error for empty rpcs slice")
	}
}

func TestCheckAppHash_MissingAppDB(t *testing.T) {
	srv := fakeRPC(t, "DEADBEEF")
	defer srv.Close()
	_, err := CheckAppHash("/tmp/does-not-exist-malcom-verify-test", 1, []string{srv.URL}, nil)
	if err == nil {
		t.Fatal("expected error for missing appdb")
	}
}
