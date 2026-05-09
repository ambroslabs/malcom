package bootstrap

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPatchTOMLKeyTopLevel(t *testing.T) {
	in := `# header comment
app-db-backend = "goleveldb"
pruning = "default"

[grpc]
enable = true
`
	got, err := runPatch(t, in, func(path string) error {
		return setTOMLString(path, "", "app-db-backend", "pebbledb")
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, `app-db-backend = "pebbledb"`) {
		t.Fatalf("missing patched value:\n%s", got)
	}
	if !strings.Contains(got, `# header comment`) {
		t.Fatal("comment not preserved")
	}
	if !strings.Contains(got, `pruning = "default"`) {
		t.Fatal("sibling key clobbered")
	}
}

func TestPatchTOMLKeyInSection(t *testing.T) {
	in := `[statesync]
enable = false
rpc_servers = ""
trust_height = 0
trust_hash = ""
`
	got, err := runPatch(t, in, func(path string) error {
		if err := setTOMLString(path, "statesync", "rpc_servers", "https://a.example,https://b.example"); err != nil {
			return err
		}
		if err := setTOMLInt64(path, "statesync", "trust_height", 31026000); err != nil {
			return err
		}
		return setTOMLString(path, "statesync", "trust_hash", "ABCD1234")
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`rpc_servers = "https://a.example,https://b.example"`,
		`trust_height = 31026000`,
		`trust_hash = "ABCD1234"`,
		`enable = false`, // untouched
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
}

func TestPatchTOMLKeyMissingKey(t *testing.T) {
	in := `pruning = "default"
`
	_, err := runPatch(t, in, func(path string) error {
		return setTOMLString(path, "", "no-such-key", "x")
	})
	if err == nil {
		t.Fatal("expected error for missing key")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("error message: %v", err)
	}
}

func TestPatchTOMLKeyDoesNotMatchInWrongSection(t *testing.T) {
	in := `[a]
foo = "old"

[b]
foo = "old"
`
	got, err := runPatch(t, in, func(path string) error {
		return setTOMLString(path, "b", "foo", "new")
	})
	if err != nil {
		t.Fatal(err)
	}
	// First match in section [a] must survive untouched; section [b]
	// must be the one updated.
	idxA := strings.Index(got, "[a]")
	idxB := strings.Index(got, "[b]")
	if idxA == -1 || idxB == -1 {
		t.Fatalf("section markers missing:\n%s", got)
	}
	aBlock := got[idxA:idxB]
	bBlock := got[idxB:]
	if !strings.Contains(aBlock, `foo = "old"`) {
		t.Errorf("section [a] foo was modified:\n%s", aBlock)
	}
	if !strings.Contains(bBlock, `foo = "new"`) {
		t.Errorf("section [b] foo not patched:\n%s", bBlock)
	}
}

// runPatch writes `in` to a temp file, runs `apply`, and returns the
// post-edit file contents.
func runPatch(t *testing.T, in string, apply func(path string) error) (string, error) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.toml")
	if err := os.WriteFile(path, []byte(in), 0o644); err != nil {
		return "", err
	}
	if err := apply(path); err != nil {
		return "", err
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(body), nil
}
