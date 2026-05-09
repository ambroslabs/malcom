// Filesystem + extension-placement helpers used by the bootstrap
// orchestrator. None of these are chain-aware; they're the pure-data
// plumbing that sits beneath the per-chain logic in run.go.

package bootstrap

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// copyFile mirrors a single file src → dst, byte-for-byte. The
// destination's parent directory must already exist.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, in)
	return err
}

// cloneTree mirrors srcDir to dstDir as an independent on-disk copy.
// Hardlinks are intentionally avoided — pebble's LOCK file aliases
// across hardlinks, which entangles the source dir's lifetime with
// gaiad's runtime, and a daemon at runtime would obsolete files via
// unlink (only decrementing link count) — leaving the source alive in
// practice but with files owned by gaiad's runtime. cloneTree gives
// the operator an independent dst they can move/delete without
// disturbing src.
func cloneTree(srcDir, dstDir string) error {
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		dst := filepath.Join(dstDir, rel)
		if info.IsDir() {
			return os.MkdirAll(dst, info.Mode())
		}
		return copyFile(path, dst)
	})
}

// placeWasmPayloads ungzips each snapshot extension payload, sha256s
// the raw wasm bytes, and writes the bytecode at the path the chain
// runtime expects:
//
//	<root>/wasm/state/wasm/<hex_checksum>                   (cosmwasm — wasmd ≥ v0.46 BaseDir convention)
//	<root>/data/08-light-client/state/wasm/<hex_checksum>   (IBC 08-wasm)
//
// wasmvm compiles on first invocation, so we only place raw bytecode.
//
// Older wasmd (≤ v0.45) used <root>/wasm/wasm/<hex_checksum>; if that
// becomes a real chain target, add a strategy switch keyed off
// chain-registry's codebase.cosmwasm.version. Today every supported
// chain uses the modern layout, so we keep it simple.
//
// Per-chain wasm dir overrides live in wasmDirByChainID — chains whose
// daemon configures wasmd's BaseDir off the default (osmosis-1's
// `<home>/wasm/wasm`) need their bytecode placed accordingly, otherwise
// startup panics with "pinning contract failed". Default of empty
// string falls through to the modern wasmd layout.
//
// Unrecognized extension dirs (anything other than `wasm` /
// `08-wasm`) cause a hard error rather than a silent skip — silently
// skipping produces a daemon that boots with missing state and
// panics later.
func placeWasmPayloads(extDir, gaiaRoot, chainID string, log *slog.Logger) error {
	entries, err := os.ReadDir(extDir)
	if err != nil {
		return fmt.Errorf("read extensions dir: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		switch e.Name() {
		case "wasm":
			sub := wasmDirByChainID[chainID]
			if sub == "" {
				sub = "wasm/state/wasm"
			}
			dst := filepath.Join(gaiaRoot, sub)
			if err := placeOneExt(filepath.Join(extDir, e.Name()), dst, log); err != nil {
				return fmt.Errorf("wasm extension: %w", err)
			}
		case "08-wasm":
			dst := filepath.Join(gaiaRoot, "data", "08-light-client", "state", "wasm")
			if err := placeOneExt(filepath.Join(extDir, e.Name()), dst, log); err != nil {
				return fmt.Errorf("08-wasm extension: %w", err)
			}
		default:
			return fmt.Errorf("unknown snapshot extension %q in %s — refusing to silently skip; file an issue with the chain id and extension name", e.Name(), extDir)
		}
	}
	return nil
}

// wasmDirByChainID overrides the default `wasm/state/wasm` BaseDir for
// chains that point wasmd elsewhere. The values are paths relative to
// the chain home; the bytecode goes at `<gaiaRoot>/<value>/<sha256>`.
//
// osmosis-1 sets wasmd's BaseDir to `<home>/wasm/wasm` (so the actual
// bytecode subdir is `<home>/wasm/wasm/state/wasm`), confirmed
// empirically by watching osmosisd auto-create `<home>/wasm/wasm/cache/`
// at first start. Other chains' overrides land here as we hit them.
var wasmDirByChainID = map[string]string{
	"osmosis-1": "wasm/wasm/state/wasm",
}

// placeOneExt iterates payload files in srcDir, gunzips each, hashes
// the raw bytes, and writes them to dstDir/<hex_sha256>. Returns nil
// for an empty source dir.
func placeOneExt(srcDir, dstDir string, log *slog.Logger) error {
	if _, err := os.Stat(srcDir); err != nil {
		return nil
	}
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return err
	}
	count := 0
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasPrefix(ent.Name(), "payload-") {
			continue
		}
		gz, err := os.ReadFile(filepath.Join(srcDir, ent.Name()))
		if err != nil {
			return err
		}
		raw, err := gunzip(gz)
		if err != nil {
			return fmt.Errorf("gunzip %s: %w", ent.Name(), err)
		}
		sum := sha256.Sum256(raw)
		dstPath := filepath.Join(dstDir, hex.EncodeToString(sum[:]))
		if err := os.WriteFile(dstPath, raw, 0o644); err != nil {
			return err
		}
		count++
	}
	log.Info("placed wasm bytecode files", "count", count, "dir", dstDir)
	return nil
}

// gunzip ungzips a single in-memory gzip blob.
func gunzip(in []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(in))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
