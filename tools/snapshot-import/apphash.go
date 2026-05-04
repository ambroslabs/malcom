// Computes the global cosmos-sdk MultiStore AppHash from the per-store
// root hashes we collected during import, and (optionally) compares it
// to the consensus AppHash fetched from a cometbft RPC.
//
// Match semantics: gaiad's CommitInfo.Hash() must equal the AppHash field
// in the cometbft block header at height H+1 (the header at H+1 carries
// the AppHash of state after committing H). If they match, our import
// reproduces consensus state byte-for-byte.

package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// hashFromByteSlices computes the RFC6962 simple-merkle root over the
// given items, matching cometbft/crypto/merkle.HashFromByteSlices.
//
//	leaf:  sha256(0x00 || leaf_bytes)
//	inner: sha256(0x01 || left_hash || right_hash)
//	empty: sha256("")
//
// Tree split: at largest k = power-of-2 with k <= n/2 (or k = n/2 if n
// is itself a power of 2), so a 5-leaf tree splits into 4|1 rather than
// 2|3 — left-leaning balanced.
func hashFromByteSlices(items [][]byte) [32]byte {
	switch len(items) {
	case 0:
		return sha256.Sum256(nil)
	case 1:
		h := sha256.New()
		h.Write([]byte{0x00})
		h.Write(items[0])
		var out [32]byte
		copy(out[:], h.Sum(nil))
		return out
	default:
		k := splitPoint(len(items))
		left := hashFromByteSlices(items[:k])
		right := hashFromByteSlices(items[k:])
		h := sha256.New()
		h.Write([]byte{0x01})
		h.Write(left[:])
		h.Write(right[:])
		var out [32]byte
		copy(out[:], h.Sum(nil))
		return out
	}
}

func splitPoint(n int) int {
	if n <= 1 {
		return 1
	}
	k := 1
	for k < n {
		k <<= 1
	}
	if k == n {
		return k / 2
	}
	return k / 2
}

// computeAppHash reproduces cosmos-sdk's CommitInfo.Hash() — the
// MultiStore AppHash gaiad commits at the end of each block.
//
// Algorithm (verified against cosmossdk.io/store v1.1.x and parent
// repo's cmd/cosmos-apphash-verify):
//
//  1. For each (store_name, store_root_hash):
//     leaf = uvarint(len(name)) || name || uvarint(32) || sha256(store_root_hash)
//     The per-store root is **pre-hashed** before going into the leaf —
//     a cosmos-sdk legacy from cometbft's old simpleMap. Load-bearing
//     for byte-equality with consensus.
//
//  2. Sort leaves lexicographically by store name.
//
//  3. RFC6962 simple-merkle root over the sorted leaves.
func computeAppHash(stores []storeInfo) [32]byte {
	sorted := make([]storeInfo, len(stores))
	copy(sorted, stores)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	items := make([][]byte, len(sorted))
	for i, si := range sorted {
		vh := sha256.Sum256(si.Hash[:])
		var buf bytes.Buffer
		var lb [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(lb[:], uint64(len(si.Name)))
		buf.Write(lb[:n])
		buf.WriteString(si.Name)
		n = binary.PutUvarint(lb[:], uint64(len(vh)))
		buf.Write(lb[:n])
		buf.Write(vh[:])
		items[i] = buf.Bytes()
	}
	return hashFromByteSlices(items)
}

// fetchAppHash retrieves the consensus app_hash hex string from a
// cometbft RPC's /commit endpoint at the given height. The header at H+1
// carries the AppHash that committing H produced — so to verify state at
// H, fetch from H+1.
func fetchAppHash(rpcBase string, height int64) (string, error) {
	url := fmt.Sprintf("%s/commit?height=%d", strings.TrimRight(rpcBase, "/"), height)
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("rpc %s HTTP %d: %s", url, resp.StatusCode, body)
	}
	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return "", err
	}
	result, _ := raw["result"].(map[string]any)
	signed, _ := result["signed_header"].(map[string]any)
	header, _ := signed["header"].(map[string]any)
	apphash, _ := header["app_hash"].(string)
	if apphash == "" {
		return "", fmt.Errorf("no app_hash in response from %s", url)
	}
	return apphash, nil
}

// verifyAppHash computes the local app-hash from the per-store roots
// we collected during import and (optionally) compares it to either an
// expected hex string or one fetched from a cometbft RPC.
//
// Returns the local hash as hex always; returns an error only if a
// comparison was requested and failed.
func verifyAppHash(stores []storeInfo, height int64,
	expectedHex string, rpcURL string, log io.Writer) (string, error) {

	local := computeAppHash(stores)
	localHex := hex.EncodeToString(local[:])
	fmt.Fprintf(log, "[verify] local AppHash:                 %s\n", strings.ToUpper(localHex))

	if expectedHex == "" && rpcURL == "" {
		fmt.Fprintf(log, "[verify] (no -expected-apphash or -rpc; not comparing)\n")
		return localHex, nil
	}

	var consensusHex string
	switch {
	case expectedHex != "":
		consensusHex = strings.TrimSpace(expectedHex)
		fmt.Fprintf(log, "[verify] expected AppHash (-expected): %s\n", strings.ToUpper(consensusHex))
	case rpcURL != "":
		fmt.Fprintf(log, "[verify] fetching consensus AppHash from %s @ height %d (= H+1 of import height)\n",
			rpcURL, height+1)
		got, err := fetchAppHash(rpcURL, height+1)
		if err != nil {
			return localHex, fmt.Errorf("fetch consensus app hash: %w", err)
		}
		consensusHex = strings.TrimSpace(got)
		fmt.Fprintf(log, "[verify] consensus AppHash @ block %d:  %s\n", height+1, strings.ToUpper(consensusHex))
	}

	consBytes, err := hex.DecodeString(consensusHex)
	if err != nil {
		return localHex, fmt.Errorf("decode consensus app hash hex: %w", err)
	}
	if !bytes.Equal(local[:], consBytes) {
		return localHex, &mismatchError{
			height:    height,
			localHex:  localHex,
			refHex:    consensusHex,
		}
	}
	fmt.Fprintf(log, "\n  ✓ MATCH — application.db is consensus-correct at height %d\n", height)
	return localHex, nil
}

// mismatchError signals that the local hash and the reference hash
// genuinely differ — distinguishable from network / parse errors so
// the caller can choose to retry the verification (transient errors)
// or fail the run (genuine mismatch).
type mismatchError struct {
	height             int64
	localHex, refHex   string
}

func (e *mismatchError) Error() string {
	return fmt.Sprintf("AppHash MISMATCH at height %d: local=%s reference=%s",
		e.height, strings.ToUpper(e.localHex), strings.ToUpper(e.refHex))
}

// isMismatch reports whether err is a genuine hash mismatch (not a
// network / parse / RPC error).
func isMismatch(err error) bool {
	_, ok := err.(*mismatchError)
	return ok
}
