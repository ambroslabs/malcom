package verify

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/cometbft/cometbft/crypto/merkle"

	malcomlog "github.com/ambroslabs/malcom/internal/log"
)

// ErrMismatch is returned by CheckAppHash when the computed local
// AppHash doesn't match the consensus AppHash from the chosen RPC.
// Distinct from generic errors (network, decode, missing CommitInfo)
// so callers can map it to a specific exit code.
var ErrMismatch = errors.New("apphash mismatch")

// ErrNoUsableRPC means we tried every RPC in the list and none
// returned an AppHash at height+1. The wrapped error is from the last
// attempt, so a debug log captures the chain of failures.
var ErrNoUsableRPC = errors.New("no usable rpc")

// Result is what CheckAppHash returns on success. On ErrMismatch it
// is also populated so the caller can log the mismatch context
// (which RPC, what local vs. consensus said).
type Result struct {
	// Height is the snapshot height the CommitInfo was read from.
	// AppHash was fetched from RPC at Height+1, matching cosmos-sdk's
	// "AppHash committed in block N is what BeginBlocker N+1 verifies
	// against" convention.
	Height int64

	// UsedRPC is the first RPC URL from the input list that returned
	// a usable AppHash. Errors against earlier RPCs are surfaced via
	// the logger; only the winner is reported here.
	UsedRPC string

	// LocalHash is the AppHash computed from the imported
	// application.db's CommitInfo at Height.
	LocalHash []byte

	// ConsensusHash is the AppHash the RPC reported at Height+1.
	// Hex-decoded from the /commit endpoint's signed_header.
	ConsensusHash []byte
}

// Match reports whether the local and consensus AppHashes agree.
// Convenience wrapper so callers don't have to bytes.Equal directly.
func (r Result) Match() bool { return bytes.Equal(r.LocalHash, r.ConsensusHash) }

// CheckAppHash opens the imported application.db at appdbParent, reads
// the CommitInfo at height, computes the cosmos-sdk MultiStore
// AppHash, and compares it against the consensus AppHash reported by
// each rpc in order until one responds. On mismatch returns the
// populated Result and ErrMismatch; on agreement returns the Result
// and nil. Other errors (db open, CommitInfo decode, every-rpc-fails)
// surface unwrapped.
//
// Producer-agnostic: the appdb can come from `malcom snapshot import`
// or from a pipelined `malcom snapshot fetch --import` — both produce
// the same on-disk shape.
func CheckAppHash(appdbParent string, height int64, rpcs []string, log *slog.Logger) (Result, error) {
	var zero Result
	if log == nil {
		log = slog.Default()
	}
	if len(rpcs) == 0 {
		return zero, errors.New("no rpcs provided")
	}

	infos, err := readCommitInfo(appdbParent, height, log)
	if err != nil {
		return zero, fmt.Errorf("read commit info: %w", err)
	}
	log.Debug("commit info read", "stores", len(infos))
	for _, si := range infos {
		log.Debug("store", "name", si.Name, "hash", fmt.Sprintf("%x", si.Hash))
	}
	localHash := computeAppHash(infos)
	log.Debug("local apphash", "apphash", fmt.Sprintf("%X", localHash))

	consHeight := height + 1
	var (
		usedRPC string
		consHex string
		lastErr error
	)
	for _, rpc := range rpcs {
		hash, err := fetchAppHash(rpc, consHeight)
		if err != nil {
			log.Warn("rpc unreachable; trying next", "rpc", rpc, "err", err)
			lastErr = err
			continue
		}
		usedRPC = rpc
		consHex = hash
		break
	}
	if usedRPC == "" {
		return zero, fmt.Errorf("%w (tried %d): %v", ErrNoUsableRPC, len(rpcs), lastErr)
	}
	consBytes, err := hex.DecodeString(consHex)
	if err != nil {
		return zero, fmt.Errorf("decode rpc apphash %q: %w", consHex, err)
	}

	res := Result{
		Height:        height,
		UsedRPC:       usedRPC,
		LocalHash:     localHash,
		ConsensusHash: consBytes,
	}
	if !res.Match() {
		return res, ErrMismatch
	}
	return res, nil
}

type storeInfo struct {
	Name string
	Hash []byte
}

func readCommitInfo(appdbParent string, height int64, log *slog.Logger) ([]storeInfo, error) {
	dbPath := filepath.Join(appdbParent, "application.db")
	db, err := pebble.Open(dbPath, &pebble.Options{
		ReadOnly: true,
		Logger:   malcomlog.PebbleShim(log.With("module", "pebble")),
	})
	if err != nil {
		return nil, err
	}
	defer db.Close()

	key := []byte(fmt.Sprintf("s/%d", height))
	val, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, fmt.Errorf("no CommitInfo at key %q", key)
	}
	if err != nil {
		return nil, err
	}
	buf := make([]byte, len(val))
	copy(buf, val)
	closer.Close()
	return parseCommitInfo(buf)
}

// parseCommitInfo decodes the CommitInfo proto we wrote in
// internal/snapshotimport/commitinfo.go: commitInfoBytes.
//
//	message CommitID    { int64 version = 1; bytes hash = 2; }
//	message StoreInfo   { string name = 1; CommitID commit_id = 2; }
//	message CommitInfo  { int64 version = 1; repeated StoreInfo store_infos = 2; }
func parseCommitInfo(buf []byte) ([]storeInfo, error) {
	var infos []storeInfo
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		field := int(tag >> 3)
		wire := tag & 0x07
		switch {
		case field == 1 && wire == 0:
			_, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return nil, fmt.Errorf("bad version")
			}
			i += m
		case field == 2 && wire == 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return nil, fmt.Errorf("bad store_info length")
			}
			i += m
			si, err := parseStoreInfo(buf[i : i+int(ln)])
			if err != nil {
				return nil, err
			}
			infos = append(infos, si)
			i += int(ln)
		default:
			return nil, fmt.Errorf("unexpected field %d wire %d in CommitInfo", field, wire)
		}
	}
	return infos, nil
}

func parseStoreInfo(buf []byte) (storeInfo, error) {
	var si storeInfo
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		field := int(tag >> 3)
		wire := tag & 0x07
		switch {
		case field == 1 && wire == 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return si, fmt.Errorf("bad name length")
			}
			i += m
			si.Name = string(buf[i : i+int(ln)])
			i += int(ln)
		case field == 2 && wire == 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return si, fmt.Errorf("bad commit_id length")
			}
			i += m
			h, err := parseCommitID(buf[i : i+int(ln)])
			if err != nil {
				return si, err
			}
			si.Hash = h
			i += int(ln)
		default:
			return si, fmt.Errorf("unexpected field %d wire %d in StoreInfo", field, wire)
		}
	}
	return si, nil
}

func parseCommitID(buf []byte) ([]byte, error) {
	var hash []byte
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		field := int(tag >> 3)
		wire := tag & 0x07
		switch {
		case field == 1 && wire == 0:
			_, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return nil, fmt.Errorf("bad commitID version")
			}
			i += m
		case field == 2 && wire == 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return nil, fmt.Errorf("bad commitID hash length")
			}
			i += m
			hash = append([]byte(nil), buf[i:i+int(ln)]...)
			i += int(ln)
		default:
			return nil, fmt.Errorf("unexpected field %d wire %d in CommitID", field, wire)
		}
	}
	return hash, nil
}

func fetchAppHash(rpcBase string, height int64) (string, error) {
	url := fmt.Sprintf("%s/commit?height=%d", strings.TrimRight(rpcBase, "/"), height)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("rpc HTTP %d: %s", resp.StatusCode, body)
	}
	var raw map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return "", err
	}
	result, _ := raw["result"].(map[string]interface{})
	signed, _ := result["signed_header"].(map[string]interface{})
	header, _ := signed["header"].(map[string]interface{})
	apphash, _ := header["app_hash"].(string)
	if apphash == "" {
		return "", fmt.Errorf("no app_hash in response")
	}
	return apphash, nil
}

// computeAppHash reproduces cosmos-sdk's CommitInfo.Hash() — the
// MultiStore AppHash that a cosmos-sdk daemon would compute and
// ICS-23-prove against.
//
//  1. For each (store_name, store_root_hash):
//     leaf_input = uvarint(len(name)) || name
//     || uvarint(32)     || sha256(store_root_hash)
//
//     The sha256 of the value is the cosmos-sdk quirk: simpleMap.Set
//     pre-hashes values before adding to the map. Load-bearing for the
//     AppHash output even though it's not load-bearing for consensus.
//
//  2. Sort leaves lexicographically by store name.
//
//  3. Run cometbft's RFC6962 simple-merkle (HashFromByteSlices) over
//     the sorted leaf inputs. cometbft applies the leaf-prefix (0x00)
//     and inner-prefix (0x01) internally.
func computeAppHash(infos []storeInfo) []byte {
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	items := make([][]byte, len(infos))
	for i, si := range infos {
		vhash := sha256.Sum256(si.Hash)
		var buf bytes.Buffer
		writeUvarint(&buf, uint64(len(si.Name)))
		buf.WriteString(si.Name)
		writeUvarint(&buf, uint64(len(vhash)))
		buf.Write(vhash[:])
		items[i] = buf.Bytes()
	}
	return merkle.HashFromByteSlices(items)
}

func writeUvarint(w io.Writer, v uint64) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	// Both call sites pass a *bytes.Buffer, whose Write never errors;
	// the error return only matters for io.Writer impls that can fail.
	_, _ = w.Write(buf[:n])
}
