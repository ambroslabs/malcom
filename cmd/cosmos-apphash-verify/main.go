// cosmos-apphash-verify reads a CommitInfo from an application.db we
// produced (via cosmos-snapshot-to-appdb), computes the cosmos-sdk
// MultiStore AppHash, fetches the consensus AppHash from a cometbft RPC
// (e.g., Polkachu), and compares.
//
// Usage:
//
//	cosmos-apphash-verify -appdb <dir> -height <H>
//	    [-rpc https://cosmos-rpc.polkachu.com]
//	    [-backend goleveldb|pebbledb]
//
// Reports the local hash, the consensus hash at H+1 (which is the
// AppHash AFTER applying block H — i.e. state at height H), and whether
// they match.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/cometbft/cometbft/crypto/merkle"
	idb "github.com/cosmos/iavl/db"
)

func main() {
	appdb := flag.String("appdb", "", "path to the application.db parent dir (the dir containing application.db/)")
	height := flag.Int64("height", 0, "snapshot height committed to application.db (required)")
	rpcURL := flag.String("rpc", "https://cosmos-rpc.polkachu.com", "cometbft RPC endpoint")
	backendStr := flag.String("backend", "", "goleveldb or pebbledb (auto-detected if empty)")
	flag.Parse()

	if *appdb == "" || *height == 0 {
		flag.Usage()
		os.Exit(2)
	}

	backend := *backendStr
	if backend == "" {
		// auto-detect: pebble dirs have CURRENT, OPTIONS-* etc.; goleveldb has LOCK + CURRENT
		dbPath := filepath.Join(*appdb, "application.db")
		if hasPebbleMarkers(dbPath) {
			backend = "pebbledb"
		} else {
			backend = "goleveldb"
		}
	}
	fmt.Printf("[verify] appdb   %s\n", *appdb)
	fmt.Printf("[verify] backend %s\n", backend)
	fmt.Printf("[verify] height  %d\n", *height)

	// Read CommitInfo at s/<height>.
	infos, err := readCommitInfo(*appdb, backend, *height)
	if err != nil {
		log.Fatalf("read commit info: %v", err)
	}
	fmt.Printf("[verify] stores: %d\n", len(infos))
	for _, si := range infos {
		fmt.Printf("            %-22s %x\n", si.Name, si.Hash)
	}

	localHash := computeAppHash(infos)
	fmt.Printf("[verify] local AppHash:     %X\n", localHash)

	// Fetch consensus AppHash from /commit?height=H+1.
	consHeight := *height + 1
	consHash, err := fetchAppHash(*rpcURL, consHeight)
	if err != nil {
		log.Fatalf("fetch consensus app hash: %v", err)
	}
	fmt.Printf("[verify] consensus AppHash @ block %d: %s\n", consHeight, consHash)

	consBytes, _ := hex.DecodeString(consHash)
	if bytes.Equal(localHash, consBytes) {
		fmt.Println("\n  ✓ MATCH — application.db is consensus-correct at height", *height)
	} else {
		fmt.Println("\n  ✗ MISMATCH")
		os.Exit(1)
	}
}

type storeInfo struct {
	Name string
	Hash []byte
}

func readCommitInfo(appdbParent, backend string, height int64) ([]storeInfo, error) {
	dbPath := filepath.Join(appdbParent, "application.db")
	switch backend {
	case "goleveldb":
		db, err := idb.NewGoLevelDB("application", appdbParent)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		return readCommitInfoFromDB(db, height)
	case "pebbledb":
		_ = dbPath
		// Open via the same adapter used for writing.
		db, err := openPebbleDB(dbPath)
		if err != nil {
			return nil, err
		}
		defer db.Close()
		return readCommitInfoFromDB(db, height)
	default:
		return nil, fmt.Errorf("unsupported backend %q", backend)
	}
}

type kvReader interface {
	Get(key []byte) ([]byte, error)
}

func readCommitInfoFromDB(db kvReader, height int64) ([]storeInfo, error) {
	key := []byte(fmt.Sprintf("s/%d", height))
	val, err := db.Get(key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, fmt.Errorf("no CommitInfo at key %q", key)
	}
	return parseCommitInfo(val)
}

// parseCommitInfo decodes the CommitInfo proto we wrote in
// internal/snapshotappdb/snapshotappdb.go: encodeCommitInfo.
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
		case field == 1 && wire == 0: // version
			_, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return nil, fmt.Errorf("bad version")
			}
			i += m
		case field == 2 && wire == 2: // store_info
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
		case field == 1 && wire == 2: // name
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return si, fmt.Errorf("bad name length")
			}
			i += m
			si.Name = string(buf[i : i+int(ln)])
			i += int(ln)
		case field == 2 && wire == 2: // commit_id
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
		case field == 1 && wire == 0: // version
			_, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return nil, fmt.Errorf("bad commitID version")
			}
			i += m
		case field == 2 && wire == 2: // hash
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

func hasPebbleMarkers(dir string) bool {
	if _, err := os.Stat(filepath.Join(dir, "OPTIONS-000003")); err == nil {
		return true
	}
	if entries, err := os.ReadDir(dir); err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "OPTIONS-") {
				return true
			}
		}
	}
	return false
}

// computeAppHash reproduces cosmos-sdk's CommitInfo.Hash() — the
// MultiStore AppHash that gaiad would compute and ICS-23-prove against.
//
// Algorithm (verified against cosmossdk.io/store v1.1.2):
//
//  1. For each (store_name, store_root_hash):
//     leaf_input = uvarint(len(name)) || name
//                  || uvarint(32)     || sha256(store_root_hash)
//
//     Note the **sha256 of the value**: cosmos-sdk's simpleMap.Set
//     pre-hashes the value before adding it to the map (legacy from
//     cometbft's old simpleMap, comment in source: "The value is
//     hashed, so you can check for equality with a cached value").
//     There's no functional reason for it on the consensus side, but
//     it's load-bearing for the AppHash output.
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
		// Pre-hash the per-store hash. This is the cosmos-sdk quirk.
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
	w.Write(buf[:n])
}

// ─── pebble adapter (just Get + Close, read-only) ────────────────────

type pebbleReader struct{ db *pebble.DB }

func openPebbleDB(dir string) (*pebbleReader, error) {
	db, err := pebble.Open(dir, &pebble.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	return &pebbleReader{db: db}, nil
}

func (p *pebbleReader) Get(key []byte) ([]byte, error) {
	v, closer, err := p.db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(v))
	copy(out, v)
	closer.Close()
	return out, nil
}

func (p *pebbleReader) Close() error { return p.db.Close() }
