// Verify-only path: read the commit-info that a prior import wrote to
// the pebble DB at `s/<height>`, decode it back to []storeInfo, and
// hand off to verifyAppHash. Lets the caller retry RPC verification
// without re-running the 10-minute import.

package main

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
)

// readCommitInfo opens the pebble DB read-only, fetches the bytes at
// `s/<height>`, and decodes the cosmos-sdk CommitInfo proto we wrote
// in commitinfo.go.
//
// Decode is the inverse of commitInfoBytes / encodeStoreInfo /
// encodeCommitID — hand-rolled to keep this submodule cosmos-free.
func readCommitInfo(dbDir string, height int64) ([]storeInfo, error) {
	db, err := pebble.Open(dbDir, &pebble.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", dbDir, err)
	}
	defer db.Close()

	key := commitInfoKey(height)
	val, closer, err := db.Get(key)
	if err == pebble.ErrNotFound {
		return nil, fmt.Errorf("no CommitInfo at key %q (was the import for height %d completed?)",
			key, height)
	}
	if err != nil {
		return nil, fmt.Errorf("pebble Get %q: %w", key, err)
	}
	defer closer.Close()

	return parseCommitInfo(val)
}

// parseCommitInfo decodes the on-the-wire CommitInfo proto:
//
//	message CommitInfo  { int64 version = 1; repeated StoreInfo store_infos = 2; }
//	message StoreInfo   { string name = 1; CommitID commit_id = 2; }
//	message CommitID    { int64 version = 1; bytes hash = 2; }
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
				return nil, fmt.Errorf("bad version varint at offset %d", i)
			}
			i += m
		case field == 2 && wire == 2: // store_info
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return nil, fmt.Errorf("bad store_info length at offset %d", i)
			}
			i += m
			si, err := parseStoreInfo(buf[i : i+int(ln)])
			if err != nil {
				return nil, fmt.Errorf("store_info[%d]: %w", len(infos), err)
			}
			infos = append(infos, si)
			i += int(ln)
		default:
			return nil, fmt.Errorf("unexpected field %d wire %d in CommitInfo at offset %d",
				field, wire, i-1)
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
			if m <= 0 || i+m+int(ln) > len(buf) {
				return si, fmt.Errorf("bad name length at offset %d", i)
			}
			i += m
			si.Name = string(buf[i : i+int(ln)])
			i += int(ln)
		case field == 2 && wire == 2: // commit_id
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return si, fmt.Errorf("bad commit_id length at offset %d", i)
			}
			i += m
			h, err := parseCommitID(buf[i : i+int(ln)])
			if err != nil {
				return si, fmt.Errorf("commit_id: %w", err)
			}
			si.Hash = h
			i += int(ln)
		default:
			return si, fmt.Errorf("unexpected field %d wire %d in StoreInfo", field, wire)
		}
	}
	return si, nil
}

func parseCommitID(buf []byte) ([32]byte, error) {
	var hash [32]byte
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		field := int(tag >> 3)
		wire := tag & 0x07
		switch {
		case field == 1 && wire == 0: // version
			_, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return hash, fmt.Errorf("bad version varint")
			}
			i += m
		case field == 2 && wire == 2: // hash
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return hash, fmt.Errorf("bad hash length")
			}
			i += m
			if int(ln) != 32 {
				// Empty stores have a zero-length hash; pad to zeros.
				if int(ln) == 0 {
					return hash, nil
				}
				return hash, fmt.Errorf("expected 32-byte store hash, got %d bytes", ln)
			}
			copy(hash[:], buf[i:i+int(ln)])
			i += int(ln)
		default:
			return hash, fmt.Errorf("unexpected field %d wire %d in CommitID", field, wire)
		}
	}
	return hash, nil
}

// runVerifyOnly opens the existing pebble DB at dbDir, reads back the
// CommitInfo we wrote at s/<height>, recomputes the AppHash, and
// (optionally) compares to a reference hex / RPC.
func runVerifyOnly(dbDir string, height int64,
	expectedHex, rpcURL string, log io.Writer) error {

	fmt.Fprintf(log, "[verify-only] opening %s read-only...\n", dbDir)
	stores, err := readCommitInfo(dbDir, height)
	if err != nil {
		return fmt.Errorf("read commit-info: %w", err)
	}
	fmt.Fprintf(log, "[verify-only] found %d store entries in CommitInfo\n", len(stores))
	for _, si := range stores {
		fmt.Fprintf(log, "[verify-only]   %-24s %x\n", si.Name, si.Hash[:8])
	}

	if _, err := verifyAppHash(stores, height, expectedHex, rpcURL, log); err != nil {
		return err
	}
	return nil
}
