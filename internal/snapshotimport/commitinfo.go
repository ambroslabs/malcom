// Hand-rolled encoder for cosmos-sdk's `CommitInfo` and `latest_version`
// records. These are the two database entries gaiad reads at startup to
// know where state-sync left things; without them, gaiad reports a fresh
// chain and re-runs initial state-sync.
//
// Wire format (cosmos-sdk store/types/commit_info.proto):
//
//	message CommitInfo {
//	    int64 version = 1;
//	    repeated StoreInfo store_infos = 2;
//	    // (timestamp field omitted — gaiad's load path tolerates absence)
//	}
//	message StoreInfo {
//	    string name = 1;
//	    CommitID commit_id = 2;
//	}
//	message CommitID {
//	    int64 version = 1;
//	    bytes hash = 2;
//	}
//
// Pebble keys:
//
//	"s/<version>" → CommitInfo proto bytes
//	"s/latest"    → varint-field(1) of the latest version
//
// (Note the leading "s/" — these live in the rootmulti namespace, not
// inside any per-store prefix.)

package snapshotimport

import (
	"encoding/binary"
	"fmt"
)

// StoreInfo is one entry in the commit-info store_infos repeated field.
//
// Hash is variable-length intentionally: cosmos-sdk represents an
// untouched store (an IAVL tree with no nodes) as an empty hash, not
// a 32-byte string of zeros. Conflating the two breaks consensus —
// `simpleMap.Set` hashes the value, and `sha256("")` ≠ `sha256(0×32)`,
// which propagates into a divergent AppHash. Use `nil` (or the empty
// slice) for empty stores; the encoder writes a length-0 field, which
// chain runtimes parse identically to an absent field per proto3.
type StoreInfo struct {
	Name string
	Hash []byte
}

// commitInfoBytes returns the protobuf-encoded CommitInfo for the given
// version + per-store roots.
func commitInfoBytes(version int64, infos []StoreInfo) []byte {
	var out []byte
	out = appendUvarintField(out, 1, uint64(version))
	for _, si := range infos {
		out = appendEmbeddedField(out, 2, encodeStoreInfo(si, version))
	}
	return out
}

// commitInfoKey returns the pebble key under which CommitInfo for
// `version` should be stored.
func commitInfoKey(version int64) []byte {
	return []byte(fmt.Sprintf("s/%d", version))
}

// latestVersionKey is the fixed pebble key gaiad reads to discover
// the most recent committed version.
var latestVersionKey = []byte("s/latest")

// latestVersionBytes returns the protobuf-encoded latest-version record:
// a single int64 field 1 (so gaiad's existing decoder can read it).
func latestVersionBytes(version int64) []byte {
	return appendUvarintField(nil, 1, uint64(version))
}

func encodeStoreInfo(si StoreInfo, ver int64) []byte {
	var out []byte
	out = appendStringField(out, 1, si.Name)
	out = appendEmbeddedField(out, 2, encodeCommitID(ver, si.Hash))
	return out
}

func encodeCommitID(version int64, hash []byte) []byte {
	var out []byte
	out = appendUvarintField(out, 1, uint64(version))
	out = appendBytesField(out, 2, hash)
	return out
}

// ─── proto wire-format primitives ────────────────────────────────────────

func appendUvarintField(dst []byte, field int, v uint64) []byte {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], v)
	dst = append(dst, byte(field<<3)|0)
	dst = append(dst, b[:n]...)
	return dst
}

func appendStringField(dst []byte, field int, s string) []byte {
	return appendBytesField(dst, field, []byte(s))
}

func appendBytesField(dst []byte, field int, b []byte) []byte {
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], uint64(len(b)))
	dst = append(dst, byte(field<<3)|2)
	dst = append(dst, lb[:n]...)
	dst = append(dst, b...)
	return dst
}

func appendEmbeddedField(dst []byte, field int, body []byte) []byte {
	return appendBytesField(dst, field, body)
}
