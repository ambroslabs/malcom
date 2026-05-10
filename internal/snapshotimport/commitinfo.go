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

	// LeafCount is the number of IAVL leaves the import wrote into
	// this store. Populated by the import; used by VerifyFast to
	// cross-check the f/ entry count. Not part of the on-wire
	// CommitInfo (encodeStoreInfo only reads Name+Hash).
	LeafCount uint64
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

// ─── decoders (used by the standalone verify-fast subcommand) ────────────

// DecodeLatestVersion extracts the version from a `latest_version`
// pebble value (the same protobuf shape latestVersionBytes writes).
func DecodeLatestVersion(b []byte) (int64, error) {
	pos := 0
	for pos < len(b) {
		tag, n := binary.Uvarint(b[pos:])
		if n <= 0 {
			return 0, fmt.Errorf("bad tag varint")
		}
		pos += n
		field, wire := tag>>3, tag&7
		switch wire {
		case 0:
			v, n := binary.Uvarint(b[pos:])
			if n <= 0 {
				return 0, fmt.Errorf("bad varint")
			}
			if field == 1 {
				return int64(v), nil
			}
			pos += n
		case 2:
			length, n := binary.Uvarint(b[pos:])
			if n <= 0 {
				return 0, fmt.Errorf("bad length")
			}
			pos += n + int(length)
		default:
			return 0, fmt.Errorf("unsupported wire type %d", wire)
		}
	}
	return 0, fmt.Errorf("latest_version: missing version field")
}

// DecodeStoreNames extracts store names from a CommitInfo pebble
// value (the same protobuf shape commitInfoBytes writes). The
// returned names follow stream order from the encoder, which sorted
// stores by Name before encoding.
func DecodeStoreNames(b []byte) ([]string, error) {
	var names []string
	pos := 0
	for pos < len(b) {
		tag, n := binary.Uvarint(b[pos:])
		if n <= 0 {
			return nil, fmt.Errorf("bad tag varint")
		}
		pos += n
		field, wire := tag>>3, tag&7
		switch wire {
		case 0:
			_, n := binary.Uvarint(b[pos:])
			if n <= 0 {
				return nil, fmt.Errorf("bad varint")
			}
			pos += n
		case 2:
			length, n := binary.Uvarint(b[pos:])
			if n <= 0 {
				return nil, fmt.Errorf("bad length")
			}
			pos += n
			end := pos + int(length)
			if end > len(b) {
				return nil, fmt.Errorf("length-delim out of bounds")
			}
			if field == 2 {
				name, err := decodeStoreInfoName(b[pos:end])
				if err != nil {
					return nil, fmt.Errorf("StoreInfo: %w", err)
				}
				names = append(names, name)
			}
			pos = end
		default:
			return nil, fmt.Errorf("unsupported wire type %d", wire)
		}
	}
	return names, nil
}

func decodeStoreInfoName(b []byte) (string, error) {
	pos := 0
	for pos < len(b) {
		tag, n := binary.Uvarint(b[pos:])
		if n <= 0 {
			return "", fmt.Errorf("bad tag")
		}
		pos += n
		field, wire := tag>>3, tag&7
		switch wire {
		case 0:
			_, n := binary.Uvarint(b[pos:])
			pos += n
		case 2:
			length, n := binary.Uvarint(b[pos:])
			pos += n
			end := pos + int(length)
			if field == 1 {
				return string(b[pos:end]), nil
			}
			pos = end
		}
	}
	return "", fmt.Errorf("missing name field")
}

// CommitInfoKey returns the pebble key under which CommitInfo for
// `version` is stored; exported for the standalone verify-fast path.
func CommitInfoKey(version int64) []byte { return commitInfoKey(version) }

// LatestVersionKey is the fixed pebble key used for latest_version;
// exported for the standalone verify-fast path.
var LatestVersionKey = latestVersionKey
