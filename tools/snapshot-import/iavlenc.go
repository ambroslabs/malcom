// IAVL wire-format encoding + hashing, no iavl/cosmos imports.
//
// All formats here mirror cosmos/iavl@v1.x's `Node.writeHashBytes`,
// `Node.writeBytes`, the `nodeKeyFormat` / `fastKeyFormat` /
// `metadataKeyFormat` key layouts in `nodedb.go`, and `fastnode.WriteBytes`.
// We reproduce the byte-level encoding here so that the resulting pebble
// database can be opened by an unmodified gaiad.
//
// Not exhaustive: we encode v1-format nodes (no legacy 32-byte child node
// keys) since snapshots always emit v1.

package main

import (
	"crypto/sha256"
	"encoding/binary"
	"math/bits"
)

// ─── per-store key namespaces ────────────────────────────────────────────
//
// The cosmos-sdk rootmulti store wraps every per-store database in a
// fixed prefix of "s/k:<storeName>/". Within that prefix, IAVL writes:
//
//   nodes:    "s" || version(8B BE) || nonce(4B BE)     — encoded node bytes
//   fast:     "f" || userKey                            — fast-storage entry
//   metadata: "m" || "storage_version" (etc.)           — per-store metadata
//
// The full pebble key for a node is therefore:
//   "s/k:<storeName>/" || "s" || version(8) || nonce(4)

func storePrefix(name string) []byte {
	out := make([]byte, 0, 4+len(name)+1)
	out = append(out, 's', '/', 'k', ':')
	out = append(out, name...)
	out = append(out, '/')
	return out
}

// nodeKeyBytes returns the 12-byte (version, nonce) tuple in the order
// IAVL stores it (big-endian for both, version first). This is the
// "nodeKey" used as the storage key suffix and as the value of the
// children's pointer fields in inner-node encoding.
func nodeKeyBytes(version int64, nonce uint32) []byte {
	out := make([]byte, 12)
	binary.BigEndian.PutUint64(out, uint64(version))
	binary.BigEndian.PutUint32(out[8:], nonce)
	return out
}

// nodeDBKey assembles the full pebble key for a node entry under the
// given store: storePrefix || 's' || version || nonce.
func nodeDBKey(storePrefix []byte, version int64, nonce uint32) []byte {
	out := make([]byte, 0, len(storePrefix)+1+12)
	out = append(out, storePrefix...)
	out = append(out, 's')
	var nk [12]byte
	binary.BigEndian.PutUint64(nk[:], uint64(version))
	binary.BigEndian.PutUint32(nk[8:], nonce)
	out = append(out, nk[:]...)
	return out
}

// fastDBKey returns the pebble key for a fast-storage entry:
// storePrefix || 'f' || userKey.
func fastDBKey(storePrefix, userKey []byte) []byte {
	out := make([]byte, 0, len(storePrefix)+1+len(userKey))
	out = append(out, storePrefix...)
	out = append(out, 'f')
	out = append(out, userKey...)
	return out
}

// metadataDBKey returns the pebble key for a per-store metadata entry:
// storePrefix || 'm' || metaKey.
func metadataDBKey(storePrefix []byte, metaKey string) []byte {
	out := make([]byte, 0, len(storePrefix)+1+len(metaKey))
	out = append(out, storePrefix...)
	out = append(out, 'm')
	out = append(out, metaKey...)
	return out
}

// ─── varint primitives ───────────────────────────────────────────────────

// putUvarint appends an unsigned varint (binary.PutUvarint encoding) to dst.
func putUvarint(dst []byte, x uint64) []byte {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], x)
	return append(dst, b[:n]...)
}

// putVarint appends a signed (zigzag) varint to dst, matching iavl's
// `encoding.EncodeVarint`. zigzag: ux = uint64(x)<<1; if x<0 ux = ^ux.
func putVarint(dst []byte, x int64) []byte {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return putUvarint(dst, ux)
}

// putBytes appends a varint-length-prefixed byte slice to dst. Matches
// iavl's `encoding.EncodeBytes`.
func putBytes(dst, b []byte) []byte {
	dst = putUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

// putHash32 appends a 32-byte hash with its 1-byte length prefix (0x20).
// Matches iavl's `encoding.Encode32BytesHash` — a fast path for the
// fixed-size hash output that elides binary.PutUvarint.
func putHash32(dst []byte, h []byte) []byte {
	dst = append(dst, 0x20)
	return append(dst, h...)
}

// uvarintSize returns the number of bytes binary.PutUvarint would write
// for x. Matches iavl's `EncodeUvarintSize`.
func uvarintSize(x uint64) int {
	if x == 0 {
		return 1
	}
	return (bits.Len64(x) + 6) / 7
}

// varintSize returns the number of bytes putVarint would write for x.
func varintSize(x int64) int {
	ux := uint64(x) << 1
	if x < 0 {
		ux = ^ux
	}
	return uvarintSize(ux)
}

// ─── hash pre-images ─────────────────────────────────────────────────────

// hashLeaf computes the IAVL hash of a leaf node:
//
//	sha256( varint(0) || varint(1) || varint(version) || varint(len(key)) || key
//	         || 0x20 || sha256(value) )
//
// The valueHash indirection lets light clients prove
// `(key, valueHash)` membership without exposing value bytes.
func hashLeaf(version int64, key, value []byte) [32]byte {
	pre := make([]byte, 0, 32+len(key)+8)
	pre = putVarint(pre, 0) // height
	pre = putVarint(pre, 1) // size
	pre = putVarint(pre, version)
	pre = putBytes(pre, key)
	vh := sha256.Sum256(value)
	pre = putHash32(pre, vh[:])
	return sha256.Sum256(pre)
}

// hashInner computes the IAVL hash of an inner node:
//
//	sha256( varint(height) || varint(size) || varint(version)
//	         || 0x20 || left.hash || 0x20 || right.hash )
//
// Note: the inner node's user-key field (the BST routing pivot) does
// NOT participate in the hash. Children are referenced by hash, not by
// their nodeKey.
func hashInner(version int64, height int8, size int64, left, right [32]byte) [32]byte {
	pre := make([]byte, 0, 80)
	pre = putVarint(pre, int64(height))
	pre = putVarint(pre, size)
	pre = putVarint(pre, version)
	pre = putHash32(pre, left[:])
	pre = putHash32(pre, right[:])
	return sha256.Sum256(pre)
}

// ─── on-disk node encoding (writeBytes) ──────────────────────────────────

// encodeLeafNode produces the bytes IAVL writes to pebble for a leaf:
//
//	varint(0) || varint(1) || EncodeBytes(key) || EncodeBytes(value)
func encodeLeafNode(key, value []byte) []byte {
	out := make([]byte, 0, 8+len(key)+len(value))
	out = putVarint(out, 0) // height
	out = putVarint(out, 1) // size
	out = putBytes(out, key)
	out = putBytes(out, value)
	return out
}

// encodeInnerNode produces the bytes IAVL writes to pebble for a v1
// inner node:
//
//	varint(height) || varint(size) || EncodeBytes(key) || 0x20 || hash
//	  || varint(mode=0)
//	  || varint(left.version) || varint(left.nonce)
//	  || varint(right.version) || varint(right.nonce)
//
// "mode" is 0 because we're not producing legacy children. The
// EncodeBytes(key) writes the BST routing pivot from the snapshot
// stream — IAVL uses it for in-memory BST search, not for hashing.
func encodeInnerNode(height int8, size int64, key []byte, hash [32]byte,
	leftVersion int64, leftNonce uint32,
	rightVersion int64, rightNonce uint32) []byte {

	out := make([]byte, 0, 96+len(key))
	out = putVarint(out, int64(height))
	out = putVarint(out, size)
	out = putBytes(out, key)
	out = putHash32(out, hash[:])
	out = putVarint(out, 0) // mode=0 (no legacy children)
	out = putVarint(out, leftVersion)
	out = putVarint(out, int64(leftNonce))
	out = putVarint(out, rightVersion)
	out = putVarint(out, int64(rightNonce))
	return out
}

// ─── fast-node encoding ──────────────────────────────────────────────────

// encodeFastNode produces the bytes IAVL writes for a fast-storage entry:
//
//	varint(versionLastUpdatedAt) || EncodeBytes(value)
//
// Cosmos-sdk's `Get(userKey)` reads from the fast-storage path
// `'f'+userKey` and falls back to a tree walk if missing. Pre-populating
// these entries during import skips the expensive post-load fast-storage
// upgrade pass.
func encodeFastNode(version int64, value []byte) []byte {
	out := make([]byte, 0, 16+len(value))
	out = putVarint(out, version)
	out = putBytes(out, value)
	return out
}

// ─── per-store metadata ──────────────────────────────────────────────────

// fastStorageVersionValue is what gaiad expects in the per-store metadata
// to consider fast-storage already-built. The full value written is
// "1.1.0-<latestVersion>" — iavl parses on '-'.
const fastStorageVersionValue = "1.1.0"
const fastStorageVersionDelimiter = "-"
