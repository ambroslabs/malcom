// IAVL wire-format encoding + hashing, no iavl/cosmos imports.
//
// All formats here mirror cosmos/iavl@v1.x's `Node.writeHashBytes`,
// `Node.writeBytes`, the `nodeKeyFormat` / `fastKeyFormat` /
// `metadataKeyFormat` key layouts in `nodedb.go`, and `fastnode.WriteBytes`.
// We reproduce the byte-level encoding here so that the resulting pebble
// database can be opened by an unmodified cosmos-sdk daemon.
//
// Not exhaustive: we encode v1-format nodes (no legacy 32-byte child node
// keys) since snapshots always emit v1.

package snapshotimport

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

// ─── append-into variants of the key + value encoders ────────────────────
//
// Each encoder takes the destination slice as a parameter and returns
// the appended-to slice, in the canonical Go append idiom. Lets callers
// supply a per-storeImporter scratch buffer that's reused across the
// ~28M addNode calls for a big store like bank, eliminating the
// per-call mallocgc + memmove cost the encode bucket was paying.
//
// The "Into" suffix mirrors stdlib (e.g. *big.Int.FillBytes /
// hash.Hash.Sum); callers pass `buf[:0]` to write from the start.

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
	return nodeDBKeyInto(make([]byte, 0, len(storePrefix)+1+12), storePrefix, version, nonce)
}

func nodeDBKeyInto(buf, storePrefix []byte, version int64, nonce uint32) []byte {
	buf = append(buf, storePrefix...)
	buf = append(buf, 's')
	var nk [12]byte
	binary.BigEndian.PutUint64(nk[:], uint64(version))
	binary.BigEndian.PutUint32(nk[8:], nonce)
	buf = append(buf, nk[:]...)
	return buf
}

// fastDBKey returns the pebble key for a fast-storage entry:
// storePrefix || 'f' || userKey.
func fastDBKey(storePrefix, userKey []byte) []byte {
	return fastDBKeyInto(make([]byte, 0, len(storePrefix)+1+len(userKey)), storePrefix, userKey)
}

func fastDBKeyInto(buf, storePrefix, userKey []byte) []byte {
	buf = append(buf, storePrefix...)
	buf = append(buf, 'f')
	buf = append(buf, userKey...)
	return buf
}

// metadataDBKey returns the pebble key for a per-store metadata entry:
// storePrefix || 'm' || metaKey.
func metadataDBKey(storePrefix []byte, metaKey string) []byte {
	return metadataDBKeyInto(make([]byte, 0, len(storePrefix)+1+len(metaKey)), storePrefix, metaKey)
}

func metadataDBKeyInto(buf, storePrefix []byte, metaKey string) []byte {
	buf = append(buf, storePrefix...)
	buf = append(buf, 'm')
	buf = append(buf, metaKey...)
	return buf
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
	return encodeLeafNodeInto(make([]byte, 0, 8+len(key)+len(value)), key, value)
}

func encodeLeafNodeInto(buf, key, value []byte) []byte {
	buf = putVarint(buf, 0) // height
	buf = putVarint(buf, 1) // size
	buf = putBytes(buf, key)
	buf = putBytes(buf, value)
	return buf
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
	return encodeInnerNodeInto(make([]byte, 0, 96+len(key)),
		height, size, key, hash, leftVersion, leftNonce, rightVersion, rightNonce)
}

func encodeInnerNodeInto(buf []byte,
	height int8, size int64, key []byte, hash [32]byte,
	leftVersion int64, leftNonce uint32,
	rightVersion int64, rightNonce uint32) []byte {
	buf = putVarint(buf, int64(height))
	buf = putVarint(buf, size)
	buf = putBytes(buf, key)
	buf = putHash32(buf, hash[:])
	buf = putVarint(buf, 0) // mode=0 (no legacy children)
	buf = putVarint(buf, leftVersion)
	buf = putVarint(buf, int64(leftNonce))
	buf = putVarint(buf, rightVersion)
	buf = putVarint(buf, int64(rightNonce))
	return buf
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
	return encodeFastNodeInto(make([]byte, 0, 16+len(value)), version, value)
}

func encodeFastNodeInto(buf []byte, version int64, value []byte) []byte {
	buf = putVarint(buf, version)
	buf = putBytes(buf, value)
	return buf
}

// ─── per-store metadata ──────────────────────────────────────────────────

// fastStorageVersionValue is what the daemon expects in the per-store metadata
// to consider fast-storage already-built. The full value written is
// "1.1.0-<latestVersion>" — iavl parses on '-'.
const fastStorageVersionValue = "1.1.0"
const fastStorageVersionDelimiter = "-"
