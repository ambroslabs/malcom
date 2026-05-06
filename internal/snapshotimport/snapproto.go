// Hand-rolled decoder for cometbft's SnapshotItem proto stream.
//
// The cometbft snapshot format wraps each item in a length-prefixed
// envelope; the envelope decodes as a SnapshotItem oneof with one of:
//
//	field 1: SnapshotStoreItem      { name string }
//	field 2: SnapshotIAVLItem       { key bytes; value bytes; version int64; height int32 }
//	field 3: SnapshotExtensionMeta  { name string; format uint32 }
//	field 4: SnapshotExtensionPayload { payload bytes }
//
// We only act on types 1 and 2 here. Extension items are recognised so the
// importer can correctly skip them; their payloads are handed off to the
// caller as raw bytes so a higher-layer driver can dump them to disk if it
// needs to (the cosmos-sdk wasm store, for instance, ships its blobs as
// type-4 items).
//
// No protobuf library import — protobuf wire format is simple enough to
// hand-decode and the structure is closed (closed-set of fields, well-
// understood encoding). Keeping the decoder local avoids dragging in any
// google/protobuf dependency.

package snapshotimport

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// itemType discriminates the four oneof variants of SnapshotItem.
type itemType uint8

const (
	itemTypeStore     itemType = 1
	itemTypeIAVL      itemType = 2
	itemTypeExtMeta   itemType = 3
	itemTypeExtPayload itemType = 4
)

// snapItem is a decoded item handed to the importer driver. Only one of
// the variant-specific fields is populated, selected by Type.
type snapItem struct {
	Type itemType

	// type 1: SnapshotStoreItem
	StoreName string

	// type 2: SnapshotIAVLItem
	IAVLKey     []byte
	IAVLValue   []byte
	IAVLVersion int64
	IAVLHeight  int8

	// type 3: SnapshotExtensionMeta
	ExtName   string
	ExtFormat uint32

	// type 4: SnapshotExtensionPayload
	ExtPayload []byte
}

// snapReader pulls SnapshotItem proto messages off an io.Reader. Each
// call to Next returns one decoded item or io.EOF.
type snapReader struct {
	br *bufio.Reader
}

func newSnapReader(r io.Reader) *snapReader {
	return &snapReader{br: bufio.NewReaderSize(r, 1<<20)}
}

// Next returns the next SnapshotItem or io.EOF when the stream ends.
//
// The wire envelope is: varint(itemLen) || itemBytes. itemBytes itself
// is a SnapshotItem proto whose first byte encodes (oneof_field<<3)|2
// (length-delimited wire type), followed by varint(innerLen) and the
// inner message bytes.
func (s *snapReader) Next() (*snapItem, error) {
	for {
		envLen, err := binary.ReadUvarint(s.br)
		if err == io.EOF {
			return nil, io.EOF
		}
		if err != nil {
			return nil, fmt.Errorf("read envelope length: %w", err)
		}
		if envLen == 0 {
			// length-zero envelopes are technically valid; skip them
			continue
		}
		buf := make([]byte, envLen)
		if _, err := io.ReadFull(s.br, buf); err != nil {
			return nil, fmt.Errorf("read envelope body: %w", err)
		}

		if len(buf) == 0 {
			continue
		}

		// First byte is the oneof field tag.
		// (field_number << 3) | wire_type, wire_type=2 (length-delimited).
		tag := buf[0]
		field := tag >> 3
		wire := tag & 7
		if wire != 2 {
			return nil, fmt.Errorf("unexpected wire type %d on oneof tag (want length-delimited)", wire)
		}

		innerLen, n := binary.Uvarint(buf[1:])
		if n <= 0 {
			return nil, errors.New("bad inner length varint on SnapshotItem")
		}
		body := buf[1+n : 1+n+int(innerLen)]

		item := &snapItem{Type: itemType(field)}
		if err := decodeOneOf(item, body); err != nil {
			return nil, err
		}
		return item, nil
	}
}

// decodeOneOf parses the inner message bytes for whichever oneof variant
// the caller pre-selected via item.Type.
func decodeOneOf(item *snapItem, buf []byte) error {
	switch item.Type {
	case itemTypeStore:
		item.StoreName = string(getBytesField(buf, 1))
	case itemTypeIAVL:
		item.IAVLKey = copyBytesField(buf, 1)
		// value field (2) may be absent for inner nodes. iavl
		// distinguishes "absent" (inner) from "empty leaf" only via
		// height==0; we intentionally do NOT normalise nil→empty here
		// for inner nodes.
		item.IAVLValue = copyBytesField(buf, 2)
		item.IAVLVersion = int64(getVarintField(buf, 3))
		item.IAVLHeight = int8(getVarintField(buf, 4))
		// Cosmos-sdk leaves with empty value: proto3 omits zero-length
		// bytes fields entirely. Restore an empty (non-nil) slice on
		// height==0 so the importer can encode the leaf correctly.
		if item.IAVLHeight == 0 && item.IAVLValue == nil {
			item.IAVLValue = []byte{}
		}
	case itemTypeExtMeta:
		item.ExtName = string(getBytesField(buf, 1))
		item.ExtFormat = uint32(getVarintField(buf, 2))
	case itemTypeExtPayload:
		item.ExtPayload = copyBytesField(buf, 1)
	default:
		return fmt.Errorf("unknown SnapshotItem oneof field %d", item.Type)
	}
	return nil
}

// getBytesField returns the bytes value at the given field number, or
// nil if the field isn't present. Returned slice aliases buf — the
// caller must copy if they need to keep it across the next Next() call.
func getBytesField(buf []byte, field int) []byte {
	want := byte((field << 3) | 2)
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		if tag == want {
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return nil
			}
			i += m
			return buf[i : i+int(ln)]
		}
		// skip this field
		switch tag & 7 {
		case 0:
			_, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return nil
			}
			i += m
		case 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return nil
			}
			i += m + int(ln)
		default:
			return nil
		}
	}
	return nil
}

// copyBytesField returns a fresh copy of the named field's bytes (or
// nil if the field is absent). Use this when the caller wants to retain
// the bytes across the next Next() call — getBytesField returns slices
// that alias the envelope buffer, which gets reused.
func copyBytesField(buf []byte, field int) []byte {
	v := getBytesField(buf, field)
	if v == nil {
		return nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

// getVarintField returns the varint value at the given field number, or
// 0 if the field isn't present.
func getVarintField(buf []byte, field int) uint64 {
	want := byte((field << 3) | 0)
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		if tag == want {
			v, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return 0
			}
			return v
		}
		switch tag & 7 {
		case 0:
			_, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return 0
			}
			i += m
		case 2:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return 0
			}
			i += m + int(ln)
		default:
			return 0
		}
	}
	return 0
}
