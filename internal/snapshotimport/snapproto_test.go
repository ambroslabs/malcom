package snapshotimport

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"
)

// TestSnapReaderNextRejectsOversizedEnvelope is the regression test for
// issue #108 finding C2: snapReader.Next reads a uvarint envelope
// length straight off the wire and hands it to make([]byte, envLen)
// without bounds. A stream advertising a MaxUint64-byte envelope wraps
// to a negative int and panics in makeslice's length guard — no
// allocation attempted, but the process dies.
//
// MaxUint64 is the cleanest test value: makeslice's len-fits-in-int
// guard fires before any allocation, so the test is host-independent
// (no risk of the kernel OOM-killing the test runner).
//
// On a fixed implementation the decoder caps envLen and returns a
// clean error before reaching make.
func TestSnapReaderNextRejectsOversizedEnvelope(t *testing.T) {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], ^uint64(0)) // MaxUint64
	stream := buf[:n]

	r := newSnapReader(bytes.NewReader(stream))

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("snapReader.Next panicked on oversized envelope "+
				"(unbounded make([]byte, envLen) reachable from the wire): %v", rec)
		}
	}()

	item, err := r.Next()
	if err == nil {
		t.Fatalf("snapReader.Next returned %+v, nil err; want an envelope-cap error", item)
	}
	if err == io.EOF {
		t.Fatal("snapReader.Next returned io.EOF; want an envelope-cap error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "envelope") ||
		!(strings.Contains(msg, "cap") ||
			strings.Contains(msg, "exceeds") ||
			strings.Contains(msg, "too large")) {
		t.Fatalf("snapReader.Next error %q is not an envelope-cap rejection", msg)
	}
}

// TestReadStoreNameRejectsOversizedNameLen covers C2's secondary case
// at internal/snapshotimport/index.go:readStoreName. There the inner
// nameLen passes a signed-int64 bound check against envEnd-s.pos, then
// is handed to make([]byte, nameLen). With envLen large but positive
// (so the bound check passes), a nameLen above maxAlloc panics
// makeslice the same way.
//
// envLen = 1<<62 and nameLen = 1<<60 are both safely positive when
// cast to int64 and both well above runtime maxAlloc (~2^48 on
// amd64), so the bound check passes and makeslice's len-out-of-range
// guard fires before any allocation.
//
// On a fixed implementation readStoreName (or its caller) caps the
// name length and returns a clean error before reaching make.
func TestReadStoreNameRejectsOversizedNameLen(t *testing.T) {
	const (
		envLen  = uint64(1 << 62)
		nameLen = uint64(1 << 60)
	)

	// StoreItem envelope wire layout:
	//   outer tag 0x0A || varint(innerLen) || inner tag 0x0A || varint(nameLen) || name...
	// The stream stops after the nameLen varint — make([]byte, nameLen)
	// is expected to panic before readExact runs.
	var stream []byte
	stream = append(stream, 0x0A)
	stream = appendUvarint(stream, envLen) // innerLen — value doesn't matter for reaching make
	stream = append(stream, 0x0A)
	stream = appendUvarint(stream, nameLen)

	s := newIndexScanner(bytes.NewReader(stream))

	defer func() {
		if rec := recover(); rec != nil {
			t.Fatalf("readStoreName panicked on oversized nameLen "+
				"(make([]byte, nameLen) reachable past the signed bound check): %v", rec)
		}
	}()

	_, err := s.readStoreName(envLen)
	if err == nil {
		t.Fatal("readStoreName returned nil error on oversized nameLen")
	}
	msg := err.Error()
	if !(strings.Contains(msg, "cap") ||
		strings.Contains(msg, "exceeds") ||
		strings.Contains(msg, "too large")) {
		t.Fatalf("readStoreName error %q is not an envelope-cap rejection", msg)
	}
}
