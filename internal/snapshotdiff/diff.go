package snapshotdiff

import (
	"bufio"
	"compress/zlib"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Stats summarises a diff after generation.
type Stats struct {
	Stores        int
	Extensions    int
	Inserts       uint64
	Updates       uint64
	Deletes       uint64
	ExtAdds       uint64
	ExtRemoves    uint64
	BodyBytesRaw  uint64 // pre-compression body size
	BodyBytesGz   uint64 // post-compression body size
	PerStore      map[string]StoreStats
}

type StoreStats struct {
	BaseItems   uint64
	TargetItems uint64
	Inserts     uint64
	Updates     uint64
	Deletes     uint64
}

// Compute reads the base and target snapshot directories, writes a diff
// file at outPath, and returns Stats describing what changed.
//
// Memory cost is O(items in largest store) — for cosmoshub the bank store
// at ~5 GB is the worst case, dominating peak RSS. If RAM is tight, swap
// for a disk-backed key→hash index.
func Compute(baseDir, targetDir, outPath string, baseHashHex, targetHashHex string, baseHeight, targetHeight uint64) (Stats, error) {
	var stats Stats
	stats.PerStore = map[string]StoreStats{}

	// Open output. Header uncompressed; body zlib-compressed.
	out, err := os.Create(outPath)
	if err != nil {
		return stats, fmt.Errorf("create %s: %w", outPath, err)
	}
	defer out.Close()
	if err := WriteHeader(out, FileHeader{
		BaseHeight:    baseHeight,
		TargetHeight:  targetHeight,
		BaseHashHex:   baseHashHex,
		TargetHashHex: targetHashHex,
	}); err != nil {
		return stats, err
	}

	// Wrap output with a counting writer + zlib for the body.
	// We tap into the byte counts so Stats is informative.
	rawCtr := &countWriter{w: out}
	zw, err := zlib.NewWriterLevel(rawCtr, zlib.DefaultCompression)
	if err != nil {
		return stats, err
	}
	defer zw.Close()
	gzCtr := &countWriter{w: zw}
	bw := bufio.NewWriterSize(gzCtr, 1<<20)
	emit := func(record []byte) error {
		// Write varint length, then record bytes.
		var lb [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(lb[:], uint64(len(record)))
		if _, err := bw.Write(lb[:n]); err != nil {
			return err
		}
		if _, err := bw.Write(record); err != nil {
			return err
		}
		return nil
	}

	// Process target snapshot: load each store's items into memory keyed by
	// key bytes, then walk the base snapshot and emit diff records.
	//
	// We pass through the snapshot stream once per side. Per-store maps are
	// freed before the next store starts, capping peak memory.
	if err := computeStream(baseDir, targetDir, &stats, emit); err != nil {
		return stats, err
	}

	if err := bw.Flush(); err != nil {
		return stats, err
	}
	if err := zw.Close(); err != nil {
		return stats, err
	}
	stats.BodyBytesRaw = gzCtr.n
	stats.BodyBytesGz = rawCtr.n
	return stats, nil
}

// computeStream is the heart of the differ. It walks both snapshots in
// parallel via storeReader, comparing the items in each store one store at
// a time.
func computeStream(baseDir, targetDir string, stats *Stats, emit func([]byte) error) error {
	baseRdr, err := newStoreReader(baseDir)
	if err != nil {
		return fmt.Errorf("open base: %w", err)
	}
	defer baseRdr.Close()
	targetRdr, err := newStoreReader(targetDir)
	if err != nil {
		return fmt.Errorf("open target: %w", err)
	}
	defer targetRdr.Close()

	// We need to interleave stores from both sides. Both walk in
	// alphabetical store order (cosmos-sdk emits stores sorted by name).
	for {
		baseStore, baseDone, err := baseRdr.peekStore()
		if err != nil {
			return err
		}
		targetStore, targetDone, err := targetRdr.peekStore()
		if err != nil {
			return err
		}
		if baseDone && targetDone {
			break
		}

		// Compare next-store names; advance whichever is alphabetically first.
		switch {
		case baseDone:
			// Only target left → all-INSERT
			if err := diffStore(stats, emit, nil, targetRdr, targetStore); err != nil {
				return err
			}
		case targetDone:
			// Only base left → all-DELETE
			if err := diffStore(stats, emit, baseRdr, nil, baseStore); err != nil {
				return err
			}
		case baseStore < targetStore:
			if err := diffStore(stats, emit, baseRdr, nil, baseStore); err != nil {
				return err
			}
		case baseStore > targetStore:
			if err := diffStore(stats, emit, nil, targetRdr, targetStore); err != nil {
				return err
			}
		default: // names match
			if err := diffStore(stats, emit, baseRdr, targetRdr, baseStore); err != nil {
				return err
			}
		}
	}

	// Phase 2: extensions. Streams for both snapshots, compare payload sets.
	if err := diffExtensions(stats, emit, baseRdr, targetRdr); err != nil {
		return err
	}
	return nil
}

// diffStore handles one store using a streaming merge of base and target
// IAVL items. baseRdr or targetRdr may be nil if that side has no records
// for this store (causing all-INSERT or all-DELETE).
//
// Both snapshot streams emit IAVL items in deterministic IAVL traversal
// order. We compare full record bytes (the SnapshotIAVLItem proto, which
// includes key+value+version+height) so identical records align and diff
// only at the divergent points. Records present in both streams (full
// byte equality) emit no diff record. This is O(1) memory per store —
// the previous in-memory target map peaked at ~10 GB for the bank store
// and was the cause of OOM kills during diff generation.
//
// When key bytes match but the full record differs (e.g., same (k, v) but
// different version), we emit Update (k, target_value). When keys differ,
// we emit Delete (smaller side) or Insert (larger side) and advance.
func diffStore(stats *Stats, emit func([]byte) error, baseRdr, targetRdr *storeReader, name string) error {
	if err := emit(buildOneOfMessage(tagStoreEnter, encStringField(1, name))); err != nil {
		return err
	}
	stats.Stores++
	ss := stats.PerStore[name]

	var baseRec, targetRec []byte
	baseHas, targetHas := false, false

	advanceBase := func() error {
		if baseRdr == nil {
			return nil
		}
		baseHas = false
		for !baseHas {
			done, err := baseRdr.readNextItemInStoreRaw(name, func(rec []byte) {
				if !isLeafIAVL(rec) {
					return
				}
				baseRec = append(baseRec[:0], rec...)
				baseHas = true
				ss.BaseItems++
			})
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
		return nil
	}
	advanceTarget := func() error {
		if targetRdr == nil {
			return nil
		}
		targetHas = false
		for !targetHas {
			done, err := targetRdr.readNextItemInStoreRaw(name, func(rec []byte) {
				if !isLeafIAVL(rec) {
					return
				}
				targetRec = append(targetRec[:0], rec...)
				targetHas = true
				ss.TargetItems++
			})
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
		return nil
	}

	if err := advanceBase(); err != nil {
		return err
	}
	if err := advanceTarget(); err != nil {
		return err
	}

	for baseHas || targetHas {
		switch {
		case !baseHas:
			k, v := parseIAVLKeyValue(targetRec)
			ss.Inserts++
			stats.Inserts++
			if err := emit(buildOneOfMessage(tagInsert, encBytesField(1, k), encBytesField(2, v))); err != nil {
				return err
			}
			if err := advanceTarget(); err != nil {
				return err
			}
		case !targetHas:
			k, _ := parseIAVLKeyValue(baseRec)
			ss.Deletes++
			stats.Deletes++
			if err := emit(buildOneOfMessage(tagDelete, encBytesField(1, k))); err != nil {
				return err
			}
			if err := advanceBase(); err != nil {
				return err
			}
		default:
			cmp := bytesCompare(baseRec, targetRec)
			switch {
			case cmp == 0:
				if err := advanceBase(); err != nil {
					return err
				}
				if err := advanceTarget(); err != nil {
					return err
				}
			case cmp < 0:
				baseK, _ := parseIAVLKeyValue(baseRec)
				targetK, targetV := parseIAVLKeyValue(targetRec)
				if bytesEqual(baseK, targetK) {
					ss.Updates++
					stats.Updates++
					if err := emit(buildOneOfMessage(tagUpdate, encBytesField(1, baseK), encBytesField(2, targetV))); err != nil {
						return err
					}
					if err := advanceBase(); err != nil {
						return err
					}
					if err := advanceTarget(); err != nil {
						return err
					}
				} else {
					ss.Deletes++
					stats.Deletes++
					if err := emit(buildOneOfMessage(tagDelete, encBytesField(1, baseK))); err != nil {
						return err
					}
					if err := advanceBase(); err != nil {
						return err
					}
				}
			case cmp > 0:
				baseK, _ := parseIAVLKeyValue(baseRec)
				targetK, targetV := parseIAVLKeyValue(targetRec)
				if bytesEqual(baseK, targetK) {
					ss.Updates++
					stats.Updates++
					if err := emit(buildOneOfMessage(tagUpdate, encBytesField(1, baseK), encBytesField(2, targetV))); err != nil {
						return err
					}
					if err := advanceBase(); err != nil {
						return err
					}
					if err := advanceTarget(); err != nil {
						return err
					}
				} else {
					ss.Inserts++
					stats.Inserts++
					if err := emit(buildOneOfMessage(tagInsert, encBytesField(1, targetK), encBytesField(2, targetV))); err != nil {
						return err
					}
					if err := advanceTarget(); err != nil {
						return err
					}
				}
			}
		}
	}

	if err := emit(buildOneOfMessage(tagStoreLeave)); err != nil {
		return err
	}
	stats.PerStore[name] = ss
	return nil
}

// isLeafIAVL returns true if the SnapshotIAVLItem body has height 0 (or
// no height field, which is the proto default for int32). Inner IAVL
// nodes have height > 0 and are excluded from leaf-only diffs because
// their value bytes change with every tree mutation (the value is the
// child hash, which depends on version), producing huge spurious diffs.
func isLeafIAVL(body []byte) bool {
	// SnapshotIAVLItem field 4 is height (int32). Walk the proto looking
	// for it; absence == 0 == leaf.
	for i := 0; i < len(body); {
		tag := body[i]
		i++
		if tag == 0x20 { // field 4 wire-type 0 (varint)
			v, m := binary.Uvarint(body[i:])
			if m <= 0 {
				return true
			}
			return v == 0
		}
		switch tag & 0x07 {
		case 0:
			_, m := binary.Uvarint(body[i:])
			if m <= 0 {
				return true
			}
			i += m
		case 2:
			ln, m := binary.Uvarint(body[i:])
			if m <= 0 || i+m+int(ln) > len(body) {
				return true
			}
			i += m + int(ln)
		default:
			return true
		}
	}
	return true
}

// bytesCompare returns -1, 0, +1 like bytes.Compare without importing
// the stdlib package.
func bytesCompare(a, b []byte) int {
	la, lb := len(a), len(b)
	n := la
	if lb < n {
		n = lb
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if la < lb {
		return -1
	}
	if la > lb {
		return 1
	}
	return 0
}

// diffExtensions compares payload sets of every extension module. Identifies
// adds and removes by sha256 of payload. Order-independent.
func diffExtensions(stats *Stats, emit func([]byte) error, baseRdr, targetRdr *storeReader) error {
	baseExt, err := baseRdr.readAllExtensions()
	if err != nil {
		return err
	}
	targetExt, err := targetRdr.readAllExtensions()
	if err != nil {
		return err
	}
	// Union of extension names.
	names := map[string]bool{}
	for n := range baseExt {
		names[n] = true
	}
	for n := range targetExt {
		names[n] = true
	}
	sortedNames := make([]string, 0, len(names))
	for n := range names {
		sortedNames = append(sortedNames, n)
	}
	sort.Strings(sortedNames)

	for _, name := range sortedNames {
		stats.Extensions++
		base := baseExt[name]
		target := targetExt[name]
		// Use the format from whichever side has the extension.
		format := uint32(1)
		if base != nil {
			format = base.format
		} else if target != nil {
			format = target.format
		}
		if err := emit(buildOneOfMessage(tagExtEnter,
			encStringField(1, name),
			encVarintField(2, uint64(format)))); err != nil {
			return err
		}

		baseHashes := map[[32]byte]bool{}
		if base != nil {
			for _, p := range base.payloads {
				baseHashes[sha256.Sum256(p)] = true
			}
		}
		targetByHash := map[[32]byte][]byte{}
		if target != nil {
			for _, p := range target.payloads {
				targetByHash[sha256.Sum256(p)] = p
			}
		}
		// Removes: in base, not in target.
		for h := range baseHashes {
			if _, ok := targetByHash[h]; !ok {
				// Need the original payload bytes for the remove record so
				// the applier can locate the right payload to drop.
				for _, p := range base.payloads {
					if sha256.Sum256(p) == h {
						stats.ExtRemoves++
						_ = emit(buildOneOfMessage(tagExtRemove, encBytesField(1, p)))
						break
					}
				}
			}
		}
		// Adds: in target, not in base.
		for h, p := range targetByHash {
			if !baseHashes[h] {
				stats.ExtAdds++
				_ = emit(buildOneOfMessage(tagExtAdd, encBytesField(1, p)))
			}
		}
		if err := emit(buildOneOfMessage(tagExtLeave)); err != nil {
			return err
		}
	}
	return nil
}

// ─── Encoding helpers ────────────────────────────────────────────────────

// buildOneOfMessage wraps a record under the top-level oneof field with the
// given tag (length-delimited). innerFields is already a serialized
// proto-style sub-message body.
func buildOneOfMessage(oneofTag int, innerFields ...[]byte) []byte {
	// Combine inner fields to compute submessage length.
	innerLen := 0
	for _, f := range innerFields {
		innerLen += len(f)
	}
	out := make([]byte, 0, 2+innerLen)
	// Top-level field tag (oneofTag, wire type 2)
	tagByte := byte((oneofTag << 3) | 2)
	out = append(out, tagByte)
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], uint64(innerLen))
	out = append(out, lb[:n]...)
	for _, f := range innerFields {
		out = append(out, f...)
	}
	return out
}

// encStringField encodes a string as proto field N wire-type 2.
func encStringField(field int, s string) []byte {
	return encBytesField(field, []byte(s))
}

// encBytesField encodes bytes as proto field N wire-type 2.
func encBytesField(field int, b []byte) []byte {
	out := make([]byte, 0, 2+len(b))
	out = append(out, byte((field<<3)|2))
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], uint64(len(b)))
	out = append(out, lb[:n]...)
	out = append(out, b...)
	return out
}

// encVarintField encodes a varint as proto field N wire-type 0.
func encVarintField(field int, v uint64) []byte {
	out := make([]byte, 0, 1+binary.MaxVarintLen64)
	out = append(out, byte((field<<3)|0))
	var lb [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lb[:], v)
	out = append(out, lb[:n]...)
	return out
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// countWriter wraps an io.Writer counting bytes written.
type countWriter struct {
	w io.Writer
	n uint64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += uint64(n)
	return n, err
}

// ─── Snapshot reader ─────────────────────────────────────────────────────

// storeReader walks a snapshot directory's chunks → zlib → SnapshotItems,
// stateful around the current store. Designed for one-pass forward scans.
type storeReader struct {
	files []*os.File
	zr    io.ReadCloser
	br    *bufio.Reader

	curStore string // current store name (or "" if before first / after last)
	pendingExt *extState // tracks extension payloads after stores end

	// Look-ahead cache for peekItem / consumeItem.
	cached    []byte
	cachedTag uint8
}

type extState struct {
	currentName   string
	currentFormat uint32
	all           map[string]*extData // name → payloads
}
type extData struct {
	format   uint32
	payloads [][]byte
}

func newStoreReader(dir string) (*storeReader, error) {
	chunks, err := listChunkPaths(dir)
	if err != nil {
		return nil, err
	}
	files := make([]*os.File, 0, len(chunks))
	readers := make([]io.Reader, 0, len(chunks))
	for _, p := range chunks {
		f, err := os.Open(p)
		if err != nil {
			for _, x := range files {
				x.Close()
			}
			return nil, err
		}
		files = append(files, f)
		readers = append(readers, f)
	}
	multi := io.MultiReader(readers...)
	zr, err := zlib.NewReader(multi)
	if err != nil {
		for _, x := range files {
			x.Close()
		}
		return nil, err
	}
	return &storeReader{
		files: files,
		zr:    zr,
		br:    bufio.NewReaderSize(zr, 1<<20),
		pendingExt: &extState{all: map[string]*extData{}},
	}, nil
}

func (s *storeReader) Close() error {
	if s.zr != nil {
		s.zr.Close()
	}
	for _, f := range s.files {
		f.Close()
	}
	return nil
}

// peekStore returns the next store name we'd read items from, or sets done
// when we've finished all stores in this snapshot. Idempotent — safe to
// call repeatedly without consuming items.
func (s *storeReader) peekStore() (name string, done bool, err error) {
	if s.curStore != "" {
		return s.curStore, false, nil
	}
	// Advance until we hit a StoreItem or end of stores section.
	for {
		tag, body, end, err := s.peekItem()
		if err != nil {
			return "", false, err
		}
		if end {
			return "", true, nil
		}
		switch tag {
		case 1: // StoreItem
			storeName := parseStoreName(body)
			s.curStore = storeName
			s.consumeItem()
			return s.curStore, false, nil
		case 3: // ExtensionMeta — stores are over
			return "", true, nil
		case 4: // ExtensionPayload — stores are over
			return "", true, nil
		default:
			// Unknown / IAVL outside a store section — skip.
			s.consumeItem()
		}
	}
}

// readNextItemInStoreRaw reads one IAVL item from the current store and
// calls fn with the raw inner SnapshotIAVLItem proto bytes (key + value +
// version + height fields, all of them). Used by the streaming differ to
// compare full records without copying out individual fields. When the
// store ends, returns done=true.
func (s *storeReader) readNextItemInStoreRaw(expectStore string, fn func(rec []byte)) (done bool, err error) {
	if s.curStore != expectStore {
		return true, nil
	}
	tag, body, end, err := s.peekItem()
	if err != nil {
		return false, err
	}
	if end {
		s.curStore = ""
		return true, nil
	}
	switch tag {
	case 2: // IAVL
		fn(body)
		s.consumeItem()
		return false, nil
	case 1: // next StoreItem — current store is done
		s.curStore = ""
		return true, nil
	case 3, 4: // extensions — stores are over
		s.curStore = ""
		return true, nil
	default:
		s.consumeItem()
		return false, nil
	}
}

// readNextItemInStore reads one IAVL item from the current store and calls
// fn with its (key, value). When the store ends (next StoreItem or EOF or
// extension), returns done=true.
func (s *storeReader) readNextItemInStore(expectStore string, fn func(k, v []byte)) (done bool, err error) {
	if s.curStore != expectStore {
		// Caller didn't position us correctly — done.
		return true, nil
	}
	tag, body, end, err := s.peekItem()
	if err != nil {
		return false, err
	}
	if end {
		s.curStore = ""
		return true, nil
	}
	switch tag {
	case 2: // IAVL
		k, v := parseIAVLKeyValue(body)
		fn(k, v)
		s.consumeItem()
		return false, nil
	case 1: // next StoreItem — current store is done
		s.curStore = ""
		return true, nil
	case 3, 4: // extensions — stores are over
		s.curStore = ""
		return true, nil
	default:
		// Unknown — skip
		s.consumeItem()
		return false, nil
	}
}

// readAllExtensions consumes everything remaining in the snapshot and
// returns extensions grouped by name.
func (s *storeReader) readAllExtensions() (map[string]*extData, error) {
	out := s.pendingExt.all
	for {
		tag, body, end, err := s.peekItem()
		if err != nil {
			return nil, err
		}
		if end {
			break
		}
		switch tag {
		case 3: // ExtensionMeta
			name, format := parseExtMeta(body)
			s.pendingExt.currentName = name
			s.pendingExt.currentFormat = format
			if _, ok := out[name]; !ok {
				out[name] = &extData{format: format}
			}
		case 4: // ExtensionPayload
			name := s.pendingExt.currentName
			payload := parseExtPayload(body)
			if name != "" {
				out[name].payloads = append(out[name].payloads, payload)
			}
		default:
			// skip
		}
		s.consumeItem()
	}
	return out, nil
}

// peekItem returns the *next* SnapshotItem's outer field tag (not yet
// consumed) and its inner sub-message body. Caller calls consumeItem() to
// commit. Returns end=true at EOF.
//
// Internally caches the lookahead so a peek + consume combo costs one read.
func (s *storeReader) peekItem() (tag uint8, body []byte, end bool, err error) {
	if s.cached != nil {
		return s.cachedTag, s.cached, false, nil
	}
	length, err := binary.ReadUvarint(s.br)
	if err == io.EOF {
		return 0, nil, true, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	if length == 0 {
		return s.peekItem()
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(s.br, buf); err != nil {
		return 0, nil, false, err
	}
	t := buf[0] >> 3
	// Inner sub-message: skip the outer tag + length prefix.
	innerLen, n := binary.Uvarint(buf[1:])
	if n <= 0 || int(innerLen) > len(buf)-1-n {
		// Malformed; cache anyway so consumeItem knows to drop it.
		s.cached = buf
		s.cachedTag = t
		return t, buf, false, nil
	}
	body = buf[1+n : 1+n+int(innerLen)]
	s.cached = body
	s.cachedTag = t
	return t, body, false, nil
}

func (s *storeReader) consumeItem() {
	s.cached = nil
}

// ─── Snapshot proto sub-message decoders ─────────────────────────────────

func parseStoreName(buf []byte) string {
	// SnapshotStoreItem is itself { string name = 1 } — but the buf passed
	// in here is *its inner body*, not wrapped further. So we expect:
	//   tag 0x0A | varint len | name bytes
	if len(buf) == 0 || buf[0] != 0x0A {
		return ""
	}
	nameLen, m := binary.Uvarint(buf[1:])
	if m <= 0 || 1+m+int(nameLen) > len(buf) {
		return ""
	}
	return string(buf[1+m : 1+m+int(nameLen)])
}

func parseIAVLKeyValue(buf []byte) (key, value []byte) {
	// SnapshotIAVLItem { bytes key = 1, bytes value = 2, ... }
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		switch tag {
		case 0x0A: // key
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return key, value
			}
			i += m
			key = append([]byte(nil), buf[i:i+int(ln)]...)
			i += int(ln)
		case 0x12: // value
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return key, value
			}
			i += m
			value = append([]byte(nil), buf[i:i+int(ln)]...)
			i += int(ln)
		default:
			// Skip unknown field by wire type.
			wire := tag & 0x07
			switch wire {
			case 0:
				_, m := binary.Uvarint(buf[i:])
				if m <= 0 {
					return key, value
				}
				i += m
			case 2:
				ln, m := binary.Uvarint(buf[i:])
				if m <= 0 || i+m+int(ln) > len(buf) {
					return key, value
				}
				i += m + int(ln)
			default:
				return key, value
			}
		}
	}
	return key, value
}

func parseExtMeta(buf []byte) (name string, format uint32) {
	for i := 0; i < len(buf); {
		tag := buf[i]
		i++
		switch tag {
		case 0x0A:
			ln, m := binary.Uvarint(buf[i:])
			if m <= 0 || i+m+int(ln) > len(buf) {
				return name, format
			}
			i += m
			name = string(buf[i : i+int(ln)])
			i += int(ln)
		case 0x10:
			v, m := binary.Uvarint(buf[i:])
			if m <= 0 {
				return name, format
			}
			i += m
			format = uint32(v)
		default:
			wire := tag & 0x07
			switch wire {
			case 0:
				_, m := binary.Uvarint(buf[i:])
				if m <= 0 {
					return name, format
				}
				i += m
			case 2:
				ln, m := binary.Uvarint(buf[i:])
				if m <= 0 || i+m+int(ln) > len(buf) {
					return name, format
				}
				i += m + int(ln)
			default:
				return name, format
			}
		}
	}
	return name, format
}

func parseExtPayload(buf []byte) []byte {
	// SnapshotExtensionPayload { bytes payload = 1 }
	if len(buf) == 0 || buf[0] != 0x0A {
		return nil
	}
	ln, m := binary.Uvarint(buf[1:])
	if m <= 0 || 1+m+int(ln) > len(buf) {
		return nil
	}
	return append([]byte(nil), buf[1+m:1+m+int(ln)]...)
}

func listChunkPaths(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type chunk struct {
		idx  int
		path string
	}
	var cs []chunk
	for _, e := range entries {
		n := e.Name()
		if !strings.HasPrefix(n, "chunk_") || !strings.HasSuffix(n, ".bin") {
			continue
		}
		idxStr := strings.TrimSuffix(strings.TrimPrefix(n, "chunk_"), ".bin")
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			continue
		}
		cs = append(cs, chunk{idx, filepath.Join(dir, n)})
	}
	sort.Slice(cs, func(i, j int) bool { return cs[i].idx < cs[j].idx })
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.path
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no chunk files in %s", dir)
	}
	return out, nil
}

// HumanBytes formats a byte count.
func HumanBytes(n uint64) string {
	const (
		k = 1024
		m = k * 1024
		g = m * 1024
	)
	switch {
	case n >= g:
		return fmt.Sprintf("%.2f GB", float64(n)/float64(g))
	case n >= m:
		return fmt.Sprintf("%.1f MB", float64(n)/float64(m))
	case n >= k:
		return fmt.Sprintf("%.1f KB", float64(n)/float64(k))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// shortHex truncates a hex string for log display.
func shortHex(h string) string {
	if len(h) > 16 {
		return h[:16] + "…"
	}
	return h
}

var _ = hex.EncodeToString // keep import in case we expand later
