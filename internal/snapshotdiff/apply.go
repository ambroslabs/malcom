package snapshotdiff

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/ambroslabs/malcom/internal/durable"
)

// ApplyStats summarises the result of an Apply run.
type ApplyStats struct {
	Stores            int
	Extensions        int
	ItemsEmitted      uint64
	PayloadsEmitted   uint64
	Chunks            int
	BytesUncompressed uint64
	BytesCompressed   uint64
}

// storeChanges holds per-store mutations parsed from one store-section of
// a diff. Memory footprint is bounded by the per-store change set, not
// the full diff body — typically a few tens of MB peak for the largest
// store.
type storeChanges struct {
	deletes map[string]struct{}
	updates map[string][]byte
	inserts map[string][]byte
}

func newStoreChanges() *storeChanges {
	return &storeChanges{
		deletes: map[string]struct{}{},
		updates: map[string][]byte{},
		inserts: map[string][]byte{},
	}
}

type extChangeSet struct {
	format  uint32
	removes map[[32]byte]struct{}
	adds    [][]byte
}

// outMeta mirrors the subset of meta.json fields produced by
// cosmos-snapshot-fetch that downstream tools (cosmos-snapshot-diff,
// cosmos-snapshot-inspect) rely on.
type outMeta struct {
	Height         uint64   `json:"height"`
	Format         uint32   `json:"format"`
	Chunks         int      `json:"chunks"`
	HashHex        string   `json:"hash_hex"`
	BytesTotal     uint64   `json:"bytes_total"`
	ChunkHashesHex []string `json:"chunk_hashes_hex"`
}

// Apply reads a diff from diffPath and applies it to the snapshot at
// baseDir, producing a new snapshot at outDir. The reconstructed snapshot
// is logically equivalent to the original target (same per-store
// (key, value) sets), but byte-level differences are expected: IAVL
// version/height fields are stripped in the diff, and chunk boundaries
// depend on chunk size and compression.
//
// Streaming: this walks base and diff side-by-side, holding only the
// current store's change set in memory at any time. Peak memory is
// bounded by the largest single-store change set, not the whole diff.
func Apply(baseDir, diffPath, outDir string) (ApplyStats, error) {
	var stats ApplyStats

	df, err := os.Open(diffPath)
	if err != nil {
		return stats, err
	}
	defer df.Close()

	hdr, err := ReadHeader(df)
	if err != nil {
		return stats, fmt.Errorf("read diff header: %w", err)
	}
	if h, err := readSnapHashFromMeta(filepath.Join(baseDir, "meta.json")); err == nil {
		if h != hdr.BaseHashHex {
			return stats, fmt.Errorf("base hash mismatch: meta=%s diff=%s", h, hdr.BaseHashHex)
		}
	}

	zr, err := zlib.NewReader(df)
	if err != nil {
		return stats, fmt.Errorf("open zlib body: %w", err)
	}
	defer zr.Close()

	dr := newDiffReader(zr)

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return stats, err
	}
	cw := newChunkWriter(outDir, 16<<20)
	zw, err := zlib.NewWriterLevel(cw, zlib.DefaultCompression)
	if err != nil {
		return stats, err
	}
	bw := bufio.NewWriterSize(zw, 1<<20)

	rawBytes := uint64(0)
	emit := func(item []byte) error {
		var lb [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(lb[:], uint64(len(item)))
		if _, err := bw.Write(lb[:n]); err != nil {
			return err
		}
		if _, err := bw.Write(item); err != nil {
			return err
		}
		rawBytes += uint64(n) + uint64(len(item))
		return nil
	}

	baseRdr, err := newStoreReader(baseDir)
	if err != nil {
		return stats, fmt.Errorf("open base: %w", err)
	}
	defer baseRdr.Close()

	// Walk both base and diff in alphabetical store order.
	for {
		baseStore, baseDone, err := baseRdr.peekStore()
		if err != nil {
			return stats, err
		}
		diffKind, diffName, _, diffEOF, err := dr.peekSection()
		if err != nil {
			return stats, err
		}
		// Stores section is over once we hit ExtEnter or EOF.
		diffStoresOver := diffEOF || diffKind == sectionExt

		if baseDone && diffStoresOver {
			break
		}

		switch {
		case baseDone || (!diffStoresOver && diffName < baseStore):
			// diff-only store (target-only): consume StoreEnter, read
			// records into a small change set, emit as all-INSERT.
			dr.consumeSection()
			ch, err := dr.readStoreChanges()
			if err != nil {
				return stats, err
			}
			if len(ch.deletes) > 0 || len(ch.updates) > 0 {
				return stats, fmt.Errorf("diff says target-only store %q has updates/deletes", diffName)
			}
			if err := emitTargetOnlyStore(diffName, ch.inserts, emit, &stats); err != nil {
				return stats, err
			}
		case diffStoresOver || diffName > baseStore:
			// base-only store (untouched by diff): pass through base items.
			if err := emitStoreFromBase(baseStore, nil, baseRdr, emit, &stats); err != nil {
				return stats, err
			}
		default:
			// Names match.
			dr.consumeSection()
			ch, err := dr.readStoreChanges()
			if err != nil {
				return stats, err
			}
			if err := emitStoreFromBase(baseStore, ch, baseRdr, emit, &stats); err != nil {
				return stats, err
			}
		}
	}

	// Extensions phase: read base extensions (small) plus any remaining
	// diff extension records.
	extData, err := baseRdr.readAllExtensions()
	if err != nil {
		return stats, err
	}
	perExt := map[string]*extChangeSet{}
	for {
		kind, name, format, eof, err := dr.peekSection()
		if err != nil {
			return stats, err
		}
		if eof || kind != sectionExt {
			break
		}
		dr.consumeSection()
		ch, err := dr.readExtChanges(format)
		if err != nil {
			return stats, err
		}
		perExt[name] = ch
	}

	extNames := map[string]bool{}
	for n := range extData {
		extNames[n] = true
	}
	for n := range perExt {
		extNames[n] = true
	}
	sortedExt := make([]string, 0, len(extNames))
	for n := range extNames {
		sortedExt = append(sortedExt, n)
	}
	sort.Strings(sortedExt)

	for _, name := range sortedExt {
		baseExt := extData[name]
		ch := perExt[name]
		var format uint32
		if baseExt != nil {
			format = baseExt.format
		}
		if ch != nil {
			format = ch.format
		}

		emittedMeta := false
		emitMetaOnce := func() error {
			if emittedMeta {
				return nil
			}
			emittedMeta = true
			return emit(buildOneOfMessage(3,
				encStringField(1, name),
				encVarintField(2, uint64(format))))
		}

		if baseExt != nil {
			for _, p := range baseExt.payloads {
				if ch != nil {
					h := sha256.Sum256(p)
					if _, dropped := ch.removes[h]; dropped {
						continue
					}
				}
				if err := emitMetaOnce(); err != nil {
					return stats, err
				}
				if err := emit(buildOneOfMessage(4, encBytesField(1, p))); err != nil {
					return stats, err
				}
				stats.PayloadsEmitted++
			}
		}
		if ch != nil {
			for _, p := range ch.adds {
				if err := emitMetaOnce(); err != nil {
					return stats, err
				}
				if err := emit(buildOneOfMessage(4, encBytesField(1, p))); err != nil {
					return stats, err
				}
				stats.PayloadsEmitted++
			}
		}
		if emittedMeta {
			stats.Extensions++
		}
	}

	if err := bw.Flush(); err != nil {
		return stats, err
	}
	if err := zw.Close(); err != nil {
		return stats, err
	}
	if err := cw.close(); err != nil {
		return stats, err
	}

	stats.Chunks = cw.chunkIdx
	stats.BytesUncompressed = rawBytes
	stats.BytesCompressed = cw.totalWritten

	meta := outMeta{
		Height:         hdr.TargetHeight,
		Format:         3,
		Chunks:         cw.chunkIdx,
		HashHex:        computeSnapshotHash(cw.chunkHashes),
		BytesTotal:     cw.totalWritten,
		ChunkHashesHex: hexHashes(cw.chunkHashes),
	}
	if err := writeMetaJSON(filepath.Join(outDir, "meta.json"), meta); err != nil {
		return stats, err
	}

	return stats, nil
}

// emitStoreFromBase emits the target version of a store by streaming base
// items and merging in changes. ch may be nil for an untouched store.
func emitStoreFromBase(name string, ch *storeChanges, baseRdr *storeReader, emit func([]byte) error, stats *ApplyStats) error {
	var insertKeys []string
	if ch != nil {
		insertKeys = make([]string, 0, len(ch.inserts))
		for k := range ch.inserts {
			insertKeys = append(insertKeys, k)
		}
		sort.Strings(insertKeys)
	}

	var baseK, baseV []byte
	baseHas := false

	nextBase := func() error {
		baseHas = false
		for !baseHas {
			done, err := baseRdr.readNextItemInStoreRaw(name, func(rec []byte) {
				if !isLeafIAVL(rec) {
					return
				}
				k := parseProtoBytes(rec, 1)
				v := parseProtoBytes(rec, 2)
				baseK = append(baseK[:0], k...)
				baseV = append(baseV[:0], v...)
				baseHas = true
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
	if err := nextBase(); err != nil {
		return err
	}

	if !baseHas && len(insertKeys) == 0 {
		return nil
	}

	if err := emit(buildOneOfMessage(1, encStringField(1, name))); err != nil {
		return err
	}
	stats.Stores++

	insertIdx := 0
	for baseHas || insertIdx < len(insertKeys) {
		if !baseHas {
			ik := insertKeys[insertIdx]
			if err := emit(buildOneOfMessage(2,
				encBytesField(1, []byte(ik)),
				encBytesField(2, ch.inserts[ik]))); err != nil {
				return err
			}
			stats.ItemsEmitted++
			insertIdx++
			continue
		}
		if insertIdx >= len(insertKeys) {
			if err := processBaseItem(name, ch, baseK, baseV, emit, stats); err != nil {
				return err
			}
			if err := nextBase(); err != nil {
				return err
			}
			continue
		}
		cmp := bytes.Compare(baseK, []byte(insertKeys[insertIdx]))
		switch {
		case cmp < 0:
			if err := processBaseItem(name, ch, baseK, baseV, emit, stats); err != nil {
				return err
			}
			if err := nextBase(); err != nil {
				return err
			}
		case cmp > 0:
			ik := insertKeys[insertIdx]
			if err := emit(buildOneOfMessage(2,
				encBytesField(1, []byte(ik)),
				encBytesField(2, ch.inserts[ik]))); err != nil {
				return err
			}
			stats.ItemsEmitted++
			insertIdx++
		default:
			// Base key matches an insert key. This happens when the
			// streaming differ emitted a delete+insert pair for the same
			// key (e.g., a record changed in version metadata, generating
			// "different" bytes that aren't aligned in the merge). The
			// base record is replaced by the insert. We process the base
			// (which will be skipped if also in deletes), then emit the
			// insert.
			if err := processBaseItem(name, ch, baseK, baseV, emit, stats); err != nil {
				return err
			}
			if err := nextBase(); err != nil {
				return err
			}
			ik := insertKeys[insertIdx]
			if err := emit(buildOneOfMessage(2,
				encBytesField(1, []byte(ik)),
				encBytesField(2, ch.inserts[ik]))); err != nil {
				return err
			}
			stats.ItemsEmitted++
			insertIdx++
		}
	}
	return nil
}

// processBaseItem applies any per-key delete/update from ch to a base item,
// then emits if not deleted.
func processBaseItem(name string, ch *storeChanges, k, v []byte, emit func([]byte) error, stats *ApplyStats) error {
	if ch != nil {
		if _, del := ch.deletes[string(k)]; del {
			return nil
		}
		if upd, ok := ch.updates[string(k)]; ok {
			if err := emit(buildOneOfMessage(2,
				encBytesField(1, k),
				encBytesField(2, upd))); err != nil {
				return err
			}
			stats.ItemsEmitted++
			return nil
		}
	}
	if err := emit(buildOneOfMessage(2,
		encBytesField(1, k),
		encBytesField(2, v))); err != nil {
		return err
	}
	stats.ItemsEmitted++
	return nil
}

func emitTargetOnlyStore(name string, inserts map[string][]byte, emit func([]byte) error, stats *ApplyStats) error {
	if len(inserts) == 0 {
		return nil
	}
	if err := emit(buildOneOfMessage(1, encStringField(1, name))); err != nil {
		return err
	}
	stats.Stores++

	keys := make([]string, 0, len(inserts))
	for k := range inserts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if err := emit(buildOneOfMessage(2,
			encBytesField(1, []byte(k)),
			encBytesField(2, inserts[k]))); err != nil {
			return err
		}
		stats.ItemsEmitted++
	}
	return nil
}

// ─── streaming diff reader ───────────────────────────────────────────────

const (
	sectionStore = 1
	sectionExt   = 2
)

// diffReader walks the decompressed diff body lazily. It exposes a
// sectional API: peek the next StoreEnter or ExtEnter, consume that
// section header, then drain its inner records into a per-section change
// set. Memory cost is bounded by one section's records, not the whole
// diff body.
type diffReader struct {
	br *bufio.Reader

	pending     bool
	pendingTag  uint8
	pendingBody []byte
	eof         bool
}

func newDiffReader(r io.Reader) *diffReader {
	return &diffReader{br: bufio.NewReaderSize(r, 1<<20)}
}

// peekRecord returns the next record's outer tag and body. Once a record
// is peeked, repeated peekRecord calls return the same record until
// consumeRecord is called.
func (d *diffReader) peekRecord() (tag uint8, body []byte, eof bool, err error) {
	if d.eof {
		return 0, nil, true, nil
	}
	if d.pending {
		return d.pendingTag, d.pendingBody, false, nil
	}
	recLen, err := binary.ReadUvarint(d.br)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		d.eof = true
		return 0, nil, true, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	if recLen == 0 {
		return d.peekRecord()
	}
	rec := make([]byte, recLen)
	if _, err := io.ReadFull(d.br, rec); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			d.eof = true
			return 0, nil, true, nil
		}
		return 0, nil, false, err
	}
	if len(rec) == 0 {
		return d.peekRecord()
	}
	outerTag := rec[0] >> 3
	innerLen, n := binary.Uvarint(rec[1:])
	if n <= 0 || int(innerLen) > len(rec)-1-n {
		return 0, nil, false, fmt.Errorf("malformed diff record: outerTag=%d", outerTag)
	}
	d.pendingBody = rec[1+n : 1+n+int(innerLen)]
	d.pendingTag = outerTag
	d.pending = true
	return outerTag, d.pendingBody, false, nil
}

func (d *diffReader) consumeRecord() {
	d.pending = false
}

// peekSection reports the next section header (StoreEnter or ExtEnter)
// without consuming it. Other record types between sections are skipped.
// Returns kind=sectionStore or kind=sectionExt, the name, and (for
// extensions) the format.
func (d *diffReader) peekSection() (kind int, name string, format uint32, eof bool, err error) {
	for {
		tag, body, end, err := d.peekRecord()
		if err != nil {
			return 0, "", 0, false, err
		}
		if end {
			return 0, "", 0, true, nil
		}
		switch int(tag) {
		case tagStoreEnter:
			return sectionStore, string(parseProtoBytes(body, 1)), 0, false, nil
		case tagExtEnter:
			return sectionExt,
				string(parseProtoBytes(body, 1)),
				uint32(parseProtoVarint(body, 2)),
				false, nil
		default:
			// Not a section start — skip stray records.
			d.consumeRecord()
		}
	}
}

func (d *diffReader) consumeSection() {
	d.consumeRecord()
}

// readStoreChanges drains records up to and including the next StoreLeave
// (or section-ending event) into a fresh storeChanges. Caller must have
// already consumed the StoreEnter that named the store.
func (d *diffReader) readStoreChanges() (*storeChanges, error) {
	ch := newStoreChanges()
	for {
		tag, body, end, err := d.peekRecord()
		if err != nil {
			return nil, err
		}
		if end {
			return ch, nil
		}
		switch int(tag) {
		case tagStoreLeave:
			d.consumeRecord()
			return ch, nil
		case tagInsert:
			d.consumeRecord()
			k := parseProtoBytes(body, 1)
			v := parseProtoBytes(body, 2)
			ch.inserts[string(k)] = append([]byte(nil), v...)
		case tagUpdate:
			d.consumeRecord()
			k := parseProtoBytes(body, 1)
			v := parseProtoBytes(body, 2)
			ch.updates[string(k)] = append([]byte(nil), v...)
		case tagDelete:
			d.consumeRecord()
			k := parseProtoBytes(body, 1)
			ch.deletes[string(k)] = struct{}{}
		case tagStoreEnter, tagExtEnter:
			// Next section reached without a StoreLeave — leave the new
			// header pending for the next peekSection call.
			return ch, nil
		default:
			// Unknown record inside a store section — skip.
			d.consumeRecord()
		}
	}
}

// readExtChanges drains records up to ExtLeave (or the next section)
// into a fresh extChangeSet. Caller must have consumed the ExtEnter.
func (d *diffReader) readExtChanges(format uint32) (*extChangeSet, error) {
	ch := &extChangeSet{
		format:  format,
		removes: map[[32]byte]struct{}{},
	}
	for {
		tag, body, end, err := d.peekRecord()
		if err != nil {
			return nil, err
		}
		if end {
			return ch, nil
		}
		switch int(tag) {
		case tagExtLeave:
			d.consumeRecord()
			return ch, nil
		case tagExtAdd:
			d.consumeRecord()
			p := parseProtoBytes(body, 1)
			ch.adds = append(ch.adds, append([]byte(nil), p...))
		case tagExtRemove:
			d.consumeRecord()
			p := parseProtoBytes(body, 1)
			ch.removes[sha256.Sum256(p)] = struct{}{}
		case tagStoreEnter, tagExtEnter:
			return ch, nil
		default:
			d.consumeRecord()
		}
	}
}

// parseProtoBytes returns the bytes for a wire-type-2 (length-delimited)
// field, or nil if not present.
func parseProtoBytes(buf []byte, field int) []byte {
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
		switch tag & 0x07 {
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

// parseProtoVarint returns the value of a wire-type-0 (varint) field, or 0
// if not present.
func parseProtoVarint(buf []byte, field int) uint64 {
	want := byte(field << 3)
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
		switch tag & 0x07 {
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

// chunkWriter writes a single byte stream to the directory as a sequence
// of fixed-size chunk_NNNNN.bin files, hashing each chunk individually
// (sha256). The boundary is byte-position-based on the post-compression
// stream — chunks are not aligned to SnapshotItem boundaries, which
// matches cosmos-sdk's format-3 convention.
type chunkWriter struct {
	dir          string
	chunkSize    int
	cur          *os.File
	hasher       hash.Hash
	curWritten   int
	chunkIdx     int
	chunkHashes  [][]byte
	totalWritten uint64
}

func newChunkWriter(dir string, chunkSize int) *chunkWriter {
	return &chunkWriter{dir: dir, chunkSize: chunkSize}
}

func (c *chunkWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		if c.cur == nil {
			if err := c.openNext(); err != nil {
				return written, err
			}
		}
		room := c.chunkSize - c.curWritten
		n := len(p)
		if n > room {
			n = room
		}
		if _, err := c.cur.Write(p[:n]); err != nil {
			return written, err
		}
		c.hasher.Write(p[:n])
		c.curWritten += n
		c.totalWritten += uint64(n)
		written += n
		p = p[n:]
		if c.curWritten >= c.chunkSize {
			if err := c.closeCurrent(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (c *chunkWriter) openNext() error {
	name := fmt.Sprintf("chunk_%05d.bin", c.chunkIdx)
	f, err := os.Create(filepath.Join(c.dir, name))
	if err != nil {
		return err
	}
	c.cur = f
	c.curWritten = 0
	c.hasher = sha256.New()
	return nil
}

func (c *chunkWriter) closeCurrent() error {
	if c.cur == nil {
		return nil
	}
	err := c.cur.Close()
	c.cur = nil
	if err != nil {
		return err
	}
	c.chunkHashes = append(c.chunkHashes, c.hasher.Sum(nil))
	c.chunkIdx++
	return nil
}

func (c *chunkWriter) close() error {
	return c.closeCurrent()
}

func computeSnapshotHash(chunkHashes [][]byte) string {
	h := sha256.New()
	for _, ch := range chunkHashes {
		h.Write(ch)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func hexHashes(in [][]byte) []string {
	out := make([]string, len(in))
	for i, h := range in {
		out[i] = hex.EncodeToString(h)
	}
	return out
}

func readSnapHashFromMeta(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	var m struct {
		HashHex string `json:"hash_hex"`
	}
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return "", err
	}
	return m.HashHex, nil
}

func writeMetaJSON(path string, m outMeta) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return durable.WriteFile(path, b, 0o644)
}
