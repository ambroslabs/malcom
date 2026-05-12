package snapfetch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"

	"github.com/ambroslabs/malcom/internal/helpers/addrbook"
	"github.com/ambroslabs/malcom/internal/logctx"
)

// parseChunkHashes decodes a cosmos-sdk format-3 snapshot Metadata blob:
//
//	repeated bytes chunk_hashes = 1;
//
// We hand-roll the decoder so we don't drag cosmos-sdk into this binary.
func parseChunkHashes(metadata []byte) ([][]byte, error) {
	var out [][]byte
	i := 0
	for i < len(metadata) {
		if metadata[i] != 0x0A {
			return nil, fmt.Errorf("unexpected tag 0x%02x at offset %d", metadata[i], i)
		}
		i++
		length, n := binary.Uvarint(metadata[i:])
		if n <= 0 {
			return nil, fmt.Errorf("bad varint at offset %d", i)
		}
		i += n
		if i+int(length) > len(metadata) {
			return nil, fmt.Errorf("hash extends past metadata (offset=%d len=%d total=%d)",
				i, length, len(metadata))
		}
		h := make([]byte, length)
		copy(h, metadata[i:i+int(length)])
		out = append(out, h)
		i += int(length)
	}
	return out, nil
}

// verifyChunkZero checks that data matches the offer's
// metadata.chunk_hashes[0]. Used during the walk to reject garbage
// chunk-0 responses before they cause an offer to be accepted.
//
// Returns an error describing the mismatch reason on failure. The
// caller is expected to ban the responder on any error.
func verifyChunkZero(offer *snapshotOffer, data []byte) error {
	chunkHashes, err := parseChunkHashes(offer.Metadata)
	if err != nil {
		return fmt.Errorf("parse chunk_hashes: %w", err)
	}
	if uint32(len(chunkHashes)) != offer.Chunks {
		return fmt.Errorf("chunk_hashes count %d, offer.Chunks %d",
			len(chunkHashes), offer.Chunks)
	}
	if len(chunkHashes) == 0 {
		return fmt.Errorf("offer has zero chunks")
	}
	h := sha256.Sum256(data)
	if !bytes.Equal(h[:], chunkHashes[0]) {
		return fmt.Errorf("chunk-0 hash mismatch")
	}
	return nil
}

func loadAddrbookPeers(ctx context.Context, addrBookPath string) []addrbook.PeerAddr {
	if addrBookPath == "" {
		return nil
	}
	items, err := addrbook.Load(addrBookPath)
	if err != nil {
		logctx.From(ctx).Error("load addrbook failed", "err", err)
		return nil
	}
	out := make([]addrbook.PeerAddr, 0, len(items))
	seen := map[string]bool{}
	for _, it := range items {
		s := it.String()
		if seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, addrbook.PeerAddr{Addr: s, Source: addrbook.SourceAddrbook})
	}
	return out
}

// writeFileAtomic writes data to path durably: tmp file, fsync the
// fd, close, rename to the final path. The caller is responsible for
// fsyncing the parent directory afterwards if it cares about the
// rename being durable across reboots.
func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// writeJSONAtomic JSON-encodes v with two-space indent and writes it
// to path via writeFileAtomic. Mirrors the previous writeJSON output
// (indented, trailing newline from json.Encoder).
func writeJSONAtomic(path string, v any) error {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return err
	}
	return writeFileAtomic(path, buf.Bytes(), 0o644)
}

// fsyncDir fsyncs a directory so that prior renames into it are
// durable. On platforms where directory fsync is unsupported the
// underlying open or sync may fail; callers treat that as best-effort
// and surface the error so it can be logged.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

