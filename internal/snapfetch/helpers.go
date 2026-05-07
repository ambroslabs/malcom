package snapfetch

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"

	"github.com/zrbecker/cosmos-p2p/internal/helpers/addrbook"
	"github.com/zrbecker/cosmos-p2p/internal/logctx"
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

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func humanBytes(n uint64) string {
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
	default:
		return fmt.Sprintf("%d B", n)
	}
}
