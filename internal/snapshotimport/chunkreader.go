// Reads a directory of cosmos snapshot chunks (chunk_NNNNN.bin) as a single
// zlib-decompressed io.Reader.
//
// Wire convention: each chunk_NNNNN.bin is a fixed-size slice (10 MiB except
// the last) of one continuous zlib-compressed byte stream. Concatenated in
// numeric order, the bytes form a valid zlib stream whose decompressed
// payload is the SnapshotItem proto stream.

package snapshotimport

import (
	"compress/zlib"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// chunkReader holds open file handles + a zlib reader stacked on a
// MultiReader over them. Close releases all resources.
type chunkReader struct {
	files []*os.File
	zr    io.ReadCloser
}

// openChunkDir opens every chunk_NNNNN.bin in dir (sorted by NNNNN) and
// returns a chunkReader. The returned reader yields the decompressed
// SnapshotItem stream.
func openChunkDir(dir string) (*chunkReader, error) {
	paths, err := listChunks(dir)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no chunk_*.bin files in %s", dir)
	}

	files := make([]*os.File, 0, len(paths))
	readers := make([]io.Reader, 0, len(paths))
	for _, p := range paths {
		f, err := os.Open(p)
		if err != nil {
			for _, x := range files {
				x.Close()
			}
			return nil, fmt.Errorf("open %s: %w", p, err)
		}
		files = append(files, f)
		readers = append(readers, f)
	}

	zr, err := zlib.NewReader(io.MultiReader(readers...))
	if err != nil {
		for _, x := range files {
			x.Close()
		}
		return nil, fmt.Errorf("zlib: %w", err)
	}

	return &chunkReader{files: files, zr: zr}, nil
}

func (c *chunkReader) Read(p []byte) (int, error) {
	return c.zr.Read(p)
}

func (c *chunkReader) Close() error {
	if c.zr != nil {
		c.zr.Close()
	}
	for _, f := range c.files {
		f.Close()
	}
	return nil
}

func listChunks(dir string) ([]string, error) {
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
	return out, nil
}
