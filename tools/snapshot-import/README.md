# snapshot-import

Standalone tool that converts a downloaded cosmos snapshot directory into
a gaiad-compatible `application.db` pebble dir. **Zero cosmos / cometbft /
iavl dependencies** — pure stdlib + pebble.

```
go.sum (cosmos/comet/iavl): empty
```

## What it does

Reads chunk_NNNNN.bin files (zlib-compressed slices of one continuous
SnapshotItem proto stream), decodes the IAVL `ExportNode`s in
depth-first post-order LRN, and emits the resulting:

- IAVL nodes (`s/k:<store>/s` + 12-byte nodeKey → encoded node bytes)
- Fast-storage entries (`s/k:<store>/f<userKey>` → encoded fastnode bytes)
- Per-store metadata (`s/k:<store>/mstorage_version` → `1.1.0-<height>`)
- Rootmulti commit-info (`s/<height>` → CommitInfo proto)
- Latest-version pointer (`s/latest` → varint(height))

into a fresh pebble database. The result can be opened by an unmodified
gaiad after a `cometbft BootstrapState` to populate `state.db` +
`blockstore.db`.

## Why standalone

The parent repo's `internal/snapshotappdb` uses cosmos/iavl's wave-parallel
`Importer` — ~600 lines of atomic-events bookkeeping designed for big
multi-core hosts. This tool is the minimal version of the same import:

- A single-goroutine stack walk holding fixed-size frames, no `*iavl.Node`
  allocations.
- Per-node hash + encode is inline, no worker pool or channels.
- Memory is **O(tree depth)** — ~1.7 KB of stack frames + pebble's
  configured memtable/cache.

For the cosmoshub-4 bank tree (~50M nodes), this trades a small amount of
hash parallelism for ~10× memory reduction and ~80% less import code.

## Usage

```sh
snapshot-import \
    -snapshot=/path/to/30960000_3 \
    -out=/path/to/application.db \
    -height=30960000 \
    [-memtable-mb=64]
```

The `-snapshot` directory should contain `chunk_00000.bin`,
`chunk_00001.bin`, … in sequence. The chunks are concatenated and
zlib-decompressed as a single stream; missing or out-of-order chunks
will fail the import (run `cosmos-snapshot-fetch` upstream to assemble a
complete chunk dir).

## Design notes

| File | Purpose |
|---|---|
| `main.go` | CLI driver |
| `chunkreader.go` | Multi-chunk zlib reader |
| `snapproto.go` | Hand-rolled SnapshotItem proto decoder |
| `iavlenc.go` | IAVL hash + writeBytes equivalents |
| `commitinfo.go` | Rootmulti CommitInfo + latest-version encoders |
| `importer.go` | Stack-based per-store importer + driver |

## What it intentionally doesn't do

- **Network**: assumes you've already downloaded chunks (e.g. via the
  parent repo's `cosmos-snapshot-fetch`).
- **Tamper validation per node**: relies on the final `cometbft
  BootstrapState` app-hash check. If a peer tampered with chunks during
  download, the resulting application.db will fail bootstrap; we don't
  catch it mid-stream.
- **Wasm payload extraction**: SnapshotExtension items are skipped. A
  fuller tool would dump them to per-store directories.
- **Cometbft state.db / blockstore.db**: out of scope. Use `cometbft
  bootstrap-state` after this tool exits.

## Verification

The resulting `application.db` is byte-format-compatible with what
`internal/snapshotappdb.Import` produces, so the same downstream
`cosmos-bootstrap-gaia` flow applies.
