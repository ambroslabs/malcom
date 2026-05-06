# experimental/

Scratch / research / future-rework binaries that are **not** part of the
maintained `malcom` gaiad bootstrap flow. Kept for reference, ad-hoc
diagnostics, and as starting points for future tooling. They build, but
they are not the canonical entry points.

For the maintained snapshot → gaiad flow, use `malcom`:

```
malcom snapshot fetch     download a cosmoshub state-sync snapshot
malcom snapshot import    snapshot dir → application.db + extensions/
malcom bootstrap          assemble a runnable gaiad home directory
malcom verify             check imported AppHash against a cometbft RPC
malcom compact            full-keyspace pebble compaction
```

## What lives here

- **Legacy P2P / block-archive work** — earlier round of the project that
  spoke the cometbft P2P wire protocol directly to fetch, archive, and
  serve blocks: `cosmos-archive`, `cosmos-block`, `cosmos-blockcache`,
  `cosmos-crawl`, `cosmos-p2p`. These still build and run; they share
  `internal/{archive, archivesync, blockcache, blocksync, crawler,
  observer, peers, pex}`.

- **Snapshot research / diagnostics** —
  `cosmos-snapshot-{apply,diff,inspect}` for examining and reconstructing
  snapshots, `cosmos-statesync-{bench,probe}` for benchmarking and peer
  surveys.

- **All-in-one driver, slated for rework** — `cosmos-rapid-bootstrap`
  drives fetch + import + bootstrap + gaiad-start in one process and
  emits phase timing. The pipelined import overlap was implicit in the
  legacy `internal/snapshotappdb`; after that path was retired in favor
  of `internal/snapshotimport`, this binary is wired to the simple
  importer and is due for a design refresh.

Add new diagnostic / research binaries here so the `cmd/` surface stays
tightly scoped to `malcom`.
