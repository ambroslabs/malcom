# malcom

A single-binary CLI for bootstrapping a cosmos-sdk node from a
state-sync snapshot. Talks the cometbft state-sync P2P protocol
directly to fetch a snapshot, decodes it into `application.db` +
extensions without going through the chain daemon, then assembles a
runnable home directory.

## Subcommands

```
malcom init               seed XDG dirs + shared config.toml
malcom add <chain-id>     register a chain (config + node key + per-chain dirs)
malcom registry refresh   re-fetch the cached chain-registry snapshot
malcom clean              back up (or --clobber) the XDG malcom dirs
malcom snapshot fetch     download a state-sync snapshot
malcom snapshot import    snapshot dir → application.db + extensions/
malcom snapshot serve     advertise a local snapshot over state-sync P2P
malcom bootstrap          assemble a runnable chain home directory
malcom bootstrap heal     recover a chain home bricked by a failed first start
malcom verify             check the imported AppHash against a cometbft RPC
malcom compact            full-keyspace pebble compaction
```

Run `malcom <command> -h` for per-command flags.

## Build

```
go build -o build/malcom ./cmd/malcom
```

## Config

malcom follows the XDG Base Directory Specification. There is no
`--config <path>` flag — to isolate a tree, override
`XDG_CONFIG_HOME` (and friends).

```
$XDG_CONFIG_HOME/malcom/config.toml          shared defaults
$XDG_CONFIG_HOME/malcom/chains/<id>.toml     per-chain identity (rpcs, peers, genesis URL)
$XDG_STATE_HOME/malcom/<id>/node_key.json    cometbft p2p ed25519 identity
$XDG_CACHE_HOME/malcom/<id>/                 peer DB, addrbook
$XDG_DATA_HOME/malcom/<id>/                  genesis cache
```

`malcom init` seeds these. `malcom add <chain-id>` registers an
additional chain.

## Bootstrapping a cosmoshub-4 node

End-to-end run-through from "freshly built binary" to "`gaiad` syncing
from the imported state." Substitute your chain id, RPC list, and
disk paths as needed. `<H>` is whatever height the fetcher picked.

### 1. Seed XDG dirs

```
malcom init
```

Writes `~/.config/malcom/config.toml` and the surrounding directory
layout described above. Idempotent.

### 2. Register the chain

```
malcom add cosmoshub-4
```

Pulls cosmoshub-4 from the cached chain-registry, writes
`~/.config/malcom/chains/cosmoshub-4.toml` with default RPCs and
bootstrap peers, and generates a node key under `XDG_STATE_HOME`. If
the chain-registry cache is stale, `malcom registry refresh` first.

### 3. Fetch + import + verify (one command)

The streamlined happy path. `--import` pipelines the import phase so
chunks land in `application.db` as they're downloaded; the default
post-import AppHash check against the chain's configured RPCs is on
unless you pass `--no-verify`.

```
malcom snapshot fetch --chain cosmoshub-4 \
    --out /data/snapshots --import --import-out /data/appdb
```

Output: `/data/snapshots/snapshot_cosmoshub-4_<H>/` and
`/data/appdb/appdb_cosmoshub-4_<H>/{application.db, extensions/, meta.json}`.
A successful run means every per-chunk hash matched the offer, the
imported state's AppHash matched the consensus AppHash at `<H>+1`,
and the snapshot is safe to use.

**If you'd rather run the phases separately** — useful when you want
to inspect intermediate state, or when the import host doesn't have
network access to the chain RPCs:

```
malcom snapshot fetch  --chain cosmoshub-4 --out /data/snapshots
malcom snapshot import --chain cosmoshub-4 \
    --snapshot /data/snapshots/snapshot_cosmoshub-4_<H> \
    --out      /data/appdb \
    --height   <H>
malcom verify --chain cosmoshub-4 \
    --appdb /data/appdb/appdb_cosmoshub-4_<H> \
    --height <H>
```

**Always run `verify` before bootstrap.** Per-chunk hash verification
only proves the bytes on disk match what the peer advertised; it does
*not* prove the snapshot reflects real chain state. `verify` is the
trust anchor that ties the imported state to a block header
consensus already agreed on. See [THREAT-MODEL.md](THREAT-MODEL.md).

### 4. Assemble the chain home

Bootstrap shells out to the chain's own daemon for `init` and the
offline-state-sync setup, so you need the binary installed first.
For cosmoshub-4 that's `gaiad`. If it's on `$PATH`, malcom finds it
automatically via the chain-registry's `daemon_name` lookup;
otherwise pass `--binary /path/to/gaiad` explicitly.

```
malcom bootstrap --chain cosmoshub-4 \
    --appdb /data/appdb/appdb_cosmoshub-4_<H> \
    --out   /data/chain \
    --height <H>
```

(Add `--binary /path/to/gaiad` if it isn't on `$PATH`.)

Runs `gaiad init` under the hood, places `application.db` at the
right path inside the chain home, overlays genesis, forwards fetch's
served peers as `persistent_peers`, and writes a recovery marker so
`malcom bootstrap heal` can recover if the first `gaiad start`
fails before cometbft consumes its offline-state-sync signal.

Output: `/data/chain/home_cosmoshub-4_<H>/`.

### 5. Start the daemon

```
gaiad --home /data/chain/home_cosmoshub-4_<H> start
```

cometbft's offline-state-sync path takes over: `state.db` is
populated from the trust-height block header (fetched at bootstrap
time), and the daemon picks up consensus from the snapshot's height
forward to the chain tip.

## Exit codes

Subcommands return exit codes an orchestrator (shell scripts,
wrapping CLIs) can branch on. Shared across every command:

| Code | Meaning |
| ---- | ------- |
| 0 | Success |
| 1 | Generic failure |
| 2 | Config / flag / setup error |
| 130 | Interrupted (SIGINT/SIGTERM) |

### `malcom snapshot fetch` (and `fetch --import`)

| Code | Constant | Meaning |
| ---- | -------- | ------- |
| 3 | `ExitNoPeers` | no bootstrap peers configured and addrbook empty |
| 4 | `ExitWalkFailed` | height-discovery walk exhausted without a usable offer |
| 5 | `ExitDownloadFailed` | chunk download gave up (per-peer + global retry budgets exhausted) |
| 6 | `ExitDiskFailed` | local disk write/read error (out of space, EROFS, permission) |
| 7 | `ExitVerifyFailed` | post-import AppHash check disagreed with the chain RPC (only with `--import`, default unless `--no-verify`) |

### `malcom snapshot serve`

| Code | Constant | Meaning |
| ---- | -------- | ------- |
| 3 | `ExitNoSnapshots` | catalog empty at startup AND not in dir-watch mode |
| 4 | `ExitVerifyFail` | startup snapshot verification failed (any mode beyond `metadata-only`) |

### `malcom snapshot import` (standalone)

| Code | Constant | Meaning |
| ---- | -------- | ------- |
| 7 | `ExitVerifyFailed` | post-import AppHash check disagreed with the chain RPC |

Other subcommands (`init`, `add`, `bootstrap`, `verify`, `compact`,
`clean`, `registry refresh`) currently use the shared 0/1/2/130 set
above — they don't yet differentiate failure categories with named
exit codes. Treat their exit codes as "0 = ok, non-zero = look at the
logs."

## Running `snapshot serve` under systemd

A template unit file is provided at
[`dist/systemd/malcom-snapshot-serve@.service`](dist/systemd/) for
operators who want serve running as a long-lived daemon. One unit, one
instance per chain (`malcom-snapshot-serve@cosmoshub-4`). See
[`dist/systemd/README.md`](dist/systemd/README.md) for install and
operation notes.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or
  http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
