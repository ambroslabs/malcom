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

## Typical pipeline

```
malcom init
malcom snapshot fetch  --chain cosmoshub-4 --out /data/snapshots
malcom snapshot import --chain cosmoshub-4 \
    --snapshot /data/snapshots/snapshot_cosmoshub-4_<H> \
    --out      /data/appdb --height <H>
malcom bootstrap       --chain cosmoshub-4 \
    --appdb /data/appdb/appdb_cosmoshub-4_<H> \
    --out   /data/chain  --height <H>
malcom verify          --chain cosmoshub-4 \
    --appdb /data/appdb/appdb_cosmoshub-4_<H> --height <H>
```

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
