# systemd unit for `malcom snapshot serve`

Run `malcom snapshot serve` as a long-running, supervised service.

The unit (`malcom-snapshot-serve@.service`) is a **template** — one
copy on disk, one instance per chain you want to serve.

## Install

Drop the unit in place, reload systemd, and enable an instance for
each chain:

```sh
# 1. binary on PATH
sudo install -m 0755 ./build/malcom /usr/local/bin/malcom

# 2. unit file
sudo install -m 0644 dist/systemd/malcom-snapshot-serve@.service \
  /etc/systemd/system/

# 3. malcom config (one-time). The unit sets XDG_CONFIG_HOME=/etc, so
#    init+add need the same env to write to the path the service will
#    read from — without it, sudo's malcom writes to /root/.config/
#    instead and the service can't find the chain.
sudo XDG_CONFIG_HOME=/etc malcom init               # writes /etc/malcom/config.toml
sudo XDG_CONFIG_HOME=/etc malcom add cosmoshub-4    # writes /etc/malcom/chains/cosmoshub-4.toml

# 4. enable + start
sudo systemctl daemon-reload
sudo systemctl enable --now malcom-snapshot-serve@cosmoshub-4
```

> **Why the explicit `XDG_CONFIG_HOME=/etc`?** systemd's
> `ConfigurationDirectory=malcom` creates `/etc/malcom` for the
> service, and the unit sets `XDG_CONFIG_HOME=/etc` so malcom reads
> from there. But `sudo` runs `malcom init` / `add` under root's
> environment, which has `XDG_CONFIG_HOME=$HOME/.config` (`/root/.config`
> typically) — different path. Setting the env var on the command line
> aligns the two.

`systemctl status malcom-snapshot-serve@cosmoshub-4` shows the
running instance; `journalctl -fu malcom-snapshot-serve@cosmoshub-4`
tails its logs (the unit pipes malcom's `-log text` to journal so
slog's key=value lines stay greppable).

To serve more chains, repeat the `enable --now` line with a different
suffix:

```sh
sudo systemctl enable --now malcom-snapshot-serve@osmosis-1
sudo systemctl enable --now malcom-snapshot-serve@babylon-1
```

Each instance is its own process with its own node key, addrbook, and
banlist under `/var/lib/malcom/<chain>/`.

## Snapshot pool

The unit's default points `-snapshots` at `/var/lib/malcom/snapshots-pool`.
Because `DynamicUser=yes` is in effect, that path is bind-mounted from
`/var/lib/private/malcom/snapshots-pool` on the host. To drop a
finished fetch into the pool:

```sh
sudo mkdir -p /var/lib/private/malcom/snapshots-pool
sudo cp -r /path/to/snapshot_cosmoshub-4_<H> \
  /var/lib/private/malcom/snapshots-pool/
sudo chmod -R a+rX /var/lib/private/malcom/snapshots-pool/

# trigger an immediate rescan (otherwise it picks up within 30s anyway)
sudo systemctl reload malcom-snapshot-serve@cosmoshub-4
```

`systemctl reload` is wired to `kill -HUP`, which the dir-watch loop
treats as "rescan now".

### Pointing at a different pool

For real deployments the snapshot pool is usually on a larger volume
(`/data/snapshots`, an NVMe mount, etc.). Override the path with
`systemctl edit` rather than editing the shipped unit — your override
survives package upgrades:

```sh
sudo systemctl edit malcom-snapshot-serve@cosmoshub-4
```

In the editor that opens, add:

```ini
[Service]
ExecStart=
ExecStart=/usr/local/bin/malcom snapshot serve \
  -chain %i \
  -snapshots /data/snapshots \
  -log text
ReadWritePaths=/data/snapshots
```

The two `ExecStart=` lines are intentional — the first (empty) clears
the inherited value so the second one becomes the only ExecStart.

Then:

```sh
sudo systemctl daemon-reload
sudo systemctl restart malcom-snapshot-serve@cosmoshub-4
```

`ProtectSystem=strict` makes the host filesystem read-only to the
service; non-default snapshot paths under `/data`, `/srv`, etc. work
for reads without further action, but write-access (which serve does
not need) would require an explicit `ReadWritePaths=` entry as shown.

## Reload vs restart

| Action                                              | Use                                             |
| --------------------------------------------------- | ----------------------------------------------- |
| New snapshot landed in the pool                     | `systemctl reload` (SIGHUP → catalog rescan)    |
| Snapshot removed from the pool                      | `systemctl reload` (or wait for periodic rescan) |
| Tweaked `/etc/malcom/chains/<id>.toml`              | `systemctl restart` (config is loaded at start) |
| Switched to a different `-snapshots` path           | `systemctl restart` (ExecStart changed)         |
| New malcom binary                                   | `systemctl restart`                             |

## Hardening notes

The unit enables strict systemd sandboxing: `ProtectSystem=strict`,
`ProtectHome=yes`, `DynamicUser=yes`, `PrivateTmp=yes`,
`MemoryDenyWriteExecute=yes`, and friends. If you hit a permission
wall (e.g. a pool on a path that's `ProtectHome`-blocked), relax the
specific directive via `systemctl edit` — don't disable the bundle.

Note that today's serve shutdown caps `sw.Stop` at 1 second, so the
`TimeoutStopSec=30s` budget isn't fully used yet. Once the graceful
in-flight chunk drain lands (issue #82), the headroom becomes real.
