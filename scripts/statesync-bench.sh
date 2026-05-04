#!/usr/bin/env bash
# statesync-bench.sh — drive a cometbft state-sync end to end and record
# how long each phase takes. Phases tracked:
#
#   1. State sync located         — gaiad picked a snapshot from peers
#   2. State sync downloaded      — all chunks delivered + applied to app
#   3. Application DB constructed — IAVL nodes written and committed
#   4. IAVL upgrade fast lookup   — gaiad's first-start fast-storage build
#   5. Gaiad bootstrapped         — node service up, RPC listening
#   6. Block sync started         — first block executed past snapshot height
#   7. Node caught up             — catching_up flips to false
#
# Output:
#   <home>/bench/timing.csv  — phase, wall_seconds, height, log_line_excerpt
#   <home>/bench/gaiad.log   — full gaiad stdout/stderr
#
# Usage:
#   scripts/statesync-bench.sh \
#       --home /root/statesync-test \
#       --gaiad /root/gaiad \
#       --chain-id cosmoshub-4 \
#       --genesis /root/genesis.cosmoshub-4.json \
#       --rpcs https://cosmos-rpc.polkachu.com,https://cosmos-rpc.publicnode.com \
#       --addrbook https://snapshots.polkachu.com/addrbook/cosmos/addrbook.json \
#       [--trust-offset 1000]   # use chain tip - N for trust height (default 1000)
#       [--rpc-port 26657]       # local RPC port to poll (default 26657)
#
# Requires: jq, curl, sed, awk, bash 4+

set -euo pipefail

# ─── arg parsing ──────────────────────────────────────────────────────

HOME_DIR=""
GAIAD=""
CHAIN_ID=""
GENESIS=""
RPCS=""
ADDRBOOK=""
TRUST_OFFSET=1000
RPC_PORT=26657

while [[ $# -gt 0 ]]; do
    case "$1" in
        --home)         HOME_DIR="$2"; shift 2 ;;
        --gaiad)        GAIAD="$2"; shift 2 ;;
        --chain-id)     CHAIN_ID="$2"; shift 2 ;;
        --genesis)      GENESIS="$2"; shift 2 ;;
        --rpcs)         RPCS="$2"; shift 2 ;;
        --addrbook)     ADDRBOOK="$2"; shift 2 ;;
        --trust-offset) TRUST_OFFSET="$2"; shift 2 ;;
        --rpc-port)     RPC_PORT="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *) echo "unknown flag: $1" >&2; exit 2 ;;
    esac
done

for v in HOME_DIR GAIAD CHAIN_ID GENESIS RPCS; do
    if [[ -z "${!v}" ]]; then echo "missing --${v,,}" >&2; exit 2; fi
done

if [[ ! -x "$GAIAD" ]]; then echo "gaiad binary not executable: $GAIAD" >&2; exit 2; fi
if [[ ! -f "$GENESIS" ]]; then echo "genesis not found: $GENESIS" >&2; exit 2; fi

PRIMARY_RPC="${RPCS%%,*}"
BENCH_DIR="$HOME_DIR/bench"
LOG="$BENCH_DIR/gaiad.log"
CSV="$BENCH_DIR/timing.csv"

# ─── pre-flight ───────────────────────────────────────────────────────

mkdir -p "$BENCH_DIR"
> "$LOG"
echo "phase,wall_seconds,height,note" > "$CSV"

# 1. Init gaiad home if not already (keeps existing data dir if present —
#    state-sync will bail anyway if data/state.db has a height).
if [[ ! -d "$HOME_DIR/config" ]]; then
    echo "[bench] init gaiad home at $HOME_DIR"
    "$GAIAD" init bench --chain-id="$CHAIN_ID" --home="$HOME_DIR" >/dev/null 2>&1
fi

# Wipe any prior data — we want a clean state-sync run each invocation.
echo "[bench] wiping data/ for fresh state-sync"
rm -rf "$HOME_DIR/data"
mkdir -p "$HOME_DIR/data"
echo '{"height":"0","round":0,"step":0}' > "$HOME_DIR/data/priv_validator_state.json"

# 2. Drop genesis
cp "$GENESIS" "$HOME_DIR/config/genesis.json"

# 3. Pick a recent trust height + hash from the primary RPC
echo "[bench] fetching trust hash from $PRIMARY_RPC ..."
TIP=$(curl -fsS "$PRIMARY_RPC/status" | jq -r '.result.sync_info.latest_block_height')
TRUST_HEIGHT=$((TIP - TRUST_OFFSET))
TRUST_HASH=$(curl -fsS "$PRIMARY_RPC/block?height=$TRUST_HEIGHT" | jq -r '.result.block_id.hash')
echo "[bench] trust_height=$TRUST_HEIGHT trust_hash=$TRUST_HASH"

# 4. Configure config.toml — enable state-sync, point at RPCs, set trust.
CFG="$HOME_DIR/config/config.toml"
# enable in [statesync]
awk -v rpcs="$RPCS" -v th="$TRUST_HEIGHT" -v hh="$TRUST_HASH" '
    /^\[statesync\]/   { in_ss=1 }
    in_ss && /^enable[[:space:]]*=/        { print "enable = true"; next }
    in_ss && /^rpc_servers[[:space:]]*=/   { printf "rpc_servers = \"%s\"\n", rpcs; next }
    in_ss && /^trust_height[[:space:]]*=/  { printf "trust_height = %s\n", th; next }
    in_ss && /^trust_hash[[:space:]]*=/    { printf "trust_hash = \"%s\"\n", hh; next }
    in_ss && /^trust_period[[:space:]]*=/  { print "trust_period = \"168h0m0s\""; next }
    /^\[/ && !/^\[statesync\]/             { in_ss=0 }
    { print }
' "$CFG" > "$CFG.tmp" && mv "$CFG.tmp" "$CFG"

# 5. Pull addrbook so we have peer candidates from the start
if [[ -n "$ADDRBOOK" ]]; then
    echo "[bench] fetching addrbook $ADDRBOOK ..."
    curl -fsSLo "$HOME_DIR/config/addrbook.json" "$ADDRBOOK"
fi

# ─── timing helpers ───────────────────────────────────────────────────

START_TS=$(date +%s)
elapsed() { echo $(($(date +%s) - START_TS)); }

declare -A FIRED
phase() {
    local name="$1"
    local height="${2:-}"
    local note="${3:-}"
    if [[ -n "${FIRED[$name]:-}" ]]; then return; fi
    FIRED[$name]=1
    local t
    t=$(elapsed)
    printf "[%4ss] %-30s height=%s %s\n" "$t" "$name" "${height:--}" "$note"
    printf "%s,%s,%s,%s\n" "$name" "$t" "$height" "${note//,/;}" >> "$CSV"
}

# ─── launch gaiad ─────────────────────────────────────────────────────

echo
echo "[bench] launching gaiad — log=$LOG"
nohup "$GAIAD" start --home="$HOME_DIR" </dev/null >"$LOG" 2>&1 &
GAIAD_PID=$!
echo "[bench] gaiad pid=$GAIAD_PID"

# Cleanup on script exit / interrupt — stop the log watcher, leave gaiad
# running so the user can poke at it.
WATCHER_PID=""
cleanup() {
    [[ -n "$WATCHER_PID" ]] && kill "$WATCHER_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ─── phase detection from gaiad log ───────────────────────────────────
# These regexes match cometbft v0.38 / cosmos-sdk v0.50+ log lines. If
# gaiad changes wording, the CSV will just be missing entries — the
# RPC-driven "caught up" detector still terminates the script.

(
    # tail -F starts at end of file (we just truncated the log) so we
    # see all lines as they're written.
    tail -F -n 0 "$LOG" 2>/dev/null | while IFS= read -r line; do
        case "$line" in
            *"Discovered new snapshot"*)
                phase "State sync located" "" "$(echo "$line" | grep -oE 'height=[0-9]+' | head -1)"
                ;;
            *"VerifyAt"*|*"Snapshot accepted"*|*"snapshot offered"*|*"snapshot.format"*)
                phase "State sync located" "" "$(echo "$line" | grep -oE 'height=[0-9]+' | head -1)"
                ;;
            *"Applied snapshot chunk to ABCI app"*|*"Fetched"*"chunks"*|*"all chunks fetched"*)
                # Many of these fire per-chunk; mark "downloaded" the
                # first time we see one referencing the FINAL chunk.
                : # handled below via "Snapshot restored" / "Snapshot applied"
                ;;
            *"State sync completed"*|*"Snapshot restored"*|*"snapshot restored"*|*"applied snapshot"*)
                phase "State sync downloaded"
                phase "Application DB constructed"
                ;;
            *"upgradeToFastStorage"*|*"Upgrading IAVL storage"*|*"Upgrading store"*"fast"*)
                phase "IAVL upgrade fast lookup"
                ;;
            *"Started node"*|*"Starting Node service"*|*"Starting RPC HTTP server"*)
                phase "Gaiad bootstrapped"
                ;;
            *"executed block"*|*"finalized block"*|*"committed state"*)
                # Pull height out of the structured fields.
                h=$(echo "$line" | grep -oE 'height=[0-9]+' | head -1 | cut -d= -f2)
                phase "Block sync started" "$h"
                ;;
            *"FATAL"*|*"panic:"*)
                printf "[%4ss] !!! %s\n" "$(elapsed)" "$line"
                ;;
        esac
    done
) &
WATCHER_PID=$!

# ─── RPC poll until catching_up flips to false ────────────────────────

echo "[bench] polling http://127.0.0.1:$RPC_PORT/status every 15s until caught up ..."
while true; do
    sleep 15
    if ! kill -0 "$GAIAD_PID" 2>/dev/null; then
        echo "[bench] !!! gaiad exited unexpectedly"
        tail -n 30 "$LOG"
        exit 1
    fi
    s=$(curl -fsS "http://127.0.0.1:$RPC_PORT/status" 2>/dev/null || true)
    [[ -z "$s" ]] && continue
    height=$(echo "$s" | jq -r '.result.sync_info.latest_block_height // "0"')
    catching=$(echo "$s" | jq -r '.result.sync_info.catching_up // false')
    tip=$(echo "$s" | jq -r '.result.sync_info.latest_block_time // ""')
    printf "[%4ss] rpc height=%s catching=%s tip_time=%s\n" "$(elapsed)" "$height" "$catching" "$tip"
    if [[ "$catching" == "false" ]]; then
        phase "Node caught up" "$height"
        break
    fi
done

echo
echo "=== timing.csv ==="
cat "$CSV"
echo
echo "[bench] gaiad still running pid=$GAIAD_PID — kill with: kill $GAIAD_PID"
