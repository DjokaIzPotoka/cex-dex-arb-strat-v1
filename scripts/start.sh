#!/usr/bin/env bash
#
# Starts the CEX and DEX ingestors together, restarts either one if it
# crashes (up to $MAX_CRASHES times each), and stops both cleanly on
# SIGINT/SIGTERM. Run directly for a foreground/attached session, or
# via `make run` to launch detached in the background.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CEX_BIN="$ROOT/build/zore_core"
DEX_BIN="$ROOT/ingestor-dex-solana/target/release/ingestor-dex-solana"

CEX_PID_FILE="/tmp/zora_cex.pid"
DEX_PID_FILE="/tmp/zora_dex.pid"
# Not one of the two PID files asked for, but needed for stop.sh to work
# correctly: see the comment above stop_pid_file() in stop.sh for why
# signalling this script (rather than the ingestors directly) is what
# keeps `make stop` from racing the crash-restart loop below.
SUPERVISOR_PID_FILE="/tmp/zora_supervisor.pid"
LOG_FILE="/tmp/zora.log"

MAX_CRASHES=5
RESTART_DELAY=2

# Everything this script (and anything it starts) prints from here on
# goes to both the terminal, if attached, and $LOG_FILE -- so `make
# logs` works whether start.sh is run directly in a foreground
# terminal or launched detached via `make run`.
exec > >(tee -a "$LOG_FILE") 2>&1

echo "$$" >"$SUPERVISOR_PID_FILE"

log() { echo "[start.sh] $*"; }

build_cex_if_needed() {
    if [ -x "$CEX_BIN" ]; then
        return
    fi
    log "CEX binary not found, building (release)..."
    mkdir -p "$ROOT/build"
    (
        cd "$ROOT/build" || exit 1
        if command -v brew >/dev/null 2>&1; then
            cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH="$(brew --prefix)" ..
        else
            cmake -DCMAKE_BUILD_TYPE=Release ..
        fi
        cmake --build .
    )
}

build_dex_if_needed() {
    if [ -x "$DEX_BIN" ]; then
        return
    fi
    log "DEX binary not found, building (release)..."
    (cd "$ROOT/ingestor-dex-solana" && cargo build --release)
}

# Prefixes with [CEX]/[DEX] while preserving the child's real PID in
# $!: `cmd > >(prefixer) &` backgrounds `cmd` itself, so $! is cmd's
# PID -- the prefixing pipeline runs as a separate, unrelated job that
# doesn't affect it. Piping directly (`cmd | prefixer &`) would instead
# give us the prefixer's PID, which we can't use to monitor or signal
# the actual ingestor.
start_cex() {
    "$CEX_BIN" > >(while IFS= read -r line; do echo "[CEX] $line"; done) 2>&1 &
    CEX_PID=$!
    echo "$CEX_PID" >"$CEX_PID_FILE"
    log "CEX ingestor started (PID $CEX_PID)"
}

start_dex() {
    "$DEX_BIN" > >(while IFS= read -r line; do echo "[DEX] $line"; done) 2>&1 &
    DEX_PID=$!
    echo "$DEX_PID" >"$DEX_PID_FILE"
    log "DEX ingestor started (PID $DEX_PID)"
}

cleanup() {
    log "Caught shutdown signal, stopping both ingestors..."
    [ -n "${CEX_PID:-}" ] && kill -TERM "$CEX_PID" 2>/dev/null
    [ -n "${DEX_PID:-}" ] && kill -TERM "$DEX_PID" 2>/dev/null
    wait "${CEX_PID:-}" "${DEX_PID:-}" 2>/dev/null
    rm -f "$CEX_PID_FILE" "$DEX_PID_FILE" "$SUPERVISOR_PID_FILE"
    log "Both ingestors stopped. Shutdown complete."
    exit 0
}
# SIGTERM here is what `stop.sh` sends (to this script, not directly to
# the ingestors -- see stop.sh) so a normal `make stop` runs this exact
# clean path instead of tripping the crash-restart logic below.
trap cleanup SIGINT SIGTERM

build_cex_if_needed
build_dex_if_needed

start_cex
start_dex

cex_crashes=0
dex_crashes=0

while true; do
    if ! kill -0 "$CEX_PID" 2>/dev/null; then
        cex_crashes=$((cex_crashes + 1))
        log "CEX ingestor died (crash $cex_crashes/$MAX_CRASHES)"
        if [ "$cex_crashes" -ge "$MAX_CRASHES" ]; then
            log "CEX crashed $MAX_CRASHES times in a row -- stopping everything."
            kill -TERM "$DEX_PID" 2>/dev/null
            rm -f "$CEX_PID_FILE" "$DEX_PID_FILE" "$SUPERVISOR_PID_FILE"
            exit 1
        fi
        sleep "$RESTART_DELAY"
        start_cex
    fi

    if ! kill -0 "$DEX_PID" 2>/dev/null; then
        dex_crashes=$((dex_crashes + 1))
        log "DEX ingestor died (crash $dex_crashes/$MAX_CRASHES)"
        if [ "$dex_crashes" -ge "$MAX_CRASHES" ]; then
            log "DEX crashed $MAX_CRASHES times in a row -- stopping everything."
            kill -TERM "$CEX_PID" 2>/dev/null
            rm -f "$CEX_PID_FILE" "$DEX_PID_FILE" "$SUPERVISOR_PID_FILE"
            exit 1
        fi
        sleep "$RESTART_DELAY"
        start_dex
    fi

    sleep 1
done
