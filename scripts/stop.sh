#!/usr/bin/env bash
#
<<<<<<< Updated upstream
# Stops both ingestors cleanly via SIGTERM (never SIGKILL, unless one
# refuses to exit -- see WAIT_SECONDS below).
=======
# Stops all three ZORA processes (SIGTERM first, so CEX/DEX clean up
# their own shared memory segments) and tears down the 'zora' tmux
# session. Called both by `make stop` (from outside tmux) and by the
# status pane's own Ctrl+C handler (from inside tmux) -- same logic
# either way.
>>>>>>> Stashed changes
set -uo pipefail

CEX_PID_FILE="/tmp/zora_cex.pid"
DEX_PID_FILE="/tmp/zora_dex.pid"
<<<<<<< Updated upstream
SUPERVISOR_PID_FILE="/tmp/zora_supervisor.pid"

WAIT_SECONDS=10

wait_for_exit() {
    local pid="$1" waited=0
    while kill -0 "$pid" 2>/dev/null; do
=======
SPR_PID_FILE="/tmp/zora_spr.pid"
MAX_WAIT_SECONDS=3

pids=()
for f in "$CEX_PID_FILE" "$DEX_PID_FILE" "$SPR_PID_FILE"; do
    if [ -f "$f" ]; then
        pid="$(cat "$f" 2>/dev/null || true)"
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            pids+=("$pid")
        fi
    fi
done

if [ "${#pids[@]}" -eq 0 ]; then
    echo "No running ZORA processes found."
else
    for pid in "${pids[@]}"; do
        echo "Sending SIGTERM to PID $pid..."
        kill -TERM "$pid" 2>/dev/null
    done

    waited=0
    while [ "$waited" -lt $((MAX_WAIT_SECONDS * 2)) ]; do
        still_alive=0
        for pid in "${pids[@]}"; do
            kill -0 "$pid" 2>/dev/null && still_alive=1
        done
        [ "$still_alive" -eq 0 ] && break
>>>>>>> Stashed changes
        sleep 0.5
        waited=$((waited + 1))
        if [ "$waited" -ge $((WAIT_SECONDS * 2)) ]; then
            return 1
        fi
    done
    return 0
}

<<<<<<< Updated upstream
stop_pid_file() {
    local name="$1" pid_file="$2"
    if [ ! -f "$pid_file" ]; then
        return
    fi
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
        rm -f "$pid_file"
        return
    fi
    echo "[stop.sh] Sending SIGTERM to $name (PID $pid)..."
    kill -TERM "$pid" 2>/dev/null
    if wait_for_exit "$pid"; then
        echo "[stop.sh] $name stopped cleanly."
    else
        echo "[stop.sh] $name did not stop after ${WAIT_SECONDS}s, sending SIGKILL."
        kill -KILL "$pid" 2>/dev/null
    fi
    rm -f "$pid_file"
}
=======
rm -f "$CEX_PID_FILE" "$DEX_PID_FILE" "$SPR_PID_FILE"
tmux kill-session -t "$SESSION" 2>/dev/null || true
>>>>>>> Stashed changes

# Preferred path: stop start.sh's own supervisor loop, which cleanly
# stops both ingestors itself via its SIGTERM trap. Signalling an
# ingestor directly while the supervisor is still watching it would
# instead look like a crash to the supervisor, which would restart it
# right back up -- fighting this script instead of cooperating with it.
stop_pid_file "supervisor" "$SUPERVISOR_PID_FILE"

# Fallback: covers a supervisor that already exited (or was never
# started -- e.g. an ingestor launched by hand, not via start.sh) while
# an ingestor is still running, and the rarer case of the supervisor
# itself being SIGKILLed, which orphans its children without letting
# its own cleanup trap run.
stop_pid_file "CEX ingestor" "$CEX_PID_FILE"
stop_pid_file "DEX ingestor" "$DEX_PID_FILE"

echo "[stop.sh] Done."
