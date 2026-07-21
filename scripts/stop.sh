#!/usr/bin/env bash
#
# Stops all three ZORA processes (SIGTERM first, so CEX/DEX clean up
# their own shared memory segments) and tears down the 'zora' tmux
# session. Called both by `make stop` (from outside tmux) and directly
# from inside tmux (e.g. Ctrl+C on the main pane just stops spread-
# detector itself -- this script is what you'd run for a full stop).
set -uo pipefail

SESSION="zora"
CEX_PID_FILE="/tmp/zora_cex.pid"
DEX_PID_FILE="/tmp/zora_dex.pid"
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
        sleep 0.5
        waited=$((waited + 1))
    done

    for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "PID $pid did not stop within ${MAX_WAIT_SECONDS}s, sending SIGKILL."
            kill -KILL "$pid" 2>/dev/null
        fi
    done
fi

rm -f "$CEX_PID_FILE" "$DEX_PID_FILE" "$SPR_PID_FILE" /tmp/zora_start_ts
tmux kill-session -t "$SESSION" 2>/dev/null || true

echo "ZORA shutdown complete"
