#!/usr/bin/env bash
#
# Stops the discovery scheduler (SIGTERM, waits, SIGKILL if needed) and
# tears down the 'zora-discovery' tmux session. Mirrors scripts/stop.sh
# for ZORA's paper-trading session -- separate script, session, and PID
# file, so stopping one pipeline never touches the other. Called both
# by `make discovery-stop` and directly from inside tmux.
set -uo pipefail

SESSION="zora-discovery"
SCHED_PID_FILE="/tmp/zora_discovery_sched.pid"
MAX_WAIT_SECONDS=3

if [ -f "$SCHED_PID_FILE" ]; then
    pid="$(cat "$SCHED_PID_FILE" 2>/dev/null || true)"
else
    pid=""
fi

if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    echo "Sending SIGTERM to PID $pid..."
    kill -TERM "$pid" 2>/dev/null

    waited=0
    while [ "$waited" -lt $((MAX_WAIT_SECONDS * 2)) ]; do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.5
        waited=$((waited + 1))
    done

    if kill -0 "$pid" 2>/dev/null; then
        echo "PID $pid did not stop within ${MAX_WAIT_SECONDS}s, sending SIGKILL."
        kill -KILL "$pid" 2>/dev/null
    fi
else
    echo "No running discovery scheduler found."
fi

rm -f "$SCHED_PID_FILE"
tmux kill-session -t "$SESSION" 2>/dev/null || true

echo "ZORA discovery shutdown complete"
