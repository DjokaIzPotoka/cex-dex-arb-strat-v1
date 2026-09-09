#!/usr/bin/env bash
#
# Stops the discovery dashboard (SIGTERM, waits, SIGKILL if needed) and
# tears down the 'zora-discovery-dash' tmux session. Mirrors
# stop_discovery.sh for the scheduler's own session -- separate script,
# session, and PID file, so stopping the dashboard never touches the
# scheduler (or vice versa). Called both by `make discovery-dashboard-stop`
# and directly from inside tmux.
set -uo pipefail

SESSION="zora-discovery-dash"
DASH_PID_FILE="/tmp/zora_discovery_dash.pid"
MAX_WAIT_SECONDS=3

if [ -f "$DASH_PID_FILE" ]; then
    pid="$(cat "$DASH_PID_FILE" 2>/dev/null || true)"
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
    echo "No running discovery dashboard found."
fi

rm -f "$DASH_PID_FILE"
tmux kill-session -t "$SESSION" 2>/dev/null || true

echo "ZORA discovery dashboard shutdown complete"
