#!/usr/bin/env bash
#
# Launches the Phase 2b discovery dashboard (discovery/dashboard/server.py)
# in its own tmux session ('zora-discovery-dash'), separate from both
# 'zora' (paper trading) and 'zora-discovery' (the Tier 1 scheduler) --
# see discovery/dashboard/server.py's module docstring for why this is a
# fully separate process/lifecycle: if it crashes, the scheduler must
# never notice, and vice versa -- this only *reads* the scheduler's
# heartbeat file, cycle_telemetry table, and log file.
#
# Unlike zora_discovery.sh, this does NOT check that ClickHouse or the
# scheduler are up before starting: the dashboard is explicitly designed
# to start (and stay up) with either one down -- see server.py's
# /api/status and /api/history degraded-state handling. Requiring them
# here would contradict that.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSION="zora-discovery-dash"
DISCOVERY_VENV="$ROOT/discovery/.venv"
PYTHON_BIN="$DISCOVERY_VENV/bin/python3"
DASH_SCRIPT="$ROOT/discovery/dashboard/server.py"

DASH_PID_FILE="/tmp/zora_discovery_dash.pid"
DASH_LOG="$ROOT/logs/zora_discovery_dash.log"

BIND="${DISCOVERY_DASHBOARD_BIND:-127.0.0.1}"
PORT="${DISCOVERY_DASHBOARD_PORT:-8787}"

if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux is not installed." >&2
    echo "  macOS:  brew install tmux" >&2
    echo "  Linux:  apt install tmux  (or your distro's package manager)" >&2
    exit 1
fi

if [ ! -x "$PYTHON_BIN" ]; then
    echo "Discovery venv not found at $DISCOVERY_VENV -- run 'make discovery-build' first." >&2
    exit 1
fi

tmux kill-session -t "$SESSION" 2>/dev/null || true
rm -f "$DASH_PID_FILE"
mkdir -p "$ROOT/logs"

tmux new-session -d -s "$SESSION" -n main "$PYTHON_BIN" "$DASH_SCRIPT"
tmux set-option -t "$SESSION:main" remain-on-exit on

DASH_PID=$(tmux list-panes -t "$SESSION:main" -F '#{pane_pid}')
echo "$DASH_PID" >"$DASH_PID_FILE"

# Mirrors the main pane's own output to a plain-text log file, without
# disturbing the pane's process itself -- same pipe-pane pattern as
# zora_discovery.sh. `sed -u` strips ANSI escapes on the way into the
# file and stays line-buffered so the file updates in near real time.
tmux pipe-pane -o -t "$SESSION:main" "sed -u 's/\x1b\[[0-9;]*m//g' >>'$DASH_LOG'"

tmux set-option -t "$SESSION" status-interval 2
tmux set-option -t "$SESSION" status-left " ZORA DISCOVERY DASH   "
tmux set-option -t "$SESSION" status-left-length 40
tmux set-option -t "$SESSION" status-right "%H:%M:%S"

echo "ZORA discovery dashboard started (tmux session '$SESSION', logging to $DASH_LOG)."
echo "  http://$BIND:$PORT"
echo "Attach with 'make discovery-dashboard-attach' to watch its own stdout."
