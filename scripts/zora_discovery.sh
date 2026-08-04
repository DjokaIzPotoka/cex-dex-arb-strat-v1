#!/usr/bin/env bash
#
# Launches the MEV discovery Tier 1 scheduler (discovery/src/scheduler.py)
# in its own tmux session ('zora-discovery'), separate from ZORA's
# paper-trading session ('zora' -- see scripts/zora.sh) so the two run
# and stop independently of each other.
#
# Unlike zora.sh's three separate OS processes (CEX/DEX/spread-detector,
# one pane/window each), discovery today is a single Python process
# that drives all 9 providers itself sequentially -- so there's one
# main pane, not one pane per source. Per-source visibility comes from
# each log line's SRC column (see discovery/src/logger.py and
# scheduler.py's SRC tag table), not from separate panes/windows.
#
# Runs the scheduler as the main pane's own process (passed to tmux as
# separate argv elements, not a shell string) so #{pane_pid} is the
# scheduler's real PID, not a wrapping shell's -- same reasoning as
# zora.sh's SPR_PID_FILE comment: this is what lets remain-on-exit show
# a crash's final output instead of the pane just closing, and what
# makes `make discovery-stop` target the right PID.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSION="zora-discovery"
DISCOVERY_VENV="$ROOT/discovery/.venv"
PYTHON_BIN="$DISCOVERY_VENV/bin/python3"
SCHEDULER_SCRIPT="$ROOT/discovery/src/scheduler.py"

SCHED_PID_FILE="/tmp/zora_discovery_sched.pid"
# Unlike CEX_LOG/DEX_LOG in zora.sh (which live in /tmp), this lives in
# the project directory and is never truncated on start -- same
# reasoning as zora.sh's SPR_LOG: it's meant to accumulate across
# restarts, not just cover the current session.
SCHED_LOG="$ROOT/logs/zora_discovery_sched.log"

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

if ! curl -s -o /dev/null "http://127.0.0.1:8123/ping"; then
    echo "ClickHouse isn't reachable at 127.0.0.1:8123 -- run 'make discovery-build' first (or start it if it's already provisioned)." >&2
    exit 1
fi

tmux kill-session -t "$SESSION" 2>/dev/null || true
rm -f "$SCHED_PID_FILE"
mkdir -p "$ROOT/logs"

tmux new-session -d -s "$SESSION" -n main "$PYTHON_BIN" "$SCHEDULER_SCRIPT"
tmux set-option -t "$SESSION:main" remain-on-exit on

SCHED_PID=$(tmux list-panes -t "$SESSION:main" -F '#{pane_pid}')
echo "$SCHED_PID" >"$SCHED_PID_FILE"

# Mirrors the main pane's own output to a plain-text log file, without
# disturbing the pane's process itself -- same pipe-pane pattern as
# zora.sh. `sed -u` strips ANSI escapes on the way into the file and
# stays line-buffered so the file updates in near real time.
tmux pipe-pane -o -t "$SESSION:main" "sed -u 's/\x1b\[[0-9;]*m//g' >>'$SCHED_LOG'"

tmux set-option -t "$SESSION" status-interval 2
tmux set-option -t "$SESSION" status-left " ZORA DISCOVERY   "
tmux set-option -t "$SESSION" status-left-length 40
tmux set-option -t "$SESSION" status-right "%H:%M:%S"

echo "ZORA discovery scheduler started (tmux session '$SESSION', logging to $SCHED_LOG)."
tmux attach -t "$SESSION"
