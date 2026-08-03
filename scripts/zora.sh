#!/usr/bin/env bash
#
# Launches ZORA: CEX and DEX run hidden in the background (logs go to
# /tmp, not any visible pane), and the spread detector runs full-screen
# in a tmux session's main window, since that's the process a human
# actually watches -- its full output is also mirrored to logs/zora_spr.log
# in the project directory (see the pipe-pane call below), appended
# across restarts rather than truncated, so it's reviewable long after
# any one session ends. Two hidden windows tail the CEX/DEX logs for
# debugging. Kills any existing 'zora' session first, so this is always
# a clean start.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSION="zora"

# Loads SOLANA_RPC_URL (and anything else) from .env if present, so the
# DEX ingestor picks up a dedicated RPC provider instead of falling back
# to the public endpoint -- see rpc.rs's build_client(). Silently a
# no-op if .env doesn't exist (e.g. a fresh checkout).
if [ -f "$ROOT/.env" ]; then
    set -a
    source "$ROOT/.env"
    set +a
fi

CEX_BIN="$ROOT/build/zore_core"
DEX_BIN="$ROOT/ingestor-dex-solana/target/release/ingestor-dex-solana"
SPR_BIN="$ROOT/target/release/spread-detector"

CEX_PID_FILE="/tmp/zora_cex.pid"
DEX_PID_FILE="/tmp/zora_dex.pid"
SPR_PID_FILE="/tmp/zora_spr.pid"
CEX_LOG="/tmp/zora_cex.log"
DEX_LOG="/tmp/zora_dex.log"
# Unlike CEX_LOG/DEX_LOG, this lives in the project directory (not
# /tmp) and is never truncated on start -- it's meant to accumulate
# across restarts, not just cover the current session.
SPR_LOG="$ROOT/logs/zora_spr.log"
START_TS_FILE="/tmp/zora_start_ts"

if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux is not installed." >&2
    echo "  macOS:  brew install tmux" >&2
    echo "  Linux:  apt install tmux  (or your distro's package manager)" >&2
    exit 1
fi

tmux kill-session -t "$SESSION" 2>/dev/null || true
rm -f "$CEX_PID_FILE" "$DEX_PID_FILE" "$SPR_PID_FILE"
mkdir -p "$ROOT/data" "$ROOT/logs"

if [ ! -x "$CEX_BIN" ] || [ ! -x "$DEX_BIN" ] || [ ! -x "$SPR_BIN" ]; then
    echo "One or more binaries not built yet -- running 'make build'..."
    make -C "$ROOT" build
fi

for bin_path in "$CEX_BIN" "$DEX_BIN" "$SPR_BIN"; do
    if [ ! -x "$bin_path" ]; then
        echo "Build did not produce $bin_path -- see build output above." >&2
        exit 1
    fi
done

echo "Starting CEX ingestor (background, logging to $CEX_LOG)..."
"$CEX_BIN" >"$CEX_LOG" 2>&1 &
echo $! >"$CEX_PID_FILE"

echo "Starting DEX ingestor (background, logging to $DEX_LOG)..."
"$DEX_BIN" >"$DEX_LOG" 2>&1 &
echo $! >"$DEX_PID_FILE"

echo "Waiting 2s for shared memory to initialize..."
sleep 2

date +%s >"$START_TS_FILE"

# The main window runs spread-detector directly as the pane's process
# (rather than typing the command into an already-running shell) --
# that makes #{pane_pid} the spread detector's *real* PID, not a
# wrapping shell's, and it's what lets remain-on-exit (below) show the
# process's final output if it crashes instead of the pane just closing.
tmux new-session -d -s "$SESSION" -n main "$SPR_BIN"
tmux set-option -t "$SESSION:main" remain-on-exit on

SPR_PID=$(tmux list-panes -t "$SESSION:main" -F '#{pane_pid}')
echo "$SPR_PID" >"$SPR_PID_FILE"

# Mirrors the main pane's own output to a plain-text log file, without
# disturbing the pane's process itself (spread-detector still runs
# directly as the pane's process -- see the PID comment above; this
# just taps a copy of the bytes the pty already receives). `sed -u`
# strips ANSI color/bold escape codes on the way into the file (the
# live pane still renders them normally) and stays line-buffered so the
# file updates in near real time, not just at process exit.
tmux pipe-pane -o -t "$SESSION:main" "sed -u 's/\x1b\[[0-9;]*m//g' >>'$SPR_LOG'"

# Hidden windows for debugging -- not visible by default, reached with
# Ctrl+B 1 / Ctrl+B 2 (or `make logs`, which doesn't need tmux at all).
tmux new-window -t "$SESSION" -n cex-log "tail -f '$CEX_LOG'"
tmux new-window -t "$SESSION" -n dex-log "tail -f '$DEX_LOG'"
tmux select-window -t "$SESSION:main"

# tmux's own status bar, session-scoped only -- doesn't touch
# ~/.tmux.conf or any other session. status-interval makes tmux
# re-evaluate status-left/status-right (and therefore re-run the
# embedded #() scripts) automatically every 2s; no loop process of our
# own is needed for this, unlike an earlier version of this script.
tmux set-option -t "$SESSION" status-interval 2
tmux set-option -t "$SESSION" status-left " ZORA ARB   #($ROOT/scripts/zora_status.sh) "
tmux set-option -t "$SESSION" status-left-length 80
# Bonus second status line (uptime / opportunity count / simulated P&L)
# beyond the single CEX/DEX/SPR line spelled out in the spec -- matches
# the second line shown in the layout mockup. Needs a 2-row status bar
# (tmux supports this via `status 2` + status-format[1]); see
# zora_status_line2.sh for how it's computed.
tmux set-option -t "$SESSION" status 2
tmux set-option -t "$SESSION" status-format[1] \
    "#($ROOT/scripts/zora_status_line2.sh '$ROOT/data/paper_trades.csv' '$START_TS_FILE')"
tmux set-option -t "$SESSION" status-right "%H:%M:%S"

echo "ZORA tmux session started."
tmux attach -t "$SESSION"
