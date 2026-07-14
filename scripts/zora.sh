#!/usr/bin/env bash
#
# Launches all three ZORA processes side by side in a tmux session,
# with a live status pane along the bottom. Kills any existing 'zora'
# session first, so this is always a clean start.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSION="zora"

CEX_BIN="$ROOT/build/zore_core"
DEX_BIN="$ROOT/ingestor-dex-solana/target/release/ingestor-dex-solana"
SPR_BIN="$ROOT/target/release/spread-detector"

CEX_PID_FILE="/tmp/zora_cex.pid"
DEX_PID_FILE="/tmp/zora_dex.pid"
SPR_PID_FILE="/tmp/zora_spr.pid"

if ! command -v tmux >/dev/null 2>&1; then
    echo "tmux is not installed." >&2
    echo "  macOS:  brew install tmux" >&2
    echo "  Linux:  apt install tmux  (or your distro's package manager)" >&2
    exit 1
fi

if [ ! -x "$CEX_BIN" ]; then
    echo "CEX binary not found at $CEX_BIN -- run 'make build' first." >&2
    exit 1
fi
if [ ! -x "$DEX_BIN" ]; then
    echo "DEX binary not found at $DEX_BIN -- run 'make build' first." >&2
    exit 1
fi
if [ ! -x "$SPR_BIN" ]; then
    echo "Spread detector binary not found at $SPR_BIN -- run 'make build' first." >&2
    exit 1
fi

tmux kill-session -t "$SESSION" 2>/dev/null || true
rm -f "$CEX_PID_FILE" "$DEX_PID_FILE" "$SPR_PID_FILE"
mkdir -p "$ROOT/data"

tmux new-session -d -s "$SESSION" -n main

# Capture real pane IDs (#{pane_id}, e.g. "%3") rather than assuming
# index numbers: robust regardless of tmux's own pane-numbering order,
# and lets every later command target the right pane unambiguously.
TOP_PANE=$(tmux display-message -t "$SESSION:main" -p '#{pane_id}')
STATUS_PANE=$(tmux split-window -t "$TOP_PANE" -v -l 10 -P -F '#{pane_id}')
# Split the remaining top area into three roughly-equal columns: first
# carve off ~2/3 to the right (leaving CEX at ~1/3 on the left), then
# split that remainder in half (DEX | SPR), landing all three near 33%.
REST_PANE=$(tmux split-window -t "$TOP_PANE" -h -p 67 -P -F '#{pane_id}')
CEX_PANE="$TOP_PANE"
SPR_PANE=$(tmux split-window -t "$REST_PANE" -h -p 50 -P -F '#{pane_id}')
DEX_PANE="$REST_PANE"

# Show each pane's title in its border. This is a window-level tmux
# option, set only for this session's window -- it does not touch
# ~/.tmux.conf or any other session/window.
tmux set-option -t "$SESSION:main" pane-border-status top
# tmux has no per-pane border *line* color (pane-border-style is
# uniform across the whole window), so "cyan/green/magenta border" is
# approximated here as a colored *title*, conditional on which pane is
# rendering its own border: real per-pane differentiation, just on the
# title text rather than the hairline itself.
tmux set-option -t "$SESSION:main" pane-border-format \
    "#{?#{==:#{pane_id},$CEX_PANE},#[fg=cyan]#{pane_title}#[default],#{?#{==:#{pane_id},$DEX_PANE},#[fg=green]#{pane_title}#[default],#{?#{==:#{pane_id},$SPR_PANE},#[fg=magenta]#{pane_title}#[default],#{pane_title}}}}"

tmux select-pane -t "$CEX_PANE" -T " ZORA CEX — Binance SOL/USDC "
tmux select-pane -t "$DEX_PANE" -T " ZORA DEX — Raydium SOL/USDC "
tmux select-pane -t "$SPR_PANE" -T " ZORA SPREAD — Opportunity Detector "
tmux select-pane -t "$STATUS_PANE" -T " ZORA STATUS "

# tmux's own status bar (the single strip below the whole pane grid,
# not the status pane above) -- session-scoped. Content is refreshed
# every second by zora_status.sh below, which sets these same two
# options with live values rather than relying on tmux's #(shell)
# format syntax, which would need extra escaping to embed color codes
# from a command's captured output.
tmux set-option -t "$SESSION" status-left " ZORA ARB "
tmux set-option -t "$SESSION" status-left-length 60
tmux set-option -t "$SESSION" status-right-length 20

# Each pane backgrounds its binary and writes the *real* PID to a file
# immediately (#{pane_pid} would give the pane's shell PID instead, not
# the process's), then waits on it -- keeping the pane attached to the
# child's lifetime, with its stdout still landing in this same pane.
tmux send-keys -t "$CEX_PANE" \
    "\"$CEX_BIN\" & echo \$! > \"$CEX_PID_FILE\"; wait" Enter
tmux send-keys -t "$DEX_PANE" \
    "\"$DEX_BIN\" & echo \$! > \"$DEX_PID_FILE\"; wait" Enter
tmux send-keys -t "$SPR_PANE" \
    "\"$SPR_BIN\" & echo \$! > \"$SPR_PID_FILE\"; wait" Enter

tmux send-keys -t "$STATUS_PANE" "\"$ROOT/scripts/zora_status.sh\"" Enter

echo "ZORA tmux session started."
echo "Attach with: tmux attach -t $SESSION   (or: make attach)"
