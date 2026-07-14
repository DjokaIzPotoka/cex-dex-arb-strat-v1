#!/usr/bin/env bash
#
# Runs inside the bottom "ZORA STATUS" tmux pane: a live dashboard
# (PIDs, uptime, RUNNING/ERROR per process) refreshed every second, and
# also updates tmux's own status bar with the same information.
# Ctrl+C here stops all three processes and tears down the whole
# session -- this is the "Ctrl+C" half of the graceful shutdown spec;
# `make stop` from outside tmux is the other half, and both call
# scripts/stop.sh.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSION="zora"

CEX_PID_FILE="/tmp/zora_cex.pid"
DEX_PID_FILE="/tmp/zora_dex.pid"
SPR_PID_FILE="/tmp/zora_spr.pid"

start_ts=$(date +%s)

is_alive() {
    [ -f "$1" ] && kill -0 "$(cat "$1" 2>/dev/null)" 2>/dev/null
}

cleanup() {
    echo ""
    echo "Caught shutdown signal -- stopping all ZORA processes..."
    bash "$ROOT/scripts/stop.sh"
    exit 0
}
trap cleanup SIGINT SIGTERM

while true; do
    now=$(date +%s)
    elapsed=$((now - start_ts))
    uptime_str=$(printf "%02d:%02d:%02d" $((elapsed / 3600)) $(((elapsed % 3600) / 60)) $((elapsed % 60)))

    if is_alive "$CEX_PID_FILE"; then
        cex_pid="$(cat "$CEX_PID_FILE")"
        cex_status="RUNNING"
        cex_dot="#[fg=green]●#[default]"
    else
        cex_pid="-"
        cex_status="ERROR"
        cex_dot="#[fg=red]●#[default]"
    fi

    if is_alive "$DEX_PID_FILE"; then
        dex_pid="$(cat "$DEX_PID_FILE")"
        dex_status="RUNNING"
        dex_dot="#[fg=green]●#[default]"
    else
        dex_pid="-"
        dex_status="ERROR"
        dex_dot="#[fg=red]●#[default]"
    fi

    if is_alive "$SPR_PID_FILE"; then
        spr_pid="$(cat "$SPR_PID_FILE")"
        spr_status="RUNNING"
        spr_dot="#[fg=green]●#[default]"
    else
        spr_pid="-"
        spr_status="ERROR"
        spr_dot="#[fg=red]●#[default]"
    fi

    # This pane is a live dashboard, not a log -- clear+redraw each
    # second is the right model here, unlike the CEX/DEX/SPR panes,
    # where every line is history worth keeping on scroll.
    clear
    echo "ZORA ARB STATUS"
    echo ""
    printf "CEX  PID=%-8s  %s\n" "$cex_pid" "$cex_status"
    printf "DEX  PID=%-8s  %s\n" "$dex_pid" "$dex_status"
    printf "SPR  PID=%-8s  %s\n" "$spr_pid" "$spr_status"
    echo ""
    echo "Uptime: $uptime_str"
    echo ""
    echo "(Ctrl+C here stops all processes and closes this session)"

    # tmux has no independently-justified "center" segment in its
    # status bar -- the middle area is the window list, not free-form
    # text -- so the three dots are appended after the left label
    # rather than mathematically centered.
    tmux set-option -t "$SESSION" status-left \
        " ZORA ARB   CEX ${cex_dot} | DEX ${dex_dot} | SPR ${spr_dot} " 2>/dev/null || true
    tmux set-option -t "$SESSION" status-right \
        "$(date +%H:%M:%S)" 2>/dev/null || true

    sleep 1
done
