#!/usr/bin/env bash
#
# Called by tmux every 2 seconds (via status-interval + status-left's
# embedded #(...)) to build the CEX/DEX/SPR dot row of the status bar.
# Must be fast and side-effect-free: tmux re-runs it on a timer, not us.
set -uo pipefail

check_proc() {
    local pid_file="$1" name="$2"
    if [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file" 2>/dev/null)" 2>/dev/null; then
        echo "#[fg=green]● $name#[default]"
    else
        echo "#[fg=red]● $name#[default]"
    fi
}

CEX=$(check_proc /tmp/zora_cex.pid "CEX")
DEX=$(check_proc /tmp/zora_dex.pid "DEX")
SPR=$(check_proc /tmp/zora_spr.pid "SPR")

echo "$CEX | $DEX | $SPR"
