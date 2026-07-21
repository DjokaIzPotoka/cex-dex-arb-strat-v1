#!/usr/bin/env bash
#
# Called by tmux every 2 seconds for status bar line 2 (uptime,
# opportunity count, simulated P&L) -- not part of the literal
# one-line status bar spec (that's zora_status.sh), but matches the
# second line shown in the layout mockup, computed here from the
# paper-trades CSV the spread detector already writes.
set -uo pipefail

CSV_PATH="${1:?usage: zora_status_line2.sh <csv-path> <start-ts-file>}"
START_TS_FILE="${2:?usage: zora_status_line2.sh <csv-path> <start-ts-file>}"

now=$(date +%s)
if [ -f "$START_TS_FILE" ]; then
    # Falls back to `now` (elapsed 0) rather than epoch 0 if the file
    # is unreadable/empty -- epoch 0 would compute "elapsed" as the
    # entire time since 1970, a multi-decade "uptime".
    start_ts="$(cat "$START_TS_FILE" 2>/dev/null)"
    [ -z "$start_ts" ] && start_ts="$now"
else
    start_ts="$now"
fi
elapsed=$(( now - start_ts ))
[ "$elapsed" -lt 0 ] && elapsed=0
uptime_str=$(printf "%02d:%02d:%02d" $((elapsed / 3600)) $(((elapsed % 3600) / 60)) $((elapsed % 60)))

if [ -f "$CSV_PATH" ]; then
    row_count=$(wc -l <"$CSV_PATH" 2>/dev/null || echo 0)
    opp_count=$(( row_count - 1 )) # minus the header row
    [ "$opp_count" -lt 0 ] && opp_count=0

    # Column 7 (estimated_profit_usd) of every data row, summed. %+.2f
    # gets the leading +/- for free rather than needing separate sign
    # logic. Works with both gawk (Linux) and BSD/"one true" awk
    # (macOS) -- plain POSIX arithmetic and printf only.
    pnl=$(awk -F',' 'NR>1 { sum += $7 } END { printf "%+.2f", sum + 0 }' "$CSV_PATH" 2>/dev/null)
    [ -z "$pnl" ] && pnl="+0.00"
else
    opp_count=0
    pnl="+0.00"
fi

# pnl is e.g. "+2.40" or "-1.50" -- insert the $ after the sign so it
# reads "+$2.40" / "-$1.50" rather than "$+2.40".
sign="${pnl:0:1}"
amount="${pnl:1}"

echo "Uptime: ${uptime_str} | Opps: ${opp_count} | Sim. P&L: ${sign}\$${amount}"
