#!/usr/bin/env python3
"""Read-only Streamlit ops dashboard for observing the Phase 1i 48h
integration test (docs/mev-discovery-plan.md) while it's running.

STRICT READ-ONLY, by design and by construction: every ClickHouse call
in this module is `client.query(...)` (a SELECT). There is no
`.insert(`, no `.command(`, no DDL, and no interaction whatsoever with
`known_entities`' check-then-insert path (detection.py owns that
entirely) -- this module only ever reads what scheduler.py/
detection.py/tier2_poller.py already wrote elsewhere. It never touches
the scheduler process itself (no PID signaling, no tmux control) --
`make discovery-stop`/`discovery-attach` remain the only way to control
that process. If a future edit to this file needs anything beyond
`client.query(...)`, that doesn't belong in this module.

Also deliberately light on ClickHouse load: every query here is either
a small `GROUP BY venue/exchange` aggregate or a `LIMIT`-bounded read,
nothing scans dex_pool_snapshots' full 90-day retention window, and the
log tail (see `_tail_lines`) reads backward from EOF rather than the
whole file -- all of this is re-run every ~30s (autorefresh) for the
full 48h test (up to ~5760 refreshes), so "cheap per call" matters here
in a way it wouldn't for a one-shot script.

No result is ever cached beyond one Streamlit rerun (only the
ClickHouse *connection object* is cached, via `st.cache_resource` --
see `_client()`) -- every number shown is from a query issued during
that rerun, so a 30s-old number is real staleness in the underlying
data, never a stale cache serving an old query result.

Launch: `make discovery-dashboard` (port 8501, same reachability
convention as the ClickHouse Play UI), or directly:
    discovery/.venv/bin/streamlit run discovery/ops_dashboard.py
"""
from __future__ import annotations

import datetime
import pathlib
import re
import sys
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from discovery.src.db import get_client  # noqa: E402
# Only the 4 hot_reason string constants -- tier2.py itself has no
# provider/RPC construction to worry about pulling in (unlike
# scheduler.py, see DEX_VENUES/CEX_EXCHANGES's own comment below), so
# importing these directly avoids a 4th place these strings could
# silently drift out of sync from tier2.py's own definitions.
from discovery.src.tier2 import (  # noqa: E402
    HOT_REASON_NEW_LISTING,
    HOT_REASON_NEW_POOL,
    HOT_REASON_TVL_SPIKE,
    HOT_REASON_VOLUME_SPIKE,
)

REFRESH_INTERVAL_MS = 30_000

# Replaces the old STALENESS_THRESHOLD_MIN (now-vs-snapshot_ts per
# source). That metric was retired, not just renamed: scheduler.py
# stamps every row in a Tier 1 cycle -- all 4 DEX venues and all 5 CEX
# exchanges alike -- with one shared snapshot_ts computed once at
# cycle start (see run_cycle()'s own docstring; this is correct and
# load-bearing for detection.py/tier2.py's "prior cycle" comparisons,
# not something to change). A per-source "now - snapshot_ts" therefore
# doesn't measure that source's own data freshness at all -- it mostly
# measures how late in the fixed DEX-then-CEX execution order that
# source happens to run: confirmed live, CEX sources (run last, after
# ~460-500s of DEX work) showed "~8-9min stale" on a row that had in
# fact just been written seconds earlier, while orca_whirlpool (run
# first) showed near-zero -- a pure sequencing artifact with nothing
# to do with any source's real health. See CLAUDE.md's "ops_dashboard
# staleness" section for the full live trace that pinned this down.
#
# Two separate, honest metrics replace it:
# - "Cycle age" (now - the current cycle's shared snapshot_ts): shown
#   ONCE, globally, not per source -- it's a property of the cycle,
#   not of any individual source, so 9 near-identical numbers differing
#   only by sequencing noise would just restate the same bias 9 times.
# - Per-source health, from ROW COUNT PRESENCE at the two most recent
#   distinct snapshot_ts values (see fetch_last_two_snapshot_ts /
#   fetch_source_row_counts_for_ts below) -- did this source write any
#   rows in the current cycle, not how long ago it did. This is exactly
#   what scheduler.py's own failure-isolation already guarantees is
#   meaningful: a source whose fetch raised is caught, logged, and
#   returns `[]` (see _run_dex_source/_run_cex_source's docstrings),
#   and _write_rows is a no-op for an empty list -- so a failed
#   source's row count at the current snapshot_ts is exactly 0, not
#   partial or delayed, regardless of where in the execution order it
#   sits. No new instrumentation needed: this reads the same two
#   tables every other query on this page already reads.

# Matches scheduler.py's DEX_SRC/CEX_SRC venue/exchange keys exactly --
# duplicated here rather than imported, deliberately: importing from
# scheduler.py would pull in that module's own provider construction
# (OrcaWhirlpoolRpcStateProvider et al., needing SOLANA_RPC_URL) just
# to read two dict literals, for a dashboard that must never construct
# or touch a live provider/RPC connection at all.
DEX_VENUES = ["orca_whirlpool", "raydium_amm_v4", "raydium_clmm", "meteora_dlmm"]
CEX_EXCHANGES = ["binance", "bybit", "okx", "bitget", "gateio"]
ALL_SOURCES = DEX_VENUES + CEX_EXCHANGES

# scheduler.py's own DEX_SRC/CEX_SRC 3-char tags, same duplication
# rationale as ALL_SOURCES above -- used for the compact per-source
# indicator row (see main()).
SRC_TAG = {
    "orca_whirlpool": "ORC",
    "raydium_amm_v4": "RA4",
    "raydium_clmm": "RCL",
    "meteora_dlmm": "MET",
    "binance": "BNC",
    "bybit": "BYB",
    "okx": "OKX",
    "bitget": "BTG",
    "gateio": "GTE",
}

# scripts/zora_discovery.sh's SCHED_LOG path -- mirrored, not derived
# (that script is bash, this is Python; there's no shared config file
# either reads from).
SCHED_LOG_PATH = pathlib.Path(__file__).resolve().parents[1] / "logs" / "zora_discovery_sched.log"

# How far back (lines) to scan the log, shared by every log-derived
# metric on this page (error tail, cycle-duration history, the
# getMultipleAccounts RPC-error counter, Meteora's cursor position --
# see fetch_log_metrics, one tail read serves all four). Bumped from
# the original 5_000 (sized only for "find the last ~20 ERROR lines"):
# the RPC-error counter needs to find the CURRENT run's own
# scheduler_start line, and Meteora's own ghost-pool parse_skip WARN
# volume dominates this log's line count -- confirmed live, just 2
# consecutive cycles produced ~67k lines end to end (grep'd
# scheduler_start-to-scheduler_start on a real run), so even a first
# guess of 50_000 measurably undershot "one recent run" in practice,
# not just a hypothetical worst case. 150_000 lines (~15MB, ~230
# chunked 64KB reads via _tail_lines -- still O(max_lines), never
# O(file size), see that function's docstring) comfortably covers
# several cycles/~30-45min of typical runtime, while staying fast
# enough on local disk for a dashboard re-reading this on every 30s
# autorefresh (see module docstring's "cheap per call" note) -- not
# raised further than that without direct evidence it's still
# insufficient in practice, since cost scales linearly with this
# constant. Beyond this window (a run that's been up longer, or an
# unusually log-heavy stretch), the RPC-error count is honestly
# labeled as a lower bound for the scanned window rather than silently
# understated as "since start" -- see fetch_log_metrics.
LOG_SCAN_LINES = 150_000
LOG_ERRORS_SHOWN = 20
CYCLE_DURATIONS_SHOWN = 20

# logger.py's format: "HH:MM:SS.mmm  LEVEL  SRC  key=value ...". LEVEL
# is one of "INFO ", "WARN ", "ERROR" (Level enum values, all 5 chars)
# -- \s+ splitting collapses the format string's literal double spaces,
# so this matches regardless of that padding.
LOG_LINE_RE = re.compile(r"^\S+\s+(\S+)\s+(\S+)\s+(.*)$")
CYCLE_ELAPSED_RE = re.compile(r"elapsed_s=([\d.]+)")
METEORA_CURSOR_RE = re.compile(r"start_page=(\d+).*end_page=(\d+).*total_pages=(\d+)")


def _as_utc(dt: Optional[datetime.datetime]) -> Optional[datetime.datetime]:
    """clickhouse-connect returns naive datetimes for DateTime64
    columns (no tzinfo attached), while every snapshot_ts/detected_ts
    this schema ever writes is produced from datetime.now(timezone.utc)
    at write time -- see tier2.py's identical note on
    AgeBasedRegistry.seed_from_history. Re-attaching UTC here is a
    correct fix, not a guess, and keeps every value this module
    computes durations against comparably aware.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    return dt


@st.cache_resource
def _client():
    """Cached across reruns (autorefresh triggers a full script rerun
    every REFRESH_INTERVAL_MS) so this dashboard doesn't open a fresh
    ClickHouse connection every ~30s -- this caches the *connection*,
    never any query result, so it has no bearing on data freshness
    (see module docstring).
    """
    return get_client()


# --- Data access (all read-only SELECTs) ----------------------------


def fetch_last_snapshot_ts(client) -> dict[str, Optional[datetime.datetime]]:
    """venue/exchange -> its most recent snapshot_ts, or None if that
    source has never written a row at all. Purely informational now
    ("when did this source last write anything, ever") -- NOT used for
    any fresh/stale judgment (see the module-level comment above
    REFRESH_INTERVAL_MS for why a per-source now-vs-snapshot_ts
    comparison is structurally misleading here). That judgment is
    fetch_source_row_counts_for_ts's job below.
    """
    result: dict[str, Optional[datetime.datetime]] = {src: None for src in ALL_SOURCES}
    dex = client.query("SELECT venue, max(snapshot_ts) FROM dex_pool_snapshots GROUP BY venue")
    for venue, ts in dex.result_rows:
        result[venue] = _as_utc(ts)
    cex = client.query("SELECT exchange, max(snapshot_ts) FROM cex_symbol_snapshots GROUP BY exchange")
    for exchange, ts in cex.result_rows:
        result[exchange] = _as_utc(ts)
    return result


def fetch_last_two_snapshot_ts(client) -> tuple[Optional[datetime.datetime], Optional[datetime.datetime]]:
    """Returns (previous_cycle_ts, current_cycle_ts) -- the two most
    recent DISTINCT snapshot_ts values from `cex_symbol_snapshots`
    specifically, NOT a union across both tables (found live, while
    validating this dashboard change, before it ever shipped: a UNION
    over dex_pool_snapshots too pulls in tier2_poller.py's OWN,
    separate `snapshot_ts = datetime.datetime.now(...)` stamped fresh
    on every ~60-90s Tier 2 poll -- a different, much more frequent
    timestamp than Tier 1's once-per-cycle one, and one CEX rows never
    share since Tier 2 only ever re-polls DEX hot pools, never CEX.
    Unioning both tables meant "current_ts" was usually a Tier 2 poll
    moment, not a Tier 1 cycle boundary -- which made every CEX source
    look permanently "STOPPED" (0 rows at a timestamp CEX never writes
    to by construction), reintroducing a structural bias against CEX
    from a different angle than the one this change exists to fix.

    `cex_symbol_snapshots` has no such pollution -- every row in it
    comes from exactly one place, scheduler.py's Tier 1 loop, so its
    own distinct snapshot_ts values ARE exactly the Tier 1 cycle
    boundaries, one per completed cycle. Since every source (DEX and
    CEX alike) shares one snapshot_ts per Tier 1 cycle (see the
    module-level comment above REFRESH_INTERVAL_MS), those same two
    values are valid reference points for checking DEX venues' row
    counts too (fetch_source_row_counts_for_ts) -- any DEX rows from
    an intervening Tier 2 poll simply won't match either exact value
    and are correctly excluded.

    A useful side effect: because CEX is always the last group fetched
    each cycle (see scheduler.py's DEX-then-CEX order), a snapshot_ts
    only ever appears in `cex_symbol_snapshots` once that whole cycle
    has actually finished -- so `current_ts` here is always a
    genuinely completed cycle, never one still in progress. `Either or
    both can be None early in a fresh run (right after a TRUNCATE,
    before even one cycle has reached its CEX phase, or after exactly
    one).
    """
    result = client.query("SELECT DISTINCT snapshot_ts FROM cex_symbol_snapshots ORDER BY snapshot_ts DESC LIMIT 2")
    ts_list = [_as_utc(row[0]) for row in result.result_rows]
    current_ts = ts_list[0] if len(ts_list) >= 1 else None
    previous_ts = ts_list[1] if len(ts_list) >= 2 else None
    return previous_ts, current_ts


def fetch_source_row_counts_for_ts(
    client, ts_values: list[datetime.datetime]
) -> dict[str, dict[datetime.datetime, int]]:
    """source -> {snapshot_ts: row_count}, for each of `ts_values`
    (only ever called with the last-two-cycles pair from
    fetch_last_two_snapshot_ts). A source/timestamp combination with
    no matching rows is simply absent from its inner dict -- callers
    treat that as count 0, not queried separately, since ClickHouse
    GROUP BY only returns combinations that actually exist.

    This is the real "is source X still working" signal (per this
    session's explicit scope: reuse existing data, no new
    instrumentation, no log parsing): scheduler.py's per-source
    failure isolation (_run_dex_source/_run_cex_source) means an
    exception during a source's fetch is caught and turned into `[]`,
    and _write_rows() is a no-op for an empty list -- so a source that
    failed THIS cycle has exactly 0 rows at the current cycle's
    snapshot_ts, full stop, regardless of how many rows it wrote in
    every prior cycle or where in the execution order it sits.
    """
    if not ts_values:
        return {src: {} for src in ALL_SOURCES}
    result: dict[str, dict[datetime.datetime, int]] = {src: {} for src in ALL_SOURCES}
    dex = client.query(
        "SELECT venue, snapshot_ts, count() FROM dex_pool_snapshots WHERE snapshot_ts IN {ts:Array(DateTime64(3))} GROUP BY venue, snapshot_ts",
        parameters={"ts": ts_values},
    )
    for venue, ts, count in dex.result_rows:
        result.setdefault(venue, {})[_as_utc(ts)] = count
    cex = client.query(
        "SELECT exchange, snapshot_ts, count() FROM cex_symbol_snapshots WHERE snapshot_ts IN {ts:Array(DateTime64(3))} GROUP BY exchange, snapshot_ts",
        parameters={"ts": ts_values},
    )
    for exchange, ts, count in cex.result_rows:
        result.setdefault(exchange, {})[_as_utc(ts)] = count
    return result


def fetch_test_start(client) -> Optional[datetime.datetime]:
    """min(snapshot_ts) across both snapshot tables -- "since the last
    TRUNCATE", per this dashboard's own operating assumption that a
    fresh 48h test starts from empty tables (see module docstring's
    load note: this is a cheap MergeTree min, not a full scan).
    """
    candidates = []
    for table in ("dex_pool_snapshots", "cex_symbol_snapshots"):
        r = client.query(f"SELECT min(snapshot_ts) FROM {table}")
        ts = r.result_rows[0][0] if r.result_rows else None
        if ts is not None:
            candidates.append(_as_utc(ts))
    return min(candidates) if candidates else None


def fetch_row_counts_since(client, test_start: Optional[datetime.datetime]) -> dict[str, int]:
    result: dict[str, int] = {src: 0 for src in ALL_SOURCES}
    if test_start is None:
        return result
    dex = client.query(
        "SELECT venue, count() FROM dex_pool_snapshots WHERE snapshot_ts >= {t:DateTime64(3)} GROUP BY venue",
        parameters={"t": test_start},
    )
    for venue, c in dex.result_rows:
        result[venue] = c
    cex = client.query(
        "SELECT exchange, count() FROM cex_symbol_snapshots WHERE snapshot_ts >= {t:DateTime64(3)} GROUP BY exchange",
        parameters={"t": test_start},
    )
    for exchange, c in cex.result_rows:
        result[exchange] = c
    return result


def fetch_tier2_summary(client) -> list[tuple[str, str, int]]:
    """Current (tier, hot_reason) -> pool_count, using each pool's most
    recent row only -- argMax(tier/hot_reason, snapshot_ts) per
    (venue, pool_address), not a raw GROUP BY over full history (which
    would massively overcount every pool by however many snapshots
    it's accumulated).
    """
    result = client.query(
        """
        SELECT tier, hot_reason, count() AS pool_count
        FROM (
            SELECT
                venue,
                pool_address,
                argMax(tier, snapshot_ts) AS tier,
                argMax(hot_reason, snapshot_ts) AS hot_reason
            FROM dex_pool_snapshots
            GROUP BY venue, pool_address
        )
        GROUP BY tier, hot_reason
        ORDER BY pool_count DESC
        """
    )
    return result.result_rows


def fetch_new_entity_count_since(client, test_start: Optional[datetime.datetime]) -> int:
    if test_start is None:
        return 0
    r = client.query(
        "SELECT count() FROM new_entity_events WHERE detected_ts >= {t:DateTime64(3)}",
        parameters={"t": test_start},
    )
    return int(r.result_rows[0][0]) if r.result_rows else 0


def fetch_recent_new_entities(client, limit: int = 10) -> list[tuple]:
    """Most recent rows overall, NOT scoped to test_start -- unlike the
    count above, new_entity_events is a permanent, un-truncated
    registry (see migrations/001_create_tables.sql's comment), so
    "most recent" is meaningful independent of when this test started.
    """
    r = client.query(
        "SELECT entity_type, venue, identifier, detected_ts FROM new_entity_events ORDER BY detected_ts DESC LIMIT {n:UInt32}",
        parameters={"n": limit},
    )
    return r.result_rows


def source_status(
    row_counts: dict[str, dict[datetime.datetime, int]],
    src: str,
    previous_ts: Optional[datetime.datetime],
    current_ts: Optional[datetime.datetime],
) -> str:
    """One of "ok" / "pending" / "STOPPED" for one source.

    `current_ts` (from fetch_last_two_snapshot_ts) is always a
    genuinely COMPLETED Tier 1 cycle, never one still in progress --
    it's anchored on cex_symbol_snapshots, which only ever gets a
    snapshot_ts once that cycle has already reached its last
    (CEX) phase (see that function's docstring for why, and for the
    Tier 2 poll-timestamp pollution bug this sidesteps). So a source
    with 0 rows at `current_ts` isn't "hasn't had its turn yet this
    round" the way the old now-vs-snapshot_ts metric could be misread
    -- that cycle is over, and this source produced nothing in it.

    Still deliberately a 3-state, not 2-state, check, though: a single
    completed cycle with 0 rows could be a one-off transient blip
    (a timeout, a momentary API hiccup) rather than the source being
    actually broken -- scheduler.py's own per-source failure isolation
    treats exactly this as expected/recoverable, not an emergency (see
    _run_dex_source/_run_cex_source's docstrings). "pending" names
    that "failed once, watching" state explicitly rather than either
    false-alarming on a single blip or silently calling it "ok".

    Only 0 rows at BOTH `current_ts` and `previous_ts` -- two
    consecutive COMPLETED cycles, not two dashboard refreshes -- counts
    as "STOPPED": a real, confirmed problem (bad URL, API change, auth
    failure), not noise.
    """
    current_count = row_counts.get(src, {}).get(current_ts, 0) if current_ts else 0
    if current_count > 0:
        return "ok"
    if previous_ts is None:
        # Only one cycle has ever completed (or none) -- can't yet
        # tell "one-off blip" apart from "broken" for this source,
        # so don't claim either.
        return "pending"
    previous_count = row_counts.get(src, {}).get(previous_ts, 0)
    return "STOPPED" if previous_count == 0 else "pending"


def compute_health_from_row_counts(
    row_counts: dict[str, dict[datetime.datetime, int]],
    previous_ts: Optional[datetime.datetime],
    current_ts: Optional[datetime.datetime],
) -> tuple[str, str, dict[str, str]]:
    """Returns (level, reason, status_by_source) where level is
    'green'/'yellow'/'red' and status_by_source[src] is
    source_status()'s per-source verdict. green: no source is
    STOPPED (a source still "pending" -- simply hasn't had its turn
    in an in-progress cycle yet -- is not a problem, see
    source_status()'s docstring). red: current_ts is None (no cycle
    has ever written anything) or every source is STOPPED. yellow:
    some but not all sources STOPPED.
    """
    total = len(ALL_SOURCES)
    if current_ts is None:
        return "red", "no Tier 1 cycle has written any data yet", {src: "pending" for src in ALL_SOURCES}
    status_by_source = {src: source_status(row_counts, src, previous_ts, current_ts) for src in ALL_SOURCES}
    stopped = [src for src, status in status_by_source.items() if status == "STOPPED"]
    if not stopped:
        return "green", f"no source has gone 2 consecutive cycles without writing (out of {total} sources)", status_by_source
    if len(stopped) == total:
        return "red", "every source has gone 2 consecutive cycles without writing -- " + ", ".join(stopped), status_by_source
    return "yellow", f"{len(stopped)}/{total} sources stopped (0 rows, 2 consecutive cycles) -- " + ", ".join(stopped), status_by_source


# --- Log tail (read-only, bounded, no full-file scan) -----------------


def _tail_lines(path: pathlib.Path, max_lines: int, chunk_size: int = 65_536) -> list[str]:
    """Reads roughly the last `max_lines` lines of `path` without
    loading the whole file -- this dashboard re-reads the log on every
    ~30s autorefresh for up to 48h (~5760 reads), and the log only
    grows over that window, so this must stay O(max_lines), not
    O(file size). Reads backward in chunk_size-byte blocks from EOF
    until enough newlines are seen or the start of the file is
    reached -- same idea as `tail -n`, just without shelling out.
    """
    with path.open("rb") as f:
        f.seek(0, 2)
        pos = f.tell()
        blocks: list[bytes] = []
        newlines_seen = 0
        while pos > 0 and newlines_seen <= max_lines:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            block = f.read(read_size)
            blocks.append(block)
            newlines_seen += block.count(b"\n")
        data = b"".join(reversed(blocks))
    lines = data.decode("utf-8", errors="replace").splitlines()
    return lines[-max_lines:] if len(lines) > max_lines else lines


def _is_error_line(line: str) -> bool:
    m = LOG_LINE_RE.match(line)
    return bool(m) and m.group(1) == "ERROR"


@dataclass
class LogMetrics:
    """Everything this page derives from the scheduler log, in one
    place -- see fetch_log_metrics.
    """

    error_lines: Optional[list[str]]
    failure_reason: Optional[str]
    cycle_durations_s: list[float]
    rpc_error_count: int
    rpc_error_count_is_lower_bound: bool
    meteora_cursor: Optional[str]


def fetch_log_metrics(log_path: pathlib.Path) -> LogMetrics:
    """One tail read serves every log-derived metric on this page --
    the ERROR-line tail, per-cycle duration history (for the
    stability line chart), the getMultipleAccounts RPC-error counter,
    and Meteora's current cursor position -- rather than re-reading
    the log once per metric. Still only ever reads backward from EOF
    (see _tail_lines), never the whole file, even across a full 48h
    run.

    `failure_reason` set (log missing/unreadable) means every other
    field is a placeholder empty/zero value, NOT "confirmed clean" --
    the UI must not conflate "nothing found" with "couldn't look",
    same distinction the original single-purpose error-tail version of
    this function made.
    """
    empty = LogMetrics(None, None, [], 0, False, None)
    if not log_path.exists():
        return LogMetrics(
            None, f"log file not found at {log_path} -- has the scheduler ever been started?", [], 0, False, None
        )
    try:
        lines = _tail_lines(log_path, LOG_SCAN_LINES)
    except OSError as exc:
        return LogMetrics(None, f"failed to read log file: {exc}", [], 0, False, None)
    if not lines:
        return empty

    error_lines = [ln for ln in lines if _is_error_line(ln)][-LOG_ERRORS_SHOWN:]

    # cycle_durations_s intentionally reads the FULL scanned window,
    # not scoped to the current run -- a stability trend line is more
    # useful spanning a recent restart than artificially truncated to
    # "since start", unlike the RPC-error counter below (which section
    # 5's own scope explicitly ties to "since the scheduler's current
    # start").
    cycle_durations_s: list[float] = []
    for ln in lines:
        if "event=cycle_done" in ln:
            m = CYCLE_ELAPSED_RE.search(ln)
            if m:
                cycle_durations_s.append(float(m.group(1)))
    cycle_durations_s = cycle_durations_s[-CYCLE_DURATIONS_SHOWN:]

    # Only lines at or after the most recent scheduler_start belong to
    # the CURRENT run -- found by scanning backward from EOF (the same
    # direction _tail_lines itself reads in), so this is the LAST
    # scheduler_start in the file, matching "current start" exactly.
    start_idx = None
    for i in range(len(lines) - 1, -1, -1):
        if "event=scheduler_start" in lines[i]:
            start_idx = i
            break
    scoped_lines = lines[start_idx:] if start_idx is not None else lines
    is_lower_bound = start_idx is None  # scheduler_start itself wasn't within the scanned window

    # "getMultipleAccounts request failed" is _get_multiple_accounts_raw's
    # own exact error-message prefix (solana_rpc_state.py) -- matches
    # every getMultipleAccounts-level failure (413s and otherwise),
    # deliberately NOT the broader "event=poll_source_FAILED" (which
    # also fires for Meteora/Raydium AMM v4's unrelated Tier 2 REST
    # failures -- see tier2_poller.py's _poll_once()).
    rpc_error_count = sum(1 for ln in scoped_lines if "getMultipleAccounts request failed" in ln)

    meteora_cursor = None
    for ln in reversed(scoped_lines):
        if "event=meteora_cursor_slice" in ln:
            m = METEORA_CURSOR_RE.search(ln)
            if m:
                meteora_cursor = f"pages {m.group(1)}-{m.group(2)} of {m.group(3)}"
            break

    return LogMetrics(error_lines, None, cycle_durations_s, rpc_error_count, is_lower_bound, meteora_cursor)


# --- UI ----------------------------------------------------------------


STATUS_EMOJI = {"ok": "\U0001f7e2", "pending": "\U0001f7e1", "STOPPED": "\U0001f534"}  # green/yellow/red circles


def main() -> None:
    # layout="wide" (item 1) + every section below sized to fit one
    # normal desktop screen without scrolling (item scope: layout only,
    # no query-logic changes -- every function called here is unchanged
    # from the version already validated last session).
    st.set_page_config(page_title="ZORA Discovery Ops", layout="wide")
    st_autorefresh(interval=REFRESH_INTERVAL_MS, key="autorefresh")

    st.markdown(
        "##### ZORA MEV Discovery -- Ops Dashboard "
        f"<span style='font-weight:normal;font-size:0.7em;color:gray'>"
        f"read-only, auto-refreshes every {REFRESH_INTERVAL_MS // 1000}s, "
        "does not control the scheduler (`make discovery-attach`/`discovery-stop` for that)"
        "</span>",
        unsafe_allow_html=True,
    )

    try:
        client = _client()
        now = datetime.datetime.now(datetime.timezone.utc)
        freshness = fetch_last_snapshot_ts(client)
        test_start = fetch_test_start(client)
        row_counts_since_start = fetch_row_counts_since(client, test_start)
        previous_ts, current_ts = fetch_last_two_snapshot_ts(client)
        cycle_row_counts = fetch_source_row_counts_for_ts(
            client, [ts for ts in (previous_ts, current_ts) if ts is not None]
        )
        tier2_summary = fetch_tier2_summary(client)
        new_entity_count = fetch_new_entity_count_since(client, test_start)
        recent_new_entities = fetch_recent_new_entities(client)
        log_metrics = fetch_log_metrics(SCHED_LOG_PATH)
    except Exception as exc:  # noqa: BLE001 -- a dashboard must degrade to a visible error, never crash silently
        st.error(f"Failed to query ClickHouse: {exc}")
        st.stop()

    level, reason, status_by_source = compute_health_from_row_counts(cycle_row_counts, previous_ts, current_ts)

    # --- top row: health badge + cycle age + test uptime, front and center (item 2) ---
    badge = {"green": st.success, "yellow": st.warning, "red": st.error}[level]
    badge(f"**{level.upper()}** -- {reason}")

    m1, m2, m3, m4 = st.columns(4)
    if current_ts is not None:
        cycle_age_min = (now - current_ts).total_seconds() / 60
        m1.metric(
            "Cycle age",
            f"{cycle_age_min:.1f}m",
            help=(
                "Time since the CURRENT (completed) cycle's shared snapshot_ts -- "
                "one value for all 9 sources, NOT a per-source fetch-recency "
                "measure (see fetch_last_two_snapshot_ts's docstring). A cycle "
                "now runs ~8-8.5min, so ~0-9min here is normal."
            ),
        )
    else:
        m1.metric("Cycle age", "n/a", help="no cycle has completed yet")
    if test_start is not None:
        uptime = now - test_start
        m2.metric(
            "Test uptime",
            f"{uptime.days}d {uptime.seconds // 3600}h {(uptime.seconds % 3600) // 60}m",
            help=f"since {test_start.isoformat()} (min snapshot_ts across both tables)",
        )
    else:
        m2.metric("Test uptime", "n/a", help="no rows in either snapshot table yet")
    ok_count = sum(1 for status in status_by_source.values() if status == "ok")
    m3.metric(
        "Sources active",
        f"{ok_count}/{len(ALL_SOURCES)}",
        help='"ok" this cycle -- a source showing "pending" below may just be a one-off blip, not yet a confirmed problem',
    )
    m4.metric("new_entity_events", new_entity_count, help="since test start (min snapshot_ts across both tables)")

    # --- getMultipleAccounts RPC-error counter (item 5a): visually prominent, not buried ---
    rpc_scope = "since scheduler start" if not log_metrics.rpc_error_count_is_lower_bound else f"in the last {LOG_SCAN_LINES:,} log lines (scheduler_start not found further back)"
    if log_metrics.failure_reason is not None:
        st.warning(f"getMultipleAccounts RPC errors: unknown -- {log_metrics.failure_reason}")
    elif log_metrics.rpc_error_count == 0:
        st.success(f"✅ getMultipleAccounts RPC errors {rpc_scope}: **0**")
    else:
        st.error(f"⚠️ getMultipleAccounts RPC errors {rpc_scope}: **{log_metrics.rpc_error_count}**")

    # --- per-source status: one compact row of 9 indicators (item 3) ---
    indicator_line = "  ".join(
        f"{STATUS_EMOJI[status_by_source.get(src, 'pending')]}{SRC_TAG[src]}" for src in ALL_SOURCES
    )
    st.markdown(f"##### {indicator_line}")
    not_ok = [f"{SRC_TAG[src]}: {status_by_source[src]}" for src in ALL_SOURCES if status_by_source.get(src) != "ok"]
    meteora_cursor_note = f"Meteora cursor: {log_metrics.meteora_cursor or 'n/a (no slice yet this run)'}"
    st.caption(("all sources ok" if not not_ok else " · ".join(not_ok)) + f"&nbsp;&nbsp;|&nbsp;&nbsp;{meteora_cursor_note}")

    st.divider()

    # --- two-column section (item 4): cycle-duration chart + Tier 2 hot-pool counts ---
    left, right = st.columns(2)
    with left:
        st.markdown(f"**Cycle duration, last {len(log_metrics.cycle_durations_s)} cycles (s)**")
        if log_metrics.cycle_durations_s:
            st.line_chart(pd.DataFrame({"elapsed_s": log_metrics.cycle_durations_s}), height=180)
        else:
            st.caption("no cycle_done lines in the scanned log window yet")
    with right:
        st.markdown("**Tier 2 hot pools by reason**")
        hot_counts = {hot_reason: 0 for hot_reason in (HOT_REASON_NEW_POOL, HOT_REASON_VOLUME_SPIKE, HOT_REASON_TVL_SPIKE, HOT_REASON_NEW_LISTING)}
        for tier, hot_reason, pool_count in tier2_summary:
            if tier == "hot" and hot_reason in hot_counts:
                hot_counts[hot_reason] = pool_count
        h1, h2, h3, h4 = st.columns(4)
        h1.metric("new_pool", hot_counts[HOT_REASON_NEW_POOL])
        h2.metric("volume_spike", hot_counts[HOT_REASON_VOLUME_SPIKE])
        h3.metric("tvl_spike", hot_counts[HOT_REASON_TVL_SPIKE])
        h4.metric("new_listing", hot_counts[HOT_REASON_NEW_LISTING])

    # --- collapsed detail sections (item 6) ---
    d1, d2 = st.columns(2)
    with d1:
        with st.expander(f"Per-source detail table ({len(ALL_SOURCES)} sources)"):
            rows = []
            for kind, sources in (("DEX", DEX_VENUES), ("CEX", CEX_EXCHANGES)):
                for src in sources:
                    ts = freshness.get(src)
                    rows.append(
                        {
                            "source": src,
                            "kind": kind,
                            "status": status_by_source.get(src, "pending"),
                            "rows this cycle": cycle_row_counts.get(src, {}).get(current_ts, 0) if current_ts else 0,
                            "rows previous cycle": cycle_row_counts.get(src, {}).get(previous_ts, 0) if previous_ts else 0,
                            "last write ever (UTC)": ts.isoformat(sep=" ", timespec="seconds") if ts else "never",
                            "rows since test start": row_counts_since_start.get(src, 0),
                        }
                    )
            st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
        with st.expander(f"new_entity_events ({new_entity_count} since test start, showing 10 most recent overall)"):
            if recent_new_entities:
                df = pd.DataFrame(recent_new_entities, columns=["entity_type", "venue", "identifier", "detected_ts"])
                st.dataframe(df, hide_index=True, width="stretch")
            else:
                st.caption("no rows in new_entity_events yet")
    with d2:
        error_summary = (
            f"log unreadable -- {log_metrics.failure_reason}"
            if log_metrics.failure_reason is not None
            else ("no errors" if not log_metrics.error_lines else f"{len(log_metrics.error_lines)} error(s) -- expand for detail")
        )
        with st.expander(f"Recent problems: {error_summary}"):
            if log_metrics.failure_reason is not None:
                st.warning(log_metrics.failure_reason)
            elif not log_metrics.error_lines:
                st.caption(f"no ERROR lines in the last {LOG_SCAN_LINES:,} scanned log lines")
            else:
                st.code("\n".join(reversed(log_metrics.error_lines)), language=None)
                st.caption(f"most recent first, from {SCHED_LOG_PATH}")


if __name__ == "__main__":
    main()
