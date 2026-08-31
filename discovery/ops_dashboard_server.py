#!/usr/bin/env python3
"""Read-only FastAPI ops dashboard for observing the Phase 1i 48h
integration test (docs/mev-discovery-plan.md) while it's running.

Replaces the earlier Streamlit version (ops_dashboard.py, removed) --
same read-only guarantee, same underlying ClickHouse queries and
health/status computation, just served as a JSON API
(GET /api/status) consumed by a static HTML/CSS/JS frontend
(discovery/static/) instead of Streamlit widgets. This split exists
because precise custom styling (per-row status coloring, dense table
layout, monospace terminal look) is easier to get exactly right in
plain HTML/CSS than through a widget framework's own styling API.

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
whole file -- the frontend polls /api/status every ~30s for up to a
full 48h test (up to ~5760 polls), so "cheap per call" matters here in
a way it wouldn't for a one-shot script.

No result is ever cached beyond one request (only the ClickHouse
*connection object* is cached, via `_client()`'s module-level
singleton) -- every number in a response is from a query issued during
that request, so a 30s-old number in the browser is real staleness in
the underlying data, never a stale cache serving an old query result.

Launch: `make discovery-dashboard` (port 8501, same reachability
convention as the ClickHouse Play UI, and the same port the old
Streamlit version used), or directly:
    discovery/.venv/bin/uvicorn discovery.ops_dashboard_server:app --host 0.0.0.0 --port 8501
"""
from __future__ import annotations

import datetime
import pathlib
import re
import sys
from dataclasses import dataclass
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

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

STATIC_DIR = pathlib.Path(__file__).resolve().parent / "static"

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
# rationale as ALL_SOURCES above -- used for the sources table.
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
# metric this endpoint returns (error/warning counts, the visible log
# tail, cycle-duration history, the getMultipleAccounts RPC-error
# counter, Meteora's cursor position -- see fetch_log_metrics, one
# tail read serves all of them). Meteora's own ghost-pool parse_skip
# WARN volume dominates this log's line count -- confirmed live, just
# 2 consecutive cycles produced ~67k lines end to end (grep'd
# scheduler_start-to-scheduler_start on a real run), so a smaller
# window can undershoot "one recent run" by a wide margin, not just in
# some hypothetical worst case. 150_000 lines (~15MB, ~230 chunked
# 64KB reads via _tail_lines -- still O(max_lines), never O(file
# size), see that function's docstring) comfortably covers several
# cycles/~30-45min of typical runtime, while staying fast enough for
# an endpoint polled every 30s. Beyond this window, the RPC-error
# count is honestly labeled as a lower bound for the scanned window
# rather than silently understated as "since start" -- see
# fetch_log_metrics.
LOG_SCAN_LINES = 150_000
CYCLE_DURATIONS_SHOWN = 12  # sparkline width -- the agreed design asks for "last 12"

# Cap on how many (non-parse_skip) log lines the API returns for the
# visible scrollable panel, independent of LOG_SCAN_LINES (the much
# larger window error_count/warning_count/the RPC counter are computed
# over) -- a ~140px scroll box only ever shows a handful of lines at
# once, so a few hundred is already many screens' worth of scrolling,
# no reason to hand the browser thousands of rows for lines nobody
# will scroll to.
LOG_DISPLAY_LINES = 300

# logger.py's format: "HH:MM:SS.mmm  LEVEL  SRC  key=value ...". LEVEL
# is one of "INFO ", "WARN ", "ERROR" (Level enum values, all 5 chars)
# -- \s+ splitting collapses the format string's literal double spaces,
# so this matches regardless of that padding. 4 groups (ts, level, src,
# message), one more than the original Streamlit version's 3-group
# version needed -- that version only ever needed the level for
# _log_level(); the JSON API needs to hand the frontend structured
# {ts, src, level, message} per line, so ts is captured here too.
LOG_LINE_RE = re.compile(r"^(\S+)\s+(\S+)\s+(\S+)\s+(.*)$")
CYCLE_ELAPSED_RE = re.compile(r"elapsed_s=([\d.]+)")
METEORA_CURSOR_RE = re.compile(r"start_page=(\d+).*end_page=(\d+).*total_pages=(\d+)")

# hot_reason -> color for the Tier 2 breakdown table, handed to the
# frontend so the color mapping lives in exactly one place (here) and
# dashboard.js just renders whatever color string it's given, rather
# than duplicating this mapping in JS too.
HOT_REASON_COLOR = {
    HOT_REASON_NEW_POOL: "#58a6ff",  # blue
    HOT_REASON_VOLUME_SPIKE: "#d29922",  # amber
    HOT_REASON_TVL_SPIKE: "#bc8cff",  # purple -- the "distinct fourth color"
    HOT_REASON_NEW_LISTING: "#3fb950",  # green
}


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


_client_singleton = None


def _client():
    """Module-level singleton, the FastAPI/uvicorn equivalent of the
    Streamlit version's `@st.cache_resource` -- created once (on this
    process's first request) and reused across every subsequent
    request, so this dashboard doesn't open a fresh ClickHouse
    connection on every ~30s poll. Caches the *connection* only, never
    any query result (see module docstring) -- uvicorn runs this
    single-worker by default (no Makefile flag adds `--workers`), so a
    plain module-level global is safe here without a lock.
    """
    global _client_singleton
    if _client_singleton is None:
        _client_singleton = get_client()
    return _client_singleton


# --- Data access (all read-only SELECTs) ------------------------------


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
    CEX alike) shares one snapshot_ts per Tier 1 cycle, those same two
    values are valid reference points for checking DEX venues' row
    counts too (fetch_source_row_counts_for_ts) -- any DEX rows from
    an intervening Tier 2 poll simply won't match either exact value
    and are correctly excluded.

    A useful side effect: because CEX is always the last group fetched
    each cycle (see scheduler.py's DEX-then-CEX order), a snapshot_ts
    only ever appears in `cex_symbol_snapshots` once that whole cycle
    has actually finished -- so `current_ts` here is always a
    genuinely completed cycle, never one still in progress. Either or
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

    This is the real "is source X still working" signal: scheduler.py's
    per-source failure isolation (_run_dex_source/_run_cex_source) means
    an exception during a source's fetch is caught and turned into `[]`,
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
    fresh 48h test starts from empty tables (this is a cheap MergeTree
    min, not a full scan).
    """
    candidates = []
    for table in ("dex_pool_snapshots", "cex_symbol_snapshots"):
        r = client.query(f"SELECT min(snapshot_ts) FROM {table}")
        ts = r.result_rows[0][0] if r.result_rows else None
        if ts is not None:
            candidates.append(_as_utc(ts))
    return min(candidates) if candidates else None


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
    round" the way a now-vs-snapshot_ts metric could be misread --
    that cycle is over, and this source produced nothing in it.

    Still deliberately a 3-state, not 2-state, check, though: a single
    completed cycle with 0 rows could be a one-off transient blip
    (a timeout, a momentary API hiccup) rather than the source being
    actually broken -- scheduler.py's own per-source failure isolation
    treats exactly this as expected/recoverable, not an emergency (see
    _run_dex_source/_run_cex_source's docstrings). "pending" names
    that "failed once, watching" state explicitly rather than either
    false-alarming on a single blip or silently calling it "ok".

    Only 0 rows at BOTH `current_ts` and `previous_ts` -- two
    consecutive COMPLETED cycles, not two dashboard polls -- counts
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
    loading the whole file -- this endpoint is polled every ~30s for up
    to 48h (~5760 reads), and the log only grows over that window, so
    this must stay O(max_lines), not O(file size). Reads backward in
    chunk_size-byte blocks from EOF until enough newlines are seen or
    the start of the file is reached -- same idea as `tail -n`, just
    without shelling out.
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


def _log_level(line: str) -> Optional[str]:
    m = LOG_LINE_RE.match(line)
    return m.group(2) if m else None


def _parse_log_line(line: str) -> dict[str, str]:
    """Splits one raw log line into {ts, src, level, message} for the
    JSON API -- the frontend renders these fields directly rather than
    re-parsing the raw line itself.
    """
    m = LOG_LINE_RE.match(line)
    if not m:
        return {"ts": "", "src": "", "level": "", "message": line}
    ts, level, src, message = m.groups()
    return {"ts": ts, "level": level.strip(), "src": src, "message": message}


# event=parse_skip is excluded from the visible log panel and its
# error/warning chips (though NOT from anything else this endpoint
# computes -- the RPC-error counter and cycle-duration history don't
# look at level or event type at all). This is a presentational
# judgment call, not a hidden one: parse_skip is Meteora's documented,
# expected ghost-pool-filtering noise (see meteora_dlmm.py's module
# docstring) at a volume (thousands of lines per cycle while Meteora's
# slice runs) that would otherwise swamp both the WARN count and the
# visible tail with the same well-understood, harmless line repeated
# thousands of times -- defeating the log panel's whole purpose ("an
# instant read on whether anything's wrong before scanning lines"),
# since that count would read in the thousands during ~1/3 of all
# cycles regardless of whether anything is actually wrong. A genuine
# parse failure elsewhere (Orca's own now-fixed tvlUsdc case, for
# instance) would still show here -- this excludes one specific,
# named, expected event type, not "WARN lines in general".
_NOISY_EVENTS = ("event=parse_skip",)


def _is_noisy_line(line: str) -> bool:
    return any(marker in line for marker in _NOISY_EVENTS)


@dataclass
class LogMetrics:
    """Everything this endpoint derives from the scheduler log, in one
    place -- see fetch_log_metrics.
    """

    display_lines: list[str]  # chronological (oldest first); excludes _NOISY_EVENTS
    error_count: int  # within the scanned window, excludes _NOISY_EVENTS
    warning_count: int  # within the scanned window, excludes _NOISY_EVENTS
    failure_reason: Optional[str]
    cycle_durations_s: list[float]
    rpc_error_count: int
    rpc_error_count_is_lower_bound: bool
    meteora_cursor: Optional[str]


def fetch_log_metrics(log_path: pathlib.Path) -> LogMetrics:
    """One tail read serves every log-derived metric this endpoint
    returns -- the visible log panel and its error/warning counts,
    per-cycle duration history (for the sparkline), the
    getMultipleAccounts RPC-error counter, and Meteora's current
    cursor position -- rather than re-reading the log once per metric.
    Still only ever reads backward from EOF (see _tail_lines), never
    the whole file, even across a full 48h run.

    `failure_reason` set (log missing/unreadable) means every other
    field is a placeholder empty/zero value, NOT "confirmed clean" --
    the API must not conflate "nothing found" with "couldn't look".
    """
    empty = LogMetrics([], 0, 0, None, [], 0, False, None)
    if not log_path.exists():
        return LogMetrics([], 0, 0, f"log file not found at {log_path} -- has the scheduler ever been started?", [], 0, False, None)
    try:
        lines = _tail_lines(log_path, LOG_SCAN_LINES)
    except OSError as exc:
        return LogMetrics([], 0, 0, f"failed to read log file: {exc}", [], 0, False, None)
    if not lines:
        return empty

    display_lines: list[str] = []
    error_count = 0
    warning_count = 0
    for ln in lines:
        if _is_noisy_line(ln):
            continue
        level = _log_level(ln)
        if level == "ERROR":
            error_count += 1
        elif level == "WARN":
            warning_count += 1
        display_lines.append(ln)
    display_lines = display_lines[-LOG_DISPLAY_LINES:]

    # cycle_durations_s intentionally reads the FULL scanned window,
    # not scoped to the current run -- a stability trend line is more
    # useful spanning a recent restart than artificially truncated to
    # "since start", unlike the RPC-error counter below (explicitly
    # scoped to "since the scheduler's current start").
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

    return LogMetrics(
        display_lines, error_count, warning_count, None, cycle_durations_s, rpc_error_count, is_lower_bound, meteora_cursor
    )


# --- JSON assembly -----------------------------------------------------


def build_status_payload() -> dict:
    """Runs every query above once and assembles the single JSON
    payload GET /api/status returns. Never raises -- a query failure
    (ClickHouse down, log unreadable) is caught and surfaced as an
    `"error"` field in the payload rather than an HTTP 500, so the
    frontend can degrade to a visible message instead of a blank page
    (same "must degrade visibly, never crash silently" contract the
    Streamlit version's own try/except had).
    """
    try:
        client = _client()
        now = datetime.datetime.now(datetime.timezone.utc)
        test_start = fetch_test_start(client)
        previous_ts, current_ts = fetch_last_two_snapshot_ts(client)
        cycle_row_counts = fetch_source_row_counts_for_ts(
            client, [ts for ts in (previous_ts, current_ts) if ts is not None]
        )
        tier2_summary = fetch_tier2_summary(client)
        new_entity_count = fetch_new_entity_count_since(client, test_start)
        log_metrics = fetch_log_metrics(SCHED_LOG_PATH)
    except Exception as exc:  # noqa: BLE001 -- must degrade visibly, never 500 the frontend into a blank page
        return {"error": f"Failed to query ClickHouse: {exc}"}

    level, reason, status_by_source = compute_health_from_row_counts(cycle_row_counts, previous_ts, current_ts)

    # Display severity: ONE 3-state value the frontend uses for BOTH the
    # status strip's tint and its label word, so the two can never
    # disagree. This replaces an earlier binary `any_problem` flag
    # (true on any non-"ok" status) that drove only the tint while the
    # label word came from `level` -- two different scales on one
    # widget, which is exactly why the strip could read "GREEN" on an
    # amber background whenever a source was merely "pending".
    #
    # Purely presentational, derived from the already-computed
    # status_by_source/level -- source_status() and
    # compute_health_from_row_counts() are untouched, and `level`/
    # `reason` are still returned below unchanged for any consumer that
    # wants the underlying health verdict rather than this display
    # mapping. The mapping is the one the dashboard's design calls for:
    # green when everything is ok, amber when something is pending
    # (a single missed cycle -- being watched, not yet confirmed
    # broken), red when something is confirmed STOPPED. `level == "red"`
    # is folded into red as well, since that's the "no cycle has ever
    # written data" case, where every status is a vacuous "pending"
    # that must not read as a mild amber.
    stopped_tags = [SRC_TAG[s] for s in ALL_SOURCES if status_by_source.get(s) == "STOPPED"]
    pending_tags = [SRC_TAG[s] for s in ALL_SOURCES if status_by_source.get(s) == "pending"]
    if level == "red" or stopped_tags:
        severity = "red"
        summary = reason  # already names the stopped sources
    elif pending_tags:
        severity = "amber"
        summary = (
            f"{len(pending_tags)}/{len(ALL_SOURCES)} pending "
            f"(0 rows in the last completed cycle, watching): " + ", ".join(pending_tags)
        )
    else:
        severity = "green"
        summary = reason

    cycle_age_min = (now - current_ts).total_seconds() / 60 if current_ts is not None else None
    if test_start is not None:
        uptime = now - test_start
        uptime_str = f"{uptime.days}d{uptime.seconds // 3600:02d}h{(uptime.seconds % 3600) // 60:02d}m"
    else:
        uptime_str = None

    sources = []
    for kind, venue_list in (("DEX", DEX_VENUES), ("CEX", CEX_EXCHANGES)):
        for src in venue_list:
            sources.append(
                {
                    "src": src,
                    "tag": SRC_TAG[src],
                    "kind": kind,
                    "status": status_by_source.get(src, "pending"),
                    "rows_this_cycle": cycle_row_counts.get(src, {}).get(current_ts, 0) if current_ts else 0,
                }
            )

    hot_counts = {reason_key: 0 for reason_key in HOT_REASON_COLOR}
    for tier, hot_reason, pool_count in tier2_summary:
        if tier == "hot" and hot_reason in hot_counts:
            hot_counts[hot_reason] = pool_count
    tier2_rows = [
        {"reason": reason_key, "count": count, "color": HOT_REASON_COLOR[reason_key]}
        for reason_key, count in hot_counts.items()
    ]

    return {
        "health": {"level": level, "reason": reason, "severity": severity, "summary": summary},
        "cycle_age_min": cycle_age_min,
        "uptime": uptime_str,
        "new_entity_count": new_entity_count,
        "rpc_error_count": log_metrics.rpc_error_count,
        "rpc_error_count_is_lower_bound": log_metrics.rpc_error_count_is_lower_bound,
        "meteora_cursor": log_metrics.meteora_cursor,
        "sources": sources,
        "tier2": {"rows": tier2_rows, "total": sum(hot_counts.values())},
        "cycle_durations_s": log_metrics.cycle_durations_s,
        "log": {
            "error_count": log_metrics.error_count,
            "warning_count": log_metrics.warning_count,
            "failure_reason": log_metrics.failure_reason,
            "scan_lines": LOG_SCAN_LINES,
            # most recent first, matching the log panel's display order
            "lines": [_parse_log_line(ln) for ln in reversed(log_metrics.display_lines)],
        },
    }


# --- FastAPI app ---------------------------------------------------------

app = FastAPI(title="ZORA Discovery Ops")


@app.get("/api/status")
def api_status() -> JSONResponse:
    return JSONResponse(build_status_payload())


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "dashboard.html")


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8501)
