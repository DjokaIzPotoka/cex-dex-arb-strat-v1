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
from typing import Optional

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from discovery.src.db import get_client  # noqa: E402

REFRESH_INTERVAL_MS = 30_000
STALENESS_THRESHOLD_MIN = 10

# Matches scheduler.py's DEX_SRC/CEX_SRC venue/exchange keys exactly --
# duplicated here rather than imported, deliberately: importing from
# scheduler.py would pull in that module's own provider construction
# (OrcaWhirlpoolRpcStateProvider et al., needing SOLANA_RPC_URL) just
# to read two dict literals, for a dashboard that must never construct
# or touch a live provider/RPC connection at all.
DEX_VENUES = ["orca_whirlpool", "raydium_amm_v4", "raydium_clmm", "meteora_dlmm"]
CEX_EXCHANGES = ["binance", "bybit", "okx", "bitget", "gateio"]
ALL_SOURCES = DEX_VENUES + CEX_EXCHANGES

# scripts/zora_discovery.sh's SCHED_LOG path -- mirrored, not derived
# (that script is bash, this is Python; there's no shared config file
# either reads from).
SCHED_LOG_PATH = pathlib.Path(__file__).resolve().parents[1] / "logs" / "zora_discovery_sched.log"

# How far back (lines) to scan the log for ERROR lines vs. how many of
# those are actually shown -- scanning more than we show means a run
# with occasional errors separated by long clean stretches still finds
# its last ~20, not just whatever happens to be in a too-small window.
LOG_SCAN_LINES = 5_000
LOG_ERRORS_SHOWN = 20

# logger.py's format: "HH:MM:SS.mmm  LEVEL  SRC  key=value ...". LEVEL
# is one of "INFO ", "WARN ", "ERROR" (Level enum values, all 5 chars)
# -- \s+ splitting collapses the format string's literal double spaces,
# so this matches regardless of that padding.
LOG_LINE_RE = re.compile(r"^\S+\s+(\S+)\s+(\S+)\s+(.*)$")


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
    source has never written a row at all (distinct from "stale" --
    surfaced separately in the health badge).
    """
    result: dict[str, Optional[datetime.datetime]] = {src: None for src in ALL_SOURCES}
    dex = client.query("SELECT venue, max(snapshot_ts) FROM dex_pool_snapshots GROUP BY venue")
    for venue, ts in dex.result_rows:
        result[venue] = _as_utc(ts)
    cex = client.query("SELECT exchange, max(snapshot_ts) FROM cex_symbol_snapshots GROUP BY exchange")
    for exchange, ts in cex.result_rows:
        result[exchange] = _as_utc(ts)
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


def compute_health(
    freshness: dict[str, Optional[datetime.datetime]], now: datetime.datetime
) -> tuple[str, str]:
    """Returns (level, reason) where level is 'green'/'yellow'/'red'.
    green: all 9 sources fresh. red: zero sources fresh (or nothing has
    ever been written). yellow: everything in between -- partial
    degradation, named explicitly in the reason string rather than
    just "some sources stale".
    """
    problems = []
    for src in ALL_SOURCES:
        ts = freshness.get(src)
        if ts is None:
            problems.append(f"{src}: no data")
        else:
            age_min = (now - ts).total_seconds() / 60
            if age_min > STALENESS_THRESHOLD_MIN:
                problems.append(f"{src}: {age_min:.1f}m stale")
    total = len(ALL_SOURCES)
    if not problems:
        return "green", f"all {total}/{total} sources have a row within the last {STALENESS_THRESHOLD_MIN} minutes"
    if len(problems) == total:
        return "red", "no source has fresh data -- " + "; ".join(problems)
    return "yellow", f"{len(problems)}/{total} sources stale or missing -- " + "; ".join(problems)


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


def fetch_recent_errors(log_path: pathlib.Path) -> tuple[Optional[list[str]], Optional[str]]:
    """Returns (error_lines, failure_reason) -- exactly one of the two
    is non-None/non-empty-meaningful: a populated failure_reason means
    the log itself couldn't be read at all (missing file, permission
    error), NOT "no errors found" -- those are different states and
    the UI must not conflate them (never fabricate a status either
    way, per this session's own instruction).
    """
    if not log_path.exists():
        return None, f"log file not found at {log_path} -- has the scheduler ever been started?"
    try:
        lines = _tail_lines(log_path, LOG_SCAN_LINES)
    except OSError as exc:
        return None, f"failed to read log file: {exc}"
    error_lines = [ln for ln in lines if _is_error_line(ln)]
    return error_lines[-LOG_ERRORS_SHOWN:], None


# --- UI ----------------------------------------------------------------


def main() -> None:
    st.set_page_config(page_title="ZORA Discovery Ops", layout="wide")
    st_autorefresh(interval=REFRESH_INTERVAL_MS, key="autorefresh")

    st.title("ZORA MEV Discovery -- Ops Dashboard")
    st.caption(
        "Read-only. Auto-refreshes every "
        f"{REFRESH_INTERVAL_MS // 1000}s. Does not control the scheduler -- "
        "use `make discovery-attach`/`discovery-stop` for that."
    )

    try:
        client = _client()
        now = datetime.datetime.now(datetime.timezone.utc)
        freshness = fetch_last_snapshot_ts(client)
        test_start = fetch_test_start(client)
        row_counts = fetch_row_counts_since(client, test_start)
        tier2_summary = fetch_tier2_summary(client)
        new_entity_count = fetch_new_entity_count_since(client, test_start)
        recent_new_entities = fetch_recent_new_entities(client)
    except Exception as exc:  # noqa: BLE001 -- a dashboard must degrade to a visible error, never crash silently
        st.error(f"Failed to query ClickHouse: {exc}")
        st.stop()

    # --- (a) health badge ---
    level, reason = compute_health(freshness, now)
    badge = {"green": st.success, "yellow": st.warning, "red": st.error}[level]
    badge(f"**{level.upper()}** -- {reason}")

    # --- (b) uptime ---
    col1, col2, col3 = st.columns(3)
    if test_start is not None:
        uptime = now - test_start
        col1.metric("Test uptime", f"{uptime.days}d {uptime.seconds // 3600}h {(uptime.seconds % 3600) // 60}m")
        col1.caption(f"since {test_start.isoformat()} (min snapshot_ts across both tables)")
    else:
        col1.metric("Test uptime", "n/a")
        col1.caption("no rows in dex_pool_snapshots or cex_symbol_snapshots yet")
    col2.metric("new_entity_events since test start", new_entity_count)
    fresh_count = sum(
        1 for src in ALL_SOURCES if freshness.get(src) and (now - freshness[src]) <= datetime.timedelta(minutes=STALENESS_THRESHOLD_MIN)
    )
    col3.metric("Sources fresh", f"{fresh_count}/{len(ALL_SOURCES)}")

    # --- (c) per-source table ---
    st.subheader("Per-source freshness")
    rows = []
    for kind, sources in (("DEX", DEX_VENUES), ("CEX", CEX_EXCHANGES)):
        for src in sources:
            ts = freshness.get(src)
            if ts is None:
                staleness_min = None
                status = "NO DATA"
            else:
                staleness_min = (now - ts).total_seconds() / 60
                status = "fresh" if staleness_min <= STALENESS_THRESHOLD_MIN else "STALE"
            rows.append(
                {
                    "source": src,
                    "kind": kind,
                    "last_snapshot_ts (UTC)": ts.isoformat(sep=" ", timespec="seconds") if ts else "never",
                    "staleness (min)": f"{staleness_min:.1f}" if staleness_min is not None else "n/a",
                    "rows since test start": row_counts.get(src, 0),
                    "status": status,
                }
            )
    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

    # --- (d) Tier 2 summary ---
    st.subheader("Tier 2 summary (current tier/hot_reason, latest row per pool)")
    if tier2_summary:
        st.dataframe(
            pd.DataFrame(tier2_summary, columns=["tier", "hot_reason", "pool_count"]),
            hide_index=True,
            width="stretch",
        )
    else:
        st.info("no rows in dex_pool_snapshots yet")

    # --- (e) new_entity_events ---
    st.subheader("Recent new_entity_events (10 most recent overall)")
    if recent_new_entities:
        df = pd.DataFrame(recent_new_entities, columns=["entity_type", "venue", "identifier", "detected_ts"])
        st.dataframe(df, hide_index=True, width="stretch")
    else:
        st.info("no rows in new_entity_events yet")

    # --- (f) recent problems (log tail) ---
    st.subheader(f"Recent problems (last {LOG_ERRORS_SHOWN} ERROR lines, scanning last {LOG_SCAN_LINES} log lines)")
    error_lines, failure_reason = fetch_recent_errors(SCHED_LOG_PATH)
    if failure_reason is not None:
        st.warning(failure_reason)
    elif not error_lines:
        st.success("no problems in visible log window")
    else:
        st.code("\n".join(reversed(error_lines)), language=None)
        st.caption(f"showing {len(error_lines)} line(s), most recent first, from {SCHED_LOG_PATH}")


if __name__ == "__main__":
    main()
