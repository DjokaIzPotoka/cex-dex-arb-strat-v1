#!/usr/bin/env python3
"""Tier 1 discovery scheduler, per docs/mev-discovery-plan.md section
3.2: continuous 5-minute cycles across the full DEX+CEX universe (all
9 sources, no filtering beyond section 3.7's "obvious ghosts only"),
each cycle followed by entity detection (section 3.6) on that cycle's
own fresh data. This is Phase 1e's scope -- Tier 2 promotion/demotion
(age-based/growth-based hot pools), proactive rate limiting on the DEX
side, and the RPC-based state provider are explicitly out of scope
here (Phase 1f/1g/1h, per this session's task boundary).

Design choices worth calling out:

- Sequential execution, not concurrent (stronger than section 5.4's
  general spirit): DEX providers don't have proactive token-bucket
  rate limiting yet -- only the 5 CEX providers do (from their own
  earlier sessions). Running all 9 sources sequentially is the safe
  default until Phase 1f retrofits uniform rate limiting; concurrent
  execution today could fire an unthrottled burst at a DEX API with no
  client-side guard at all.

- Per-source failure isolation (section 5.4 point 5): each of the 9
  sources runs inside its own try/except (_run_dex_source /
  _run_cex_source below). A single source's exception (timeout, API
  error, parse failure) is logged and the cycle moves on to the next
  source -- it never aborts the whole cycle. A source that raised
  contributes zero rows to that cycle and is simply skipped for
  detection this cycle (see detect_new_entities' empty-candidates
  no-op in detection.py) -- its existing known_entities/
  new_entity_events history for that venue/exchange is untouched, not
  corrupted or reset by one bad cycle.

- Fixed-rate scheduling, not fixed-delay: the next cycle is scheduled
  CYCLE_INTERVAL_S after the *start* of the current one, not
  CYCLE_INTERVAL_S after it *ends*. This matters concretely here: a
  full unbounded broad pull across all 4 DEX venues empirically takes
  several minutes on its own (observed live during Phase 1d's seed:
  Orca ~60s, Raydium AMM v4 ~103s, Raydium CLMM ~4s, Meteora ~110s --
  roughly 4.6 minutes for DEX alone, before CEX and detection). If a
  cycle runs long, the next one starts immediately (with a WARN log
  noting the overrun) instead of sleeping and drifting the schedule
  further behind on every subsequent cycle.

- Detection runs against *this cycle's own* rows, passed in directly
  from the fetch step -- not a "whatever's currently newest snapshot_ts"
  query the way the one-off run_entity_detection.py had to do (that
  script predates this scheduler and had no other way to know what
  counted as "one cycle"). The scheduler already knows precisely which
  rows belong to the cycle it just ran, so no extra query is needed to
  rediscover that.

- Per-source SRC tags on this module's own log lines (table below),
  distinct from the shared "DSC" every provider/one-shot-script log
  call still defaults to (see logger.py's docstring). This process is
  the first place in the codebase where one long-lived process
  interleaves all 9 sources' activity in one stdout stream, so telling
  them apart by SRC (not just by an `exchange=`/`venue=` field buried
  in the message) matters here in a way it didn't for the one-shot
  scripts, which only ever logged about a single source at a time.
  Provider-internal log calls (e.g. "exchange_info_fetched") are left
  at their existing default "DSC" -- rewriting those call sites across
  9 provider files for this alone wasn't worth the churn; this
  module's own start/done/failed wrapper lines around each source
  already bracket them with a distinguishing SRC on either side.

Not a service manager: this process just loops on its own thread until
killed (SIGTERM/SIGINT, handled gracefully below). Process supervision
(start/stop/status, tmux integration) lives in scripts/zora_discovery.sh
and the Makefile's discovery-start/stop/status/attach targets, mirroring
scripts/zora.sh's pattern for ZORA's existing paper-trading pipeline.

Usage:
    python3 discovery/src/scheduler.py
"""
from __future__ import annotations

import datetime
import pathlib
import signal
import sys
import time
import traceback

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from discovery.src.db import get_client  # noqa: E402
from discovery.src.detection import (  # noqa: E402
    ENTITY_TYPE_CEX_SYMBOL,
    ENTITY_TYPE_DEX_POOL,
    build_cex_candidate,
    build_dex_candidate,
    detect_new_entities,
)
from discovery.src.logger import Level, log  # noqa: E402
from discovery.src.providers.binance_symbols import BinanceSymbolProvider  # noqa: E402
from discovery.src.providers.bitget_symbols import BitgetSymbolProvider  # noqa: E402
from discovery.src.providers.bybit_symbols import BybitSymbolProvider  # noqa: E402
from discovery.src.providers.gateio_symbols import GateioSymbolProvider  # noqa: E402
from discovery.src.providers.meteora_dlmm import MeteoraDlmmRestProvider  # noqa: E402
from discovery.src.providers.okx_symbols import OkxSymbolProvider  # noqa: E402
from discovery.src.providers.orca_rest import OrcaRestProvider  # noqa: E402
from discovery.src.providers.raydium_amm_v4 import RaydiumAmmV4RestProvider  # noqa: E402
from discovery.src.providers.raydium_clmm import RaydiumClmmRestProvider  # noqa: E402

CYCLE_INTERVAL_S = 300.0

# Per-source SRC tags, 3 chars to match ZORA's existing CEX/DEX/SPR
# convention. SCH = the scheduler's own orchestration lines (cycle
# start/done, sleeping, overrun warnings); DET = detection step
# summaries.
DEX_SRC = {
    "orca_whirlpool": "ORC",
    "raydium_amm_v4": "RA4",
    "raydium_clmm": "RCL",
    "meteora_dlmm": "MET",
}
CEX_SRC = {
    "binance": "BNC",
    "bybit": "BYB",
    "okx": "OKX",
    "bitget": "BTG",
    "gateio": "GTE",
}
DETECTION_SRC = "DET"
SCHEDULER_SRC = "SCH"

_shutdown_requested = False


def _handle_shutdown(signum, _frame) -> None:
    global _shutdown_requested
    _shutdown_requested = True
    log(Level.INFO, [("event", "shutdown_signal_received"), ("signum", str(signum))], src=SCHEDULER_SRC)


def _build_dex_providers() -> dict[str, object]:
    return {
        "orca_whirlpool": OrcaRestProvider(),
        "raydium_amm_v4": RaydiumAmmV4RestProvider(),
        "raydium_clmm": RaydiumClmmRestProvider(),
        "meteora_dlmm": MeteoraDlmmRestProvider(),
    }


def _build_cex_providers() -> dict[str, object]:
    return {
        "binance": BinanceSymbolProvider(),
        "bybit": BybitSymbolProvider(),
        "okx": OkxSymbolProvider(),
        "bitget": BitgetSymbolProvider(),
        "gateio": GateioSymbolProvider(),
    }


def _run_dex_source(venue: str, provider, snapshot_ts: datetime.datetime) -> list[dict]:
    """Full unbounded broad pull for one DEX venue (Tier 1 -- "ceo DEX
    univerzum, bez izuzetka", section 3.2), dropping zero-24h-volume
    ghosts per section 3.7, same filter every run_*_snapshot.py script
    already applies. Never raises: any exception from the provider
    (network, parse, API error) is caught, logged, and turned into an
    empty result for this cycle -- see this module's docstring on
    per-source failure isolation.
    """
    src = DEX_SRC[venue]
    t0 = time.monotonic()
    log(Level.INFO, [("event", "source_start"), ("venue", venue)], src=src)
    try:
        rows: list[dict] = []
        kept, dropped_ghost = 0, 0
        for pool_row in provider.list_pools():
            if pool_row.volume_24h_usd <= 0:
                dropped_ghost += 1
                continue
            rows.append(pool_row.to_snapshot_row(snapshot_ts=snapshot_ts, tier="discovery", hot_reason=""))
            kept += 1
        elapsed = time.monotonic() - t0
        log(
            Level.INFO,
            [
                ("event", "source_done"),
                ("venue", venue),
                ("kept", str(kept)),
                ("dropped_ghost", str(dropped_ghost)),
                ("elapsed_s", f"{elapsed:.1f}"),
            ],
            src=src,
        )
        return rows
    except Exception as exc:  # noqa: BLE001 -- deliberately broad, see module docstring
        elapsed = time.monotonic() - t0
        log(
            Level.ERROR,
            [
                ("event", "source_FAILED"),
                ("venue", venue),
                ("elapsed_s", f"{elapsed:.1f}"),
                ("err", str(exc)),
            ],
            src=src,
        )
        return []


def _run_cex_source(exchange: str, provider, snapshot_ts: datetime.datetime) -> list[dict]:
    """One exchangeInfo-shaped call for one CEX exchange, per section
    3.1 -- no filtering (section 3.7's "widen, don't filter" applies
    as-is on the CEX side, same as every run_*_snapshot.py CEX script).
    Never raises, same failure-isolation contract as _run_dex_source.
    """
    src = CEX_SRC[exchange]
    t0 = time.monotonic()
    log(Level.INFO, [("event", "source_start"), ("exchange", exchange)], src=src)
    try:
        symbols = provider.fetch_symbols()
        rows = [s.to_snapshot_row(snapshot_ts) for s in symbols]
        elapsed = time.monotonic() - t0
        log(
            Level.INFO,
            [("event", "source_done"), ("exchange", exchange), ("rows", str(len(rows))), ("elapsed_s", f"{elapsed:.1f}")],
            src=src,
        )
        return rows
    except Exception as exc:  # noqa: BLE001 -- deliberately broad, see module docstring
        elapsed = time.monotonic() - t0
        log(
            Level.ERROR,
            [
                ("event", "source_FAILED"),
                ("exchange", exchange),
                ("elapsed_s", f"{elapsed:.1f}"),
                ("err", str(exc)),
            ],
            src=src,
        )
        return []


def _write_rows(client, table: str, rows: list[dict], src: str) -> None:
    """Writes immediately after each source's own fetch completes
    (called right after _run_dex_source/_run_cex_source below), not
    batched to the end of the cycle -- so an earlier source's
    successfully-fetched data is already durable before a later
    source in the same cycle gets a chance to fail.
    """
    if not rows:
        return
    columns = list(rows[0].keys())
    data = [[row[c] for c in columns] for row in rows]
    client.insert(table, data, column_names=columns)
    log(Level.INFO, [("event", "clickhouse_insert"), ("table", table), ("rows", str(len(rows)))], src=src)


def run_cycle(client) -> float:
    """Runs one full Tier 1 cycle: all 4 DEX venues, then all 5 CEX
    exchanges (sequential -- see module docstring), writing each
    source's rows to ClickHouse immediately, then one detect_new_entities
    call per venue/exchange against exactly this cycle's own rows.
    Returns the cycle's wall-clock duration in seconds.
    """
    cycle_start = time.monotonic()
    snapshot_ts = datetime.datetime.now(datetime.timezone.utc)
    log(Level.INFO, [("event", "cycle_start"), ("snapshot_ts", snapshot_ts.isoformat())], src=SCHEDULER_SRC)

    dex_rows_by_venue: dict[str, list[dict]] = {}
    for venue, provider in _build_dex_providers().items():
        rows = _run_dex_source(venue, provider, snapshot_ts)
        dex_rows_by_venue[venue] = rows
        _write_rows(client, "dex_pool_snapshots", rows, DEX_SRC[venue])

    cex_rows_by_exchange: dict[str, list[dict]] = {}
    for exchange, provider in _build_cex_providers().items():
        rows = _run_cex_source(exchange, provider, snapshot_ts)
        cex_rows_by_exchange[exchange] = rows
        _write_rows(client, "cex_symbol_snapshots", rows, CEX_SRC[exchange])

    total_checked, total_new = 0, 0
    for venue, rows in dex_rows_by_venue.items():
        if not rows:
            # Source failed or (very unlikely) returned nothing real
            # this cycle -- nothing to check, and detect_new_entities'
            # empty-candidates no-op means known_entities/
            # new_entity_events for this venue are simply untouched
            # this cycle, not reset or corrupted.
            continue
        candidates = [build_dex_candidate(r) for r in rows]
        result = detect_new_entities(client, ENTITY_TYPE_DEX_POOL, venue, candidates, snapshot_ts)
        log(
            Level.INFO,
            [("event", "detection"), ("venue", venue), ("checked", str(result.checked)), ("new", str(result.new_count))],
            src=DETECTION_SRC,
        )
        total_checked += result.checked
        total_new += result.new_count

    for exchange, rows in cex_rows_by_exchange.items():
        if not rows:
            continue
        candidates = [build_cex_candidate(r) for r in rows]
        result = detect_new_entities(client, ENTITY_TYPE_CEX_SYMBOL, exchange, candidates, snapshot_ts)
        log(
            Level.INFO,
            [("event", "detection"), ("exchange", exchange), ("checked", str(result.checked)), ("new", str(result.new_count))],
            src=DETECTION_SRC,
        )
        total_checked += result.checked
        total_new += result.new_count

    elapsed = time.monotonic() - cycle_start
    log(
        Level.INFO,
        [
            ("event", "cycle_done"),
            ("elapsed_s", f"{elapsed:.1f}"),
            ("total_checked", str(total_checked)),
            ("total_new", str(total_new)),
        ],
        src=SCHEDULER_SRC,
    )
    return elapsed


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    client = get_client()
    log(
        Level.INFO,
        [("event", "scheduler_start"), ("cycle_interval_s", str(CYCLE_INTERVAL_S))],
        src=SCHEDULER_SRC,
    )

    while not _shutdown_requested:
        try:
            elapsed = run_cycle(client)
        except Exception:  # noqa: BLE001 -- last-resort guard, see comment below
            # Should be unreachable: every source and the detection
            # call already catch their own exceptions. But a scheduler-
            # level bug here must never silently kill the whole
            # process -- log the full traceback and keep the loop
            # alive rather than dying on something genuinely
            # unanticipated.
            log(Level.ERROR, [("event", "cycle_CRASHED")], src=SCHEDULER_SRC)
            traceback.print_exc()
            elapsed = CYCLE_INTERVAL_S  # don't hot-loop retrying a cycle that just crashed

        if _shutdown_requested:
            break

        sleep_s = CYCLE_INTERVAL_S - elapsed
        if sleep_s <= 0:
            log(
                Level.WARN,
                [("event", "cycle_overran_interval"), ("elapsed_s", f"{elapsed:.1f}"), ("interval_s", str(CYCLE_INTERVAL_S))],
                src=SCHEDULER_SRC,
            )
            continue

        log(Level.INFO, [("event", "sleeping"), ("sleep_s", f"{sleep_s:.1f}")], src=SCHEDULER_SRC)
        # Sleep in short increments so a shutdown signal arriving
        # during the idle window is honored promptly, instead of
        # blocking in one long time.sleep() call for up to ~5 minutes.
        slept = 0.0
        while slept < sleep_s and not _shutdown_requested:
            chunk = min(1.0, sleep_s - slept)
            time.sleep(chunk)
            slept += chunk

    log(Level.INFO, [("event", "scheduler_stopped")], src=SCHEDULER_SRC)


if __name__ == "__main__":
    main()
