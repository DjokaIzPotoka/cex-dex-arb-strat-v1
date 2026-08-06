#!/usr/bin/env python3
"""Tier 1 + Tier 2 discovery scheduler, per docs/mev-discovery-plan.md
section 3.2: continuous 5-minute Tier 1 cycles across the full DEX+CEX
universe (all 9 sources, no filtering beyond section 3.7's "obvious
ghosts only"), each cycle followed by entity detection (section 3.6)
on that cycle's own fresh data, PLUS Tier 2's 1-minute re-polling of
whichever pools are currently "hot" (age-based new-pool window or
growth-based volume/TVL/listing spikes -- see discovery/src/tier2.py
for the promotion/demotion logic itself, this module only wires it
into the cycle). Originally Phase 1e's scope (Tier 1 loop); Phase 1f
(a later session) retrofitted TokenBucket rate limiting onto the 4 DEX
providers (see orca_rest.py/raydium_common.py/meteora_dlmm.py's own
"Rate limiting" docstring sections) -- all 9 sources now share the
same mechanism. Phase 1g added Tier 2 promotion/demotion and its
1-minute polling, via the same REST providers Tier 1 uses (see
tier2_poller.py's docstring for the "best-effort ~1-minute cadence,
not a hard guarantee" tradeoff this implies -- still true). Phase 1h
(this session) replaced Tier 2's REST state polling with an RPC-based
PoolStateProvider for Orca Whirlpool and Raydium CLMM specifically
(solana_rpc_state.py, batched `getMultipleAccounts`) -- see
tier2_poller.py's module docstring for the hybrid RPC-state/
REST-volume design this scheduler now gets for free via
build_tier2_providers() (this module doesn't construct Tier 2's
providers itself, so no change was needed here beyond this docstring).
Raydium AMM v4 and Meteora DLMM still go through REST for Tier 2, same
as before.

Design choices worth calling out:

- Sequential execution, not concurrent. Originally (Phase 1e) this was
  a stopgap forced by the DEX providers having no rate limiting at all
  -- concurrent execution then could fire an unthrottled burst at a
  DEX API with no client-side guard whatsoever. That specific reason
  is gone as of Phase 1f: all 9 sources now own a TokenBucket sized
  per docs/mev-discovery-plan.md section 5.1/5.4, so nothing here would
  blow through a rate limit merely by running concurrently.
  TODO(concurrency decision, not this session): sequential execution
  is still what runs today -- switching to concurrent is a deliberate
  choice deferred on purpose, not attempted here, because it needs its
  own testing (thread-safety of the provider/session objects built
  fresh per cycle, ClickHouse client concurrent-write behavior, and
  the Raydium AMM v4/CLMM same-host caveat raydium_common.py's
  docstring calls out -- two independent per-provider buckets are only
  safe *because* they're never drawn from concurrently today; running
  them concurrently would need those two providers to share one
  bucket instead). Nothing about *this* session's retrofit should be
  read as recommending the switch, only as removing the one blocker
  that made sequential execution non-optional.

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
from discovery.src.tier2 import (  # noqa: E402
    TIER2_SRC,
    Tier2State,
    apply_new_listing_promotions,
    latest_dex_pools_by_base_symbol,
)
from discovery.src.tier2_poller import TIER2_POLL_INTERVAL_S, Tier2Poller  # noqa: E402

CYCLE_INTERVAL_S = 300.0

# Fixed backoff after a scheduler-level crash (the outer try/except in
# main(), not a per-source failure -- those are isolated and never
# reach here). Deliberately not CYCLE_INTERVAL_S or 0: long enough to
# avoid hot-looping against whatever just broke, short enough to
# recover quickly once it's transient. See main()'s crash-handling
# comment for the bug this replaced.
CRASH_BACKOFF_S = 30.0

# Per-source SRC tags, 3 chars to match ZORA's existing CEX/DEX/SPR
# convention. SCH = the scheduler's own orchestration lines (cycle
# start/done, sleeping, overrun warnings); DET = detection step
# summaries. T2M (tier2.py) = Tier 2 promotion/demotion decisions;
# T2P (tier2_poller.py) = Tier 2's 1-minute poll execution -- kept
# distinct from T2M since one is "what changed" and the other is
# "what got re-fetched", useful to tell apart when grepping logs.
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


def _interruptible_sleep(seconds: float) -> None:
    """Sleeps in 1s increments, checking _shutdown_requested between
    each, so a signal arriving mid-sleep is honored within ~1s instead
    of blocking for the full duration (which matters here both for the
    normal up-to-~5-minute inter-cycle sleep and for CRASH_BACKOFF_S).
    """
    slept = 0.0
    while slept < seconds and not _shutdown_requested:
        chunk = min(1.0, seconds - slept)
        time.sleep(chunk)
        slept += chunk


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


def _run_dex_source(venue: str, provider, snapshot_ts: datetime.datetime, tier2_state: Tier2State) -> list[dict]:
    """Full unbounded broad pull for one DEX venue (Tier 1 -- "ceo DEX
    univerzum, bez izuzetka", section 3.2), dropping zero-24h-volume
    ghosts per section 3.7, same filter every run_*_snapshot.py script
    already applies. Never raises: any exception from the provider
    (network, parse, API error) is caught, logged, and turned into an
    empty result for this cycle -- see this module's docstring on
    per-source failure isolation.

    tier/hot_reason are looked up per pool from `tier2_state` (already
    pruned/settled from the *previous* cycle's decisions -- this
    cycle's own growth-based re-scoring and age-based promotions
    happen after this function returns, in run_cycle(), so they apply
    starting next observation, not retroactively to the row being
    built right now). Concretely: a pool's very first-ever row is
    always written as tier='discovery' even though it's about to be
    flagged 'new_pool' moments later by detect_new_entities() -- you
    can't know a pool is new before you've recorded it once and
    checked. That's expected, not a bug; its next row (whether the
    next Tier 2 poll or the next Tier 1 cycle) correctly shows
    tier='hot'.
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
            tier, hot_reason = tier2_state.tier_and_reason(venue, pool_row.pool_address)
            rows.append(pool_row.to_snapshot_row(snapshot_ts=snapshot_ts, tier=tier, hot_reason=hot_reason))
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


def run_cycle(client, tier2_state: Tier2State, tier2_poller: Tier2Poller) -> float:
    """Runs one full Tier 1 cycle: all 4 DEX venues, then all 5 CEX
    exchanges (sequential -- see module docstring), writing each
    source's rows to ClickHouse immediately, then one detect_new_entities
    call per venue/exchange against exactly this cycle's own rows.
    Interleaved with all of that (per-source, at each of the 9
    checkpoints below): Tier 2's growth-based re-scoring, age-based
    promotion, new-listing matching, and `tier2_poller.maybe_poll()`
    (a no-op unless ~60s have elapsed -- see tier2_poller.py's
    docstring for why this is checked here instead of on its own
    thread). Returns the cycle's wall-clock duration in seconds.
    """
    cycle_start = time.monotonic()
    snapshot_ts = datetime.datetime.now(datetime.timezone.utc)
    log(Level.INFO, [("event", "cycle_start"), ("snapshot_ts", snapshot_ts.isoformat())], src=SCHEDULER_SRC)

    # Pure clock comparison, no re-evaluation of activity -- section
    # 3.2's explicit rule for age-based Tier 2 membership. Done once,
    # up front, so every row this cycle (Tier 1 and any interleaved
    # Tier 2 poll) reflects the same settled age-based state.
    tier2_state.age_based.prune_expired(snapshot_ts)

    dex_rows_by_venue: dict[str, list[dict]] = {}
    for venue, provider in _build_dex_providers().items():
        rows = _run_dex_source(venue, provider, snapshot_ts, tier2_state)
        dex_rows_by_venue[venue] = rows
        _write_rows(client, "dex_pool_snapshots", rows, DEX_SRC[venue])
        if rows:
            # Growth-based re-scoring against the prior Tier 1 cycle,
            # per section 3.2 ("re-evaluira se na svaki Tier 1
            # ciklus") -- promotions/demotions decided here apply
            # starting from the *next* observation of each pool (the
            # next Tier 2 poll or Tier 1 cycle), same reasoning as
            # _run_dex_source's docstring on why the row just written
            # can't reflect a decision made from its own data.
            tier2_state.growth_based.evaluate(client, venue, rows, snapshot_ts)
        tier2_poller.maybe_poll()

    cex_rows_by_exchange: dict[str, list[dict]] = {}
    for exchange, provider in _build_cex_providers().items():
        rows = _run_cex_source(exchange, provider, snapshot_ts)
        cex_rows_by_exchange[exchange] = rows
        _write_rows(client, "cex_symbol_snapshots", rows, CEX_SRC[exchange])
        tier2_poller.maybe_poll()

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
        # Age-based promotion: exactly the pool_addresses
        # detect_new_entities just confirmed have never been seen
        # before, for this venue. Independent of growth-based (see
        # tier2.py's module docstring) -- this never touches
        # growth_based at all, even for a pool that happens to also
        # trigger a growth spike on this same first cycle.
        tier2_state.age_based.promote(venue, result.new_identifiers, snapshot_ts)

    new_cex_symbols: list[tuple[str, str]] = []  # (exchange, base_asset), for new_listing matching below
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
        if result.new_identifiers:
            by_symbol = {row["symbol"]: row["base_asset"] for row in rows}
            new_cex_symbols.extend((exchange, by_symbol[sym]) for sym in result.new_identifiers if sym in by_symbol)

    if new_cex_symbols:
        # One base_symbol lookup for the whole cycle, not per exchange
        # -- see latest_dex_pools_by_base_symbol's own docstring.
        dex_pools_by_symbol = latest_dex_pools_by_base_symbol(client)
        newly_promoted = apply_new_listing_promotions(tier2_state.growth_based, new_cex_symbols, dex_pools_by_symbol)
        if newly_promoted:
            log(
                Level.INFO,
                [("event", "new_listing_promoted"), ("count", str(len(newly_promoted)))],
                src=TIER2_SRC,
            )

    elapsed = time.monotonic() - cycle_start
    log(
        Level.INFO,
        [
            ("event", "cycle_done"),
            ("elapsed_s", f"{elapsed:.1f}"),
            ("total_checked", str(total_checked)),
            ("total_new", str(total_new)),
            ("tier2_age_based", str(len(tier2_state.age_based))),
            ("tier2_growth_based", str(len(tier2_state.growth_based))),
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
        [
            ("event", "scheduler_start"),
            ("cycle_interval_s", str(CYCLE_INTERVAL_S)),
            ("tier2_poll_interval_s", str(TIER2_POLL_INTERVAL_S)),
        ],
        src=SCHEDULER_SRC,
    )

    tier2_state = Tier2State()
    now = datetime.datetime.now(datetime.timezone.utc)
    tier2_state.age_based.seed_from_history(client, now)
    tier2_poller = Tier2Poller(client, tier2_state)

    while not _shutdown_requested:
        try:
            elapsed = run_cycle(client, tier2_state, tier2_poller)
        except Exception:  # noqa: BLE001 -- last-resort guard, see comment below
            # Should be unreachable: every source and the detection
            # call already catch their own exceptions. But a scheduler-
            # level bug here must never silently kill the whole
            # process -- log the full traceback and keep the loop
            # alive rather than dying on something genuinely
            # unanticipated.
            #
            # Pre-existing bug found and fixed during this session's
            # own testing (a Tier 2 bug happened to be the first thing
            # that ever actually hit this branch): the previous
            # version set `elapsed = CYCLE_INTERVAL_S` here, intending
            # "don't retry immediately" -- but that makes
            # `sleep_s = CYCLE_INTERVAL_S - elapsed` exactly 0, which
            # hits the *overran-interval* branch below and `continue`s
            # with **zero** sleep, i.e. the exact opposite of the
            # intent: a hot loop, observed live retrying and crashing
            # every ~3-15ms. Fixed by sleeping a fixed backoff here
            # directly and `continue`-ing past the normal elapsed/
            # sleep_s math entirely, rather than trying to smuggle
            # "please back off" through a variable that math was never
            # meant to receive.
            log(Level.ERROR, [("event", "cycle_CRASHED")], src=SCHEDULER_SRC)
            traceback.print_exc()
            if _shutdown_requested:
                break
            log(Level.WARN, [("event", "crash_backoff"), ("sleep_s", str(CRASH_BACKOFF_S))], src=SCHEDULER_SRC)
            _interruptible_sleep(CRASH_BACKOFF_S)
            continue

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
        _interruptible_sleep(sleep_s)

    log(Level.INFO, [("event", "scheduler_stopped")], src=SCHEDULER_SRC)


if __name__ == "__main__":
    main()
