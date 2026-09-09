"""Tier 2's 1-minute polling, per docs/mev-discovery-plan.md section
3.2: "Poolovi promovisani u ovaj tier... 1 min [kadenca]". Re-polls
whatever's currently hot (Tier2State.current_hot_pools(), the union of
age-based and growth-based membership) between Tier 1's 5-minute
sweeps.

As of Phase 1h (this session), Orca Whirlpool and Raydium CLMM hot
pools go through solana_rpc_state.py's batched `getMultipleAccounts`
RPC provider for `price_native`/`active_liquidity` instead of the
one-request-per-pool REST call this module used through Phase 1g --
see that module's docstring, and _poll_venue_via_rpc below, for the
hybrid design (RPC for state on this 1-min cadence, REST-derived
metadata/volume carried forward from ClickHouse rather than
re-fetched, per section 3.3). Raydium AMM v4 and Meteora DLMM are
unaffected -- still REST `get_pools()`, same as every prior phase; AMM
v4 was never the bottleneck (its `active_liquidity` is exactly
`tvl_usd` by construction, no RPC decoding buys anything there), and
Meteora's bin-based state is explicitly out of scope for this session
(see solana_rpc_state.py's docstring).

Per-venue poll batch is capped (MAX_POOLS_PER_VENUE_PER_POLL) and
rotates round-robin through a large hot set rather than always trying
to poll every hot pool in one pass. Found live, not anticipated in the
original design: age-based seeding from new_entity_events'
full-72h history (Tier2State.age_based.seed_from_history, called once
at scheduler startup) can produce thousands of "hot" pools per venue
in a real deployment -- observed live, one seed produced 5558
orca_whirlpool entries alone. Before Phase 1h, Orca and Meteora had no
batch pool lookup endpoint (one HTTP GET per pool_address, see each
REST provider's get_pools docstring), so an uncapped poll pass tried
one-request-per-pool against Orca's conservative 5 req/s REST bucket:
5558/5 ~= 1112s (~18.5 minutes) for Orca's share alone, which is
exactly what happened the first time this ran live against real
history -- the whole scheduler process appeared hung for ~19 minutes,
blocked inside a single maybe_poll() call, before this cap was added.
The cap stays in place even now that Orca/CLMM go through RPC
(MAX_POOLS_PER_VENUE_PER_POLL is still real ceiling on getMultipleAccounts'
own per-call batch limit and on ClickHouse write volume per poll, and
Meteora's REST path still has the original per-pool-HTTP-call
bottleneck this cap was built for). Capping and rotating means a very
large hot set doesn't get refreshed in full every single ~60s interval
(each pool gets re-polled roughly every
`ceil(hot_count / MAX_POOLS_PER_VENUE_PER_POLL)` poll passes, not every
pass) -- for Meteora specifically this is still a real, documented
degradation at scale, not a hidden one; for Orca/CLMM, Phase 1h's RPC
batching means a single getMultipleAccounts call already covers a full
100-pool batch in one round trip, so the practical cadence hit from
capping is far smaller there than it was under REST.

Cadence is best-effort, not a hard real-time guarantee, and that's a
deliberate tradeoff worth stating plainly rather than overselling:
scheduler.py's Tier 1 loop is single-threaded and (per Phase 1e/1f's
own findings) already has essentially zero idle time within a 5-minute
cycle -- a full unbounded DEX sweep alone takes ~280-320s. Two ways to
get a genuine 1-minute cadence exist: run Tier 2 polling on a separate
thread, or check a clock at natural break points inside the existing
single-threaded loop and poll inline when ~60s have elapsed. This
module is the second approach. It was chosen over real threading
specifically to avoid a class of problems that would need their own
dedicated testing effort before landing (per scheduler.py's own
concurrency TODO): concurrent access to a shared ClickHouse client
(clickhouse-connect's Client is not documented as thread-safe for
concurrent use), and concurrent HTTP traffic against the same DEX host
Tier 1 might be mid-fetch against. Because this module's poll only
ever runs from a `maybe_poll()` call sited inside scheduler.py's
single sequential thread (never its own thread), those problems don't
exist here at all -- a Tier 2 poll and a Tier 1 fetch can never
literally overlap in wall-clock time, by construction, not by careful
locking. The cost is cadence precision: `maybe_poll()` is checked at
each of the 9 per-source checkpoints inside one Tier 1 cycle, so the
realized gap between two consecutive Tier 2 polls is whatever that
checkpoint spacing happens to be (dominated by DEX sources, which take
tens of seconds to ~2 minutes each) -- expect polls roughly every
60-120s in practice, not a metronomic 60.0s. Real achieved spacing
from live validation is reported in this session's summary, not
assumed here.
"""
from __future__ import annotations

import datetime
import os
import time
from typing import Optional

from discovery.src.logger import Level, log
from discovery.src.normalize import STABLE_SYMBOLS
from discovery.src.providers.meteora_dlmm import MeteoraDlmmRestProvider
from discovery.src.providers.raydium_amm_v4 import RaydiumAmmV4RestProvider
from discovery.src.providers.solana_rpc_state import (
    VENUE_ORCA,
    VENUE_RAYDIUM_CLMM,
    OrcaWhirlpoolRpcStateProvider,
    RaydiumClmmRpcStateProvider,
)
from discovery.src.telemetry import RunTelemetry, segment_or_noop
from discovery.src.tier2 import AGE_BASED_WINDOW, DEMOTION_CONSECUTIVE_CYCLES, Tier2State

TIER2_POLL_SRC = "T2P"
TIER2_POLL_INTERVAL_S = 60.0

# Sized so even the slowest REST venue (Meteora, one HTTP call per
# pool -- see module docstring) finishes one poll pass comfortably
# under TIER2_POLL_INTERVAL_S, instead of the ~18.5 minutes an
# uncapped pass against a large real hot set took live (see module
# docstring for the incident this constant exists because of). For
# the RPC venues (Orca/Raydium CLMM), this is the per-*poll* address
# budget, not the per-*call* batch size -- solana_rpc_state.py's
# QUICKNODE_BATCH_LIMIT (4, well under this) further chunks it into
# several getMultipleAccounts calls internally, since the configured
# RPC host enforces a tighter limit than this constant or the
# protocol's own 100-account cap.
MAX_POOLS_PER_VENUE_PER_POLL = 100

# Venues whose Tier 2 state polling goes through solana_rpc_state.py's
# batched RPC provider (Phase 1h) instead of REST get_pools(). Meteora
# DLMM's bin-based state and Raydium AMM v4 (no RPC benefit -- see
# module docstring) stay REST-only.
RPC_VENUES = {VENUE_ORCA, VENUE_RAYDIUM_CLMM}

DEFAULT_SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"

# Phase 2a (telemetry) only: venues whose active_liquidity is a genuine
# temporary placeholder (tvl_usd copied verbatim, pending real decoding
# -- see meteora_dlmm.py's own TODO(Phase 1h) comment), as opposed to
# raydium_amm_v4's tvl_usd==active_liquidity, which is correct BY
# DESIGN for a constant-product pool (see raydium_amm_v4.py's
# _compute_active_liquidity: "Not a TODO(Phase 1h) placeholder, unlike
# CLMM... tvl_usd genuinely *is* active_liquidity", and design doc
# section 3.5's own note). Deliberately NOT raydium_amm_v4 -- see this
# session's summary for why status.json nulls active_liquidity here
# for meteora_dlmm only, not for both, despite CLAUDE.md's Phase 1h
# paragraph grouping the two together as both "carrying the tvl_usd
# placeholder" (a looser summary that doesn't distinguish
# correct-by-design from genuinely-temporary the way the code and the
# design doc both do).
TELEMETRY_PLACEHOLDER_ACTIVE_LIQUIDITY_VENUES = {"meteora_dlmm"}


def build_tier2_providers(rpc_url: Optional[str] = None) -> dict[str, object]:
    """Long-lived provider instances, constructed once at scheduler
    startup and reused for every poll over the process's lifetime --
    deliberately unlike scheduler.py's Tier 1 providers, which are
    rebuilt fresh every 5-minute cycle (_build_dex_providers()). Tier 2
    doesn't need a fresh session/bucket every cycle the way Tier 1's
    per-cycle rebuild pattern does (that pattern exists for Tier 1's
    own reasons, not something Tier 2 needs to inherit) -- a bucket
    that persists across the whole run lets its capacity naturally
    reflect real sustained usage instead of resetting to full every 5
    minutes regardless of how much was actually consumed. This also
    matters specifically for solana_rpc_state.py's providers: their
    mint-decimals cache (see that module's docstring) is only useful
    at all if the instance survives across polls, not just within one.

    `rpc_url` defaults to `SOLANA_RPC_URL` from the environment (same
    convention as rpc.rs::build_client and
    run_raydium_snapshot.py::main), falling back to the public mainnet
    endpoint if unset -- see scripts/zora_discovery.sh for why that env
    var must actually be present by the time this runs in production
    (it now sources .env, same as scripts/zora.sh already did).
    """
    rpc_url = rpc_url or os.environ.get("SOLANA_RPC_URL") or DEFAULT_SOLANA_RPC_URL
    return {
        VENUE_ORCA: OrcaWhirlpoolRpcStateProvider(rpc_url),
        "raydium_amm_v4": RaydiumAmmV4RestProvider(),
        VENUE_RAYDIUM_CLMM: RaydiumClmmRpcStateProvider(rpc_url),
        "meteora_dlmm": MeteoraDlmmRestProvider(),
    }


class Tier2Poller:
    """Owns the "has it been ~60s since the last poll" clock and the
    long-lived provider instances. `maybe_poll()` is the only entry
    point scheduler.py calls -- a no-op most of the time, an actual
    poll pass when enough time has elapsed. See module docstring for
    why this is checked inline rather than run on its own thread.
    """

    def __init__(
        self,
        client,
        tier2_state: Tier2State,
        providers: Optional[dict[str, object]] = None,
        rpc_url: Optional[str] = None,
        telemetry: Optional[RunTelemetry] = None,
    ):
        self._client = client
        self._tier2_state = tier2_state
        self._providers = providers or build_tier2_providers(rpc_url)
        self._telemetry = telemetry
        self._last_poll_monotonic: Optional[float] = None
        # Round-robin cursor per venue, into that venue's *sorted*
        # current hot-pool list -- sorted so the window is stable
        # across calls even as the underlying set (a Python set,
        # unordered) gains/loses members between polls. Persists across
        # calls (unlike hot_pools itself, which is recomputed fresh
        # every poll), so a large backlog is worked through over
        # several consecutive polls rather than the same first N
        # addresses being polled forever while the rest starve.
        self._venue_offset: dict[str, int] = {}

    def maybe_poll(self) -> Optional[dict]:
        """Returns None if this call didn't poll (not enough time
        elapsed yet), or a small summary dict if it did. The first
        call always polls (there's no "last poll" to compare against
        yet) -- harmless even if Tier 2 is empty at scheduler startup
        (poll_once() below is a fast no-op when there's nothing hot).
        """
        now = time.monotonic()
        if self._last_poll_monotonic is not None and (now - self._last_poll_monotonic) < TIER2_POLL_INTERVAL_S:
            return None
        self._last_poll_monotonic = now
        return self._poll_once()

    def _poll_once(self) -> dict:
        hot_pools = self._tier2_state.current_hot_pools()
        if not hot_pools:
            log(Level.INFO, [("event", "poll_skipped"), ("reason", "no_hot_pools")], src=TIER2_POLL_SRC)
            if self._telemetry is not None:
                self._telemetry.record_tier2_snapshot(self._tier2_snapshot(0.0, {}, []))
            return {"hot_pools": 0, "rows_written": 0}

        with segment_or_noop(self._telemetry, "tier2_poll", "tier2_poll") as seg:
            # Split by mechanism, not just by venue: growth-based membership
            # is prioritized whole into every batch (see _next_batch's
            # docstring for why), so it needs to stay a separate list from
            # age-based rather than being pre-merged into one undifferentiated
            # per-venue rotation the way current_hot_pools() merges them.
            growth_by_venue: dict[str, list[str]] = {}
            for venue, pool_address in self._tier2_state.growth_based.hot_keys():
                growth_by_venue.setdefault(venue, []).append(pool_address)

            age_by_venue: dict[str, list[str]] = {}
            for venue, pool_address in self._tier2_state.age_based.hot_keys():
                age_by_venue.setdefault(venue, []).append(pool_address)

            by_venue: dict[str, list[str]] = {}
            for venue, pool_address in hot_pools:
                by_venue.setdefault(venue, []).append(pool_address)

            snapshot_ts = datetime.datetime.now(datetime.timezone.utc)
            t0 = time.monotonic()
            rows_written = 0
            venue_capped: dict[str, bool] = {}
            pool_entries: list[dict] = []

            for venue, addresses in by_venue.items():
                provider = self._providers.get(venue)
                if provider is None:
                    continue  # shouldn't happen -- every DEX venue has a Tier 2 provider -- but never crash the poll over it

                batch = self._next_batch(venue, growth_by_venue.get(venue, []), age_by_venue.get(venue, []))
                capped = len(batch) < len(addresses)
                venue_capped[venue] = capped
                if capped:
                    log(
                        Level.INFO,
                        [
                            ("event", "poll_batch_capped"),
                            ("venue", venue),
                            ("hot_total", str(len(addresses))),
                            ("growth_total", str(len(growth_by_venue.get(venue, [])))),
                            ("this_batch", str(len(batch))),
                        ],
                        src=TIER2_POLL_SRC,
                    )

                path = "rpc" if venue in RPC_VENUES else "rest"
                if venue in RPC_VENUES:
                    rows = self._poll_venue_via_rpc(venue, provider, batch, snapshot_ts)
                else:
                    try:
                        pool_rows = provider.get_pools(batch)
                    except Exception as exc:  # noqa: BLE001 -- same failure-isolation contract as scheduler.py's Tier 1 sources
                        log(
                            Level.ERROR,
                            [("event", "poll_source_FAILED"), ("venue", venue), ("pool_count", str(len(batch))), ("err", str(exc))],
                            src=TIER2_POLL_SRC,
                        )
                        pool_entries.extend(
                            self._pool_telemetry_entry(venue, addr, path, snapshot_ts) for addr in batch
                        )
                        continue

                    rows = []
                    for pool_row in pool_rows:
                        tier, hot_reason = self._tier2_state.tier_and_reason(venue, pool_row.pool_address)
                        rows.append(pool_row.to_snapshot_row(snapshot_ts=snapshot_ts, tier=tier, hot_reason=hot_reason))

                produced = {row["pool_address"] for row in rows}
                for row in rows:
                    pool_entries.append(self._pool_telemetry_entry(venue, row["pool_address"], path, snapshot_ts, row=row))
                for addr in batch:
                    if addr not in produced:
                        pool_entries.append(self._pool_telemetry_entry(venue, addr, path, snapshot_ts))

                if rows:
                    columns = list(rows[0].keys())
                    data = [[row[c] for c in columns] for row in rows]
                    self._client.insert("dex_pool_snapshots", data, column_names=columns)
                    rows_written += len(rows)

            elapsed = time.monotonic() - t0
            log(
                Level.INFO,
                [
                    ("event", "poll_done"),
                    ("hot_pools", str(len(hot_pools))),
                    ("rows_written", str(rows_written)),
                    ("elapsed_s", f"{elapsed:.1f}"),
                ],
                src=TIER2_POLL_SRC,
            )

            seg.rows = rows_written
            seg.extra = {"venue_capped": venue_capped, "rpc_venues": sorted(RPC_VENUES)}

            if self._telemetry is not None:
                self._telemetry.record_tier2_snapshot(self._tier2_snapshot(elapsed, venue_capped, pool_entries))

        return {"hot_pools": len(hot_pools), "rows_written": rows_written}

    def _tier2_snapshot(self, elapsed_s: float, venue_capped: dict[str, bool], pools: list[dict]) -> dict:
        """Phase 2a (telemetry) only: the dict merged into status.json's
        "tier2" section by RunTelemetry.record_tier2_snapshot(). Read
        directly from Tier2State/this poller's own already-in-memory
        state -- no query, so this stays cheap even though it's built
        on every poll pass (including the "nothing hot" no-op above).
        """
        return {
            "hot_total": len(self._tier2_state.current_hot_pools()),
            "age_based": {
                "count": len(self._tier2_state.age_based),
                "window_h": AGE_BASED_WINDOW.total_seconds() / 3600.0,
            },
            "growth_based": {
                "count": len(self._tier2_state.growth_based),
                "hysteresis_cycles": DEMOTION_CONSECUTIVE_CYCLES,
            },
            "last_poll_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "last_poll_duration_s": round(elapsed_s, 1),
            "max_pools_per_venue_per_poll": MAX_POOLS_PER_VENUE_PER_POLL,
            "venue_capped": venue_capped,
            "pools": pools,
        }

    def _pool_telemetry_entry(
        self, venue: str, pool_address: str, path: str, snapshot_ts: datetime.datetime, row: Optional[dict] = None
    ) -> dict:
        """Phase 2a (telemetry) only: one status.json "tier2.pools"
        entry, from a row this poll pass already produced (`row` given
        -- a successful poll) or from just the address (`row=None` --
        this pool was in the batch but didn't come back, e.g. an RPC
        account-not-found or a whole-batch REST failure). Never queries
        anything -- growth data comes from GrowthBasedRegistry.entry(),
        already-in-memory state, not a fresh computation.

        `active_liquidity` is nulled for
        TELEMETRY_PLACEHOLDER_ACTIVE_LIQUIDITY_VENUES (meteora_dlmm
        only -- see that constant's own comment for why raydium_amm_v4
        is deliberately NOT included despite CLAUDE.md's Phase 1h
        paragraph grouping the two together).
        """
        growth_entry = self._tier2_state.growth_based.entry(venue, pool_address)
        if row is not None:
            pair = f"{row['base_symbol']}/{row['quote_symbol']}"
            active_liquidity = None if venue in TELEMETRY_PLACEHOLDER_ACTIVE_LIQUIDITY_VENUES else row["active_liquidity"]
            price_native = row["price_native"]
            hot_reason = row["hot_reason"]
        else:
            pair = None
            active_liquidity = None
            price_native = None
            _, hot_reason = self._tier2_state.tier_and_reason(venue, pool_address)

        return {
            "pool_id": pool_address,
            "venue": venue,
            "pair": pair,
            "path": path,
            "price_native": price_native,
            "active_liquidity": active_liquidity,
            "volume_growth_24h": growth_entry.last_volume_growth if growth_entry else None,
            "tvl_growth_24h": growth_entry.last_tvl_growth if growth_entry else None,
            "hot_reason": hot_reason,
            "consecutive_below": growth_entry.consecutive_below_threshold if growth_entry else 0,
            "last_polled_at": snapshot_ts.isoformat(),
            "last_poll_failed": row is None,
        }

    def _fetch_latest_known_rows(self, venue: str, pool_addresses: list[str]) -> dict[str, dict]:
        """Latest known REST-derived row per pool_address (from
        whatever Tier 1's last successful pull wrote to
        dex_pool_snapshots), for carrying forward base_mint/
        base_symbol/quote_mint/quote_symbol/fee_bps/tvl_usd/
        volume_24h_usd into an RPC-sourced row -- see
        _poll_venue_via_rpc. `pool_addresses` is at most
        MAX_POOLS_PER_VENUE_PER_POLL (100) per call, well under the
        ~1000-identifier threshold where CLAUDE.md's documented
        ClickHouse HTTP Array(String) field-size limit starts to
        matter (see detection.py's EXISTENCE_CHECK_CHUNK_SIZE), so no
        chunking is needed here.
        """
        if not pool_addresses:
            return {}
        result = self._client.query(
            """
            SELECT pool_address, base_mint, base_symbol, quote_mint, quote_symbol, fee_bps, tvl_usd, volume_24h_usd
            FROM dex_pool_snapshots
            WHERE venue = {venue:String} AND pool_address IN {addresses:Array(String)}
              AND (pool_address, snapshot_ts) IN (
                  SELECT pool_address, max(snapshot_ts) FROM dex_pool_snapshots
                  WHERE venue = {venue:String} AND pool_address IN {addresses:Array(String)}
                  GROUP BY pool_address
              )
            """,
            parameters={"venue": venue, "addresses": pool_addresses},
        )
        known: dict[str, dict] = {}
        for pool_address, base_mint, base_symbol, quote_mint, quote_symbol, fee_bps, tvl_usd, volume_24h_usd in result.result_rows:
            known[pool_address] = {
                "base_mint": base_mint,
                "base_symbol": base_symbol,
                "quote_mint": quote_mint,
                "quote_symbol": quote_symbol,
                "fee_bps": fee_bps,
                "tvl_usd": tvl_usd,
                "volume_24h_usd": volume_24h_usd,
            }
        return known

    def _poll_venue_via_rpc(self, venue: str, provider, batch: list[str], snapshot_ts: datetime.datetime) -> list[dict]:
        """RPC path for hot pools on an RPC-covered venue (Orca
        Whirlpool, Raydium CLMM -- see solana_rpc_state.py): fetches
        fresh price_native/active_liquidity via getMultipleAccounts,
        and combines it with each pool's most recent REST-derived
        metadata/volume row from ClickHouse -- the hybrid design
        section 3.3 calls for (RPC state on this 1-min cadence, REST
        volume/metadata on Tier 1's 5-min baseline, not refetched
        here). price_usd is recomputed fresh from the new price_native
        (not carried forward stale) using the same
        quote-is-a-stablecoin convention every REST provider already
        uses (see e.g. orca_rest.py's own price_usd comment).
        """
        known = self._fetch_latest_known_rows(venue, batch)
        missing = [addr for addr in batch if addr not in known]
        if missing:
            # Shouldn't happen in steady state: a pool only becomes
            # Tier 2-hot after Tier 1 has already written at least one
            # REST-derived row for it (age-based promotion is fed by
            # detect_new_entities, itself downstream of a Tier 1 REST
            # pull; growth-based promotion needs a *prior* row to
            # compute growth against at all -- see tier2.py's
            # GrowthBasedRegistry.evaluate()). Logged rather than
            # silently dropped so a real gap (e.g. pruned ClickHouse
            # data) stays visible instead of just quietly under-polling.
            log(
                Level.WARN,
                [("event", "rpc_poll_skip_no_known_row"), ("venue", venue), ("count", str(len(missing)))],
                src=TIER2_POLL_SRC,
            )

        addresses = list(known.keys())
        if not addresses:
            return []

        base_mint_by_pool = {addr: row["base_mint"] for addr, row in known.items()}
        try:
            states = provider.fetch_state(addresses, base_mint_by_pool)
        except Exception as exc:  # noqa: BLE001 -- same failure-isolation contract as the REST path
            log(
                Level.ERROR,
                [("event", "poll_source_FAILED"), ("venue", venue), ("pool_count", str(len(addresses))), ("err", str(exc))],
                src=TIER2_POLL_SRC,
            )
            return []

        rows = []
        for state in states:
            known_row = known[state.pool_address]
            tier, hot_reason = self._tier2_state.tier_and_reason(venue, state.pool_address)
            price_usd = state.price_native if known_row["quote_symbol"] in STABLE_SYMBOLS else None
            rows.append(
                {
                    "snapshot_ts": snapshot_ts,
                    "venue": venue,
                    "pool_address": state.pool_address,
                    "base_mint": known_row["base_mint"],
                    "base_symbol": known_row["base_symbol"],
                    "quote_mint": known_row["quote_mint"],
                    "quote_symbol": known_row["quote_symbol"],
                    "fee_bps": known_row["fee_bps"],
                    "tvl_usd": known_row["tvl_usd"],
                    "active_liquidity": state.active_liquidity,
                    "volume_24h_usd": known_row["volume_24h_usd"],
                    "price_native": state.price_native,
                    "price_usd": price_usd,
                    "tier": tier,
                    "hot_reason": hot_reason,
                }
            )
        return rows

    def _next_batch(self, venue: str, growth_addresses: list[str], age_addresses: list[str]) -> list[str]:
        """Returns up to MAX_POOLS_PER_VENUE_PER_POLL addresses for
        `venue` this call.

        Growth-based pools go in first, in full, every single call --
        that set is expected to stay small (section 3.2's continuous
        re-scoring only keeps actively-spiking pools hot, unlike
        age-based's fixed 72h window over the whole new-pool backlog),
        and growth-based hotness (a live volume/TVL spike or a new CEX
        listing) is exactly the kind of signal Tier 2's 1-minute
        cadence exists to catch promptly. Age-based pools then fill
        whatever budget remains, round-robining through the backlog
        exactly as before.

        Before this split, both sets were pre-merged into one
        undifferentiated per-venue rotation (current_hot_pools()'s
        union) and round-robined together -- live validation found
        age-based backlogs outnumber growth-based hot pools by roughly
        230x per venue, so a growth-based pool's turn in a capped
        batch came up only once every ~230/MAX_POOLS_PER_VENUE_PER_POLL
        rotations, i.e. almost never got polled at 1-minute cadence at
        all. Splitting means growth-based pools are no longer subject
        to the round-robin cursor -- they're in every batch, every
        poll, regardless of how large the age-based backlog is.

        Sorted first (both lists) for a stable, deterministic window --
        the inputs are built from Python sets each poll, so iteration
        order isn't itself stable across calls.
        """
        # Defensive cap: growth-based is expected to stay well under
        # MAX_POOLS_PER_VENUE_PER_POLL, but if it somehow doesn't, it
        # must not be allowed to silently starve age-based of its
        # entire budget the same way the old undifferentiated rotation
        # let age-based starve growth-based.
        growth_batch = sorted(set(growth_addresses))[:MAX_POOLS_PER_VENUE_PER_POLL]
        remaining_budget = MAX_POOLS_PER_VENUE_PER_POLL - len(growth_batch)

        # Dedupe against growth_batch: a pool can be hot in both
        # registries at once (tier2.py's Tier2State docstring), and
        # polling the same address twice in one batch would double-
        # count it in rows_written and waste rate-limiter budget.
        age_only = sorted(set(age_addresses) - set(growth_batch))
        if remaining_budget <= 0 or not age_only:
            return growth_batch

        if len(age_only) <= remaining_budget:
            age_batch = age_only
        else:
            offset = self._venue_offset.get(venue, 0) % len(age_only)
            end = offset + remaining_budget
            if end <= len(age_only):
                age_batch = age_only[offset:end]
            else:
                # Wrap around: take the tail, then the head, so the
                # rotation covers the whole list evenly instead of
                # stalling at the boundary.
                age_batch = age_only[offset:] + age_only[: end - len(age_only)]
            self._venue_offset[venue] = end % len(age_only)

        return growth_batch + age_batch
