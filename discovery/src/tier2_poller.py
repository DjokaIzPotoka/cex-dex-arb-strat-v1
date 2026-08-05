"""Tier 2's 1-minute polling, per docs/mev-discovery-plan.md section
3.2: "Poolovi promovisani u ovaj tier... 1 min [kadenca]". Re-polls
whatever's currently hot (Tier2State.current_hot_pools(), the union of
age-based and growth-based membership) between Tier 1's 5-minute
sweeps, via the exact same REST providers Tier 1 uses -- this session
deliberately does NOT build an RPC-based state-only fast path (that's
Phase 1h, per this session's explicit scope).

Per-venue poll batch is capped (MAX_POOLS_PER_VENUE_PER_POLL) and
rotates round-robin through a large hot set rather than always trying
to poll every hot pool in one pass. Found live, not anticipated in the
original design: age-based seeding from new_entity_events'
full-72h history (Tier2State.age_based.seed_from_history, called once
at scheduler startup) can produce thousands of "hot" pools per venue
in a real deployment -- observed live, one seed produced 5558
orca_whirlpool entries alone. Orca and Meteora have no batch pool
lookup endpoint (one HTTP GET per pool_address, see each provider's
get_pools docstring), so an uncapped poll pass tries to make
one-request-per-pool against Orca's conservative 5 req/s bucket:
5558/5 ~= 1112s (~18.5 minutes) for Orca's share alone, which is
exactly what happened the first time this ran live against real
history -- the whole scheduler process appeared hung for ~19 minutes,
blocked inside a single maybe_poll() call, before this cap was added.
Capping and rotating means a very large hot set doesn't get refreshed
in full every single ~60s interval (each pool gets re-polled roughly
every `ceil(hot_count / MAX_POOLS_PER_VENUE_PER_POLL)` poll passes,
not every pass) -- a real, documented degradation under REST-only
Tier 2 at this scale, not a hidden one. This is precisely the gap
docs/mev-discovery-plan.md section 3.3 designed the future RPC
PoolStateProvider to close (`getMultipleAccounts`, batch up to 100
addresses per call, no per-pool HTTP round-trip at all) -- Phase 1h,
out of this session's scope, not attempted here.

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
import time
from typing import Optional

from discovery.src.logger import Level, log
from discovery.src.providers.meteora_dlmm import MeteoraDlmmRestProvider
from discovery.src.providers.orca_rest import OrcaRestProvider
from discovery.src.providers.raydium_amm_v4 import RaydiumAmmV4RestProvider
from discovery.src.providers.raydium_clmm import RaydiumClmmRestProvider
from discovery.src.tier2 import Tier2State

TIER2_POLL_SRC = "T2P"
TIER2_POLL_INTERVAL_S = 60.0

# Sized so even the slowest venue (Orca, 5 req/s, one HTTP call per
# pool -- see module docstring) finishes one poll pass in ~20s, well
# under TIER2_POLL_INTERVAL_S, instead of the ~18.5 minutes an
# uncapped pass against a large real hot set took live (see module
# docstring for the incident this constant exists because of).
MAX_POOLS_PER_VENUE_PER_POLL = 100


def build_tier2_providers() -> dict[str, object]:
    """Long-lived provider instances, constructed once at scheduler
    startup and reused for every poll over the process's lifetime --
    deliberately unlike scheduler.py's Tier 1 providers, which are
    rebuilt fresh every 5-minute cycle (_build_dex_providers()). Tier 2
    doesn't need a fresh session/bucket every cycle the way Tier 1's
    per-cycle rebuild pattern does (that pattern exists for Tier 1's
    own reasons, not something Tier 2 needs to inherit) -- a bucket
    that persists across the whole run lets its capacity naturally
    reflect real sustained usage instead of resetting to full every 5
    minutes regardless of how much was actually consumed.
    """
    return {
        "orca_whirlpool": OrcaRestProvider(),
        "raydium_amm_v4": RaydiumAmmV4RestProvider(),
        "raydium_clmm": RaydiumClmmRestProvider(),
        "meteora_dlmm": MeteoraDlmmRestProvider(),
    }


class Tier2Poller:
    """Owns the "has it been ~60s since the last poll" clock and the
    long-lived provider instances. `maybe_poll()` is the only entry
    point scheduler.py calls -- a no-op most of the time, an actual
    poll pass when enough time has elapsed. See module docstring for
    why this is checked inline rather than run on its own thread.
    """

    def __init__(self, client, tier2_state: Tier2State, providers: Optional[dict[str, object]] = None):
        self._client = client
        self._tier2_state = tier2_state
        self._providers = providers or build_tier2_providers()
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
            return {"hot_pools": 0, "rows_written": 0}

        by_venue: dict[str, list[str]] = {}
        for venue, pool_address in hot_pools:
            by_venue.setdefault(venue, []).append(pool_address)

        snapshot_ts = datetime.datetime.now(datetime.timezone.utc)
        t0 = time.monotonic()
        rows_written = 0

        for venue, addresses in by_venue.items():
            provider = self._providers.get(venue)
            if provider is None:
                continue  # shouldn't happen -- every DEX venue has a Tier 2 provider -- but never crash the poll over it

            batch = self._next_batch(venue, addresses)
            if len(batch) < len(addresses):
                log(
                    Level.INFO,
                    [
                        ("event", "poll_batch_capped"),
                        ("venue", venue),
                        ("hot_total", str(len(addresses))),
                        ("this_batch", str(len(batch))),
                    ],
                    src=TIER2_POLL_SRC,
                )

            try:
                pool_rows = provider.get_pools(batch)
            except Exception as exc:  # noqa: BLE001 -- same failure-isolation contract as scheduler.py's Tier 1 sources
                log(
                    Level.ERROR,
                    [("event", "poll_source_FAILED"), ("venue", venue), ("pool_count", str(len(batch))), ("err", str(exc))],
                    src=TIER2_POLL_SRC,
                )
                continue

            rows = []
            for pool_row in pool_rows:
                tier, hot_reason = self._tier2_state.tier_and_reason(venue, pool_row.pool_address)
                rows.append(pool_row.to_snapshot_row(snapshot_ts=snapshot_ts, tier=tier, hot_reason=hot_reason))

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
        return {"hot_pools": len(hot_pools), "rows_written": rows_written}

    def _next_batch(self, venue: str, addresses: list[str]) -> list[str]:
        """Returns up to MAX_POOLS_PER_VENUE_PER_POLL addresses for
        `venue` this call, advancing that venue's round-robin cursor
        so the *next* call picks up where this one left off -- see
        module docstring for why an uncapped pass is unsafe at scale.
        Sorted first for a stable, deterministic window (the input is
        built from a Python set each poll, so its iteration order
        isn't itself stable across calls).
        """
        if len(addresses) <= MAX_POOLS_PER_VENUE_PER_POLL:
            return addresses
        ordered = sorted(addresses)
        offset = self._venue_offset.get(venue, 0) % len(ordered)
        end = offset + MAX_POOLS_PER_VENUE_PER_POLL
        if end <= len(ordered):
            batch = ordered[offset:end]
        else:
            # Wrap around: take the tail, then the head, so the
            # rotation covers the whole list evenly instead of
            # stalling at the boundary.
            batch = ordered[offset:] + ordered[: end - len(ordered)]
        self._venue_offset[venue] = end % len(ordered)
        return batch
