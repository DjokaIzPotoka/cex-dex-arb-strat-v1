"""Tier 2 (Hot / New Entity) promotion and demotion, per
docs/mev-discovery-plan.md section 3.2. Two independent mechanisms,
deliberately not merged into one (the doc's own instruction: "koja se
namerno ne mesaju u jedinstvenu logiku"):

- Age-based (fixed window, no re-evaluation): a pool enters Tier 2 the
  moment detect_new_entities() flags it as a brand-new dex_pool, stays
  for a fixed 72h regardless of what happens to its activity in the
  meantime, and is never demoted early by this module -- only by the
  clock. See `AgeBasedRegistry`.
- Growth-based (continuous re-scoring with hysteresis): any pool --
  regardless of age -- can enter Tier 2 because of a volume/TVL spike
  or a new CEX listing for its base token, re-evaluated every Tier 1
  cycle, demoted only after N consecutive below-threshold cycles (not
  a single one, to avoid flapping). See `GrowthBasedRegistry`.

Where this state lives: NOT a ClickHouse table. The design doc is
explicit about this (section 3.5, the closing note on `tier`/
`hot_reason`): "Bookkeeping trenutnog stanja Tier 2 liste (koji pool
je trenutno hot, od kada, po kom razlogu, koliko uzastopnih ciklusa je
ispod praga za demociju) vodi se odvojeno od ovih append-only snapshot
tabela -- kao interno stanje scheduler procesa, ne kao deo istorijske
seme." ("Tier 2's current-state bookkeeping is kept separately from
these append-only snapshot tables -- as internal scheduler process
state, not part of the historical schema.") So `Tier2State` below is
plain in-process Python state, not a new migration. The practical
consequence -- and a real, documented limitation, not an oversight --
is that this state does NOT survive a scheduler restart on its own.
Age-based state is cheaply and correctly recoverable on startup by
replaying `new_entity_events` (permanent, no TTL) for the last 72h --
see `AgeBasedRegistry.seed_from_history()`. Growth-based state
(hot_reason, the consecutive-below-threshold counter) has no
persisted equivalent anywhere in the schema and is genuinely lost on
restart -- a pool mid-hysteresis-countdown restarts its counter at 0,
and a currently-hot growth-based pool is simply not hot anymore until
it re-triggers. Acceptable for Phase 1's scope (matches the doc's own
framing of this as ephemeral scheduler state), but worth remembering
if scheduler restarts turn out to be frequent in practice.
"""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import Optional

from discovery.src.detection import ENTITY_TYPE_DEX_POOL
from discovery.src.logger import Level, log

TIER2_SRC = "T2M"  # Tier 2 management: promotion/demotion decisions, distinct from T2P's poll-execution lines (tier2_poller.py)

AGE_BASED_WINDOW = datetime.timedelta(hours=72)
DEMOTION_CONSECUTIVE_CYCLES = 3

# Section 3.2's example thresholds, explicitly called out in this
# session's task as "a starting point" pending real-data review, not a
# calibrated figure -- unlike priority_score's weights (also
# explicitly deferred, section 3.2), this simple OR rule is what the
# doc says to actually implement in Phase 1, not a placeholder.
GROWTH_VOLUME_THRESHOLD = 3.0  # volume_growth_24h > 3x
GROWTH_TVL_THRESHOLD = 2.0  # tvl_growth_24h > 2x

HOT_REASON_NEW_POOL = "new_pool"
HOT_REASON_VOLUME_SPIKE = "volume_spike"
HOT_REASON_TVL_SPIKE = "tvl_spike"
HOT_REASON_NEW_LISTING = "new_listing"

TIER_DISCOVERY = "discovery"
TIER_HOT = "hot"


def _safe_growth(current: float, prior: float) -> Optional[float]:
    """current/prior, guarding the one case that would raise
    (prior==0). Going from zero to any positive activity is itself a
    dramatic, real change worth flagging -- returns float('inf') for
    that case rather than skipping it (an actual "went from silent to
    active" transition shouldn't be invisible to the growth check
    just because the ratio is undefined). Two genuine zeros (prior==0,
    current==0) carry no signal at all -- returns None, meaning "not
    evaluable", not "zero growth" (which would be misleading: 0/0
    isn't 0x growth, it's no observation).
    """
    if prior == 0:
        return float("inf") if current > 0 else None
    return current / prior


@dataclass
class GrowthEntry:
    """One growth-based Tier 2 pool's live bookkeeping.
    `hot_reason` is fixed at promotion time and does not change while
    the pool stays hot -- even if, say, a volume_spike-promoted pool
    later also crosses the TVL threshold, the reported reason stays
    "volume_spike" for this hot episode. This is a deliberate choice,
    not something the design doc spells out explicitly: hot_reason's
    whole purpose (section 3.2) is segmented analysis of "what kind of
    event brought this pool to our attention", and a label that
    flip-flops cycle to cycle between spike types would undermine that
    more than it would add information. A pool demoted and later
    re-promoted for a different reason gets a fresh GrowthEntry with
    the new reason -- that's a new episode, not a continuation.
    """

    hot_reason: str
    consecutive_below_threshold: int = 0


class AgeBasedRegistry:
    """venue+pool_address -> expires_at (first_seen_ts + 72h). Pure
    time-based membership: `is_hot()` is just "have we recorded this
    key with an expiry in the future", no re-evaluation of the pool's
    actual activity at all, per section 3.2's explicit instruction not
    to demote these early.
    """

    def __init__(self) -> None:
        self._expires_at: dict[tuple[str, str], datetime.datetime] = {}

    def seed_from_history(self, client, now: datetime.datetime) -> int:
        """Rebuilds this registry from new_entity_events on scheduler
        startup -- see module docstring on why this is possible for
        age-based state (permanent, no-TTL history) but not
        growth-based state. Returns the number of entries seeded.
        """
        cutoff = now - AGE_BASED_WINDOW
        result = client.query(
            "SELECT venue, identifier, detected_ts FROM new_entity_events "
            "WHERE entity_type = {entity_type:String} AND detected_ts > {cutoff:DateTime64(3)}",
            parameters={"entity_type": ENTITY_TYPE_DEX_POOL, "cutoff": cutoff},
        )
        for venue, identifier, detected_ts in result.result_rows:
            # clickhouse-connect returns naive datetimes for
            # DateTime64 columns (no tzinfo attached), while every
            # timestamp this module otherwise handles comes straight
            # from datetime.now(timezone.utc) in scheduler.py (aware).
            # This table is written exclusively with UTC-aware
            # timestamps (detect_new_entities always receives one),
            # so re-attaching UTC here is a correct, not just
            # convenient, fix -- not a guess -- and keeps every value
            # in this dict comparably aware, avoiding
            # "can't compare offset-naive and offset-aware datetimes"
            # against prune_expired()'s now.
            if detected_ts.tzinfo is None:
                detected_ts = detected_ts.replace(tzinfo=datetime.timezone.utc)
            self._expires_at[(venue, identifier)] = detected_ts + AGE_BASED_WINDOW
        log(
            Level.INFO,
            [("event", "age_based_seeded"), ("count", str(len(self._expires_at))), ("cutoff", cutoff.isoformat())],
            src=TIER2_SRC,
        )
        return len(self._expires_at)

    def promote(self, venue: str, identifiers: list[str], detected_ts: datetime.datetime) -> None:
        if not identifiers:
            return
        for ident in identifiers:
            self._expires_at[(venue, ident)] = detected_ts + AGE_BASED_WINDOW
        log(
            Level.INFO,
            [
                ("event", "age_based_promoted"),
                ("venue", venue),
                ("count", str(len(identifiers))),
                ("expires_at", (detected_ts + AGE_BASED_WINDOW).isoformat()),
            ],
            src=TIER2_SRC,
        )

    def prune_expired(self, now: datetime.datetime) -> list[tuple[str, str]]:
        """Removes entries whose 72h window has passed. Returns the
        (venue, pool_address) keys that expired this call, purely for
        logging -- there is no "demotion decision" here, just a clock
        comparison; section 3.2 is explicit that these are never
        re-evaluated against activity.
        """
        expired = [key for key, expires_at in self._expires_at.items() if expires_at <= now]
        for key in expired:
            del self._expires_at[key]
        if expired:
            log(Level.INFO, [("event", "age_based_expired"), ("count", str(len(expired)))], src=TIER2_SRC)
        return expired

    def is_hot(self, venue: str, pool_address: str) -> bool:
        return (venue, pool_address) in self._expires_at

    def __len__(self) -> int:
        return len(self._expires_at)


class GrowthBasedRegistry:
    """venue+pool_address -> GrowthEntry. Re-evaluated once per Tier 1
    cycle via `evaluate()`; see that method for the promotion/
    hysteresis/demotion logic.
    """

    def __init__(self) -> None:
        self._entries: dict[tuple[str, str], GrowthEntry] = {}

    def is_hot(self, venue: str, pool_address: str) -> bool:
        return (venue, pool_address) in self._entries

    def hot_reason(self, venue: str, pool_address: str) -> Optional[str]:
        entry = self._entries.get((venue, pool_address))
        return entry.hot_reason if entry else None

    def __len__(self) -> int:
        return len(self._entries)

    def fetch_prior_snapshot(self, client, venue: str, snapshot_ts: datetime.datetime) -> dict[str, tuple[float, float]]:
        """Pulls the immediately-preceding Tier 1 cycle's kept rows for
        this venue (pool_address -> (volume_24h_usd, tvl_usd)) --
        "immediately preceding" meaning the venue's own latest
        snapshot_ts strictly before this cycle's, not a fixed 5-minute
        lookback, so this is correct even if a prior cycle ran late or
        a source failed and skipped a cycle entirely. Only ever reads
        the *kept* (non-ghost) rows dex_pool_snapshots actually holds
        (thousands per venue, not the full unfiltered universe REST
        pulls scan), so this is a cheap local ClickHouse scan, not
        remotely comparable in cost to the external REST calls that
        produced `snapshot_ts`'s own rows.
        """
        result = client.query(
            "SELECT pool_address, volume_24h_usd, tvl_usd FROM dex_pool_snapshots "
            "WHERE venue = {venue:String} AND snapshot_ts = ("
            "  SELECT max(snapshot_ts) FROM dex_pool_snapshots "
            "  WHERE venue = {venue:String} AND snapshot_ts < {snapshot_ts:DateTime64(3)}"
            ")",
            parameters={"venue": venue, "snapshot_ts": snapshot_ts},
        )
        return {row[0]: (row[1], row[2]) for row in result.result_rows}

    def evaluate(self, client, venue: str, current_rows: list[dict], snapshot_ts: datetime.datetime) -> dict[str, list]:
        """Re-scores every currently-kept pool in `current_rows`
        against its prior Tier 1 cycle (section 3.2: "re-evaluira se
        na svaki Tier 1 ciklus"), using the simple OR rule (section
        3.2's explicit Phase-1 substitute for the full weighted
        priority_score): volume_growth_24h > GROWTH_VOLUME_THRESHOLD
        OR tvl_growth_24h > GROWTH_TVL_THRESHOLD.

        Pools with no prior row at all (this is their first-ever
        appearance) are skipped here entirely -- that's exactly what
        AgeBasedRegistry.promote() already covers for a pool's first
        cycle; growth-based scoring needs a baseline to compare
        against and genuinely has nothing to say about a pool that
        didn't exist in the prior snapshot.

        Returns {"promoted": [...], "demoted": [...], "still_hot": [...]}
        as (venue, pool_address, hot_reason) / (venue, pool_address)
        tuples, for logging and reporting -- callers that don't need
        the detail can just inspect the lengths.
        """
        prior = self.fetch_prior_snapshot(client, venue, snapshot_ts)
        promoted: list[tuple[str, str, str]] = []
        demoted: list[tuple[str, str]] = []
        still_hot: list[tuple[str, str]] = []

        for row in current_rows:
            addr = row["pool_address"]
            prior_row = prior.get(addr)
            if prior_row is None:
                continue  # first-ever appearance -- age-based's territory, not growth-based's
            prior_volume, prior_tvl = prior_row
            volume_growth = _safe_growth(row["volume_24h_usd"], prior_volume)
            tvl_growth = _safe_growth(row["tvl_usd"], prior_tvl)
            volume_triggered = volume_growth is not None and volume_growth > GROWTH_VOLUME_THRESHOLD
            tvl_triggered = tvl_growth is not None and tvl_growth > GROWTH_TVL_THRESHOLD
            triggered = volume_triggered or tvl_triggered

            key = (venue, addr)
            existing = self._entries.get(key)

            if triggered:
                if existing is None:
                    # volume checked first: section 3.2's OR rule lists
                    # volume_growth before tvl_growth, and in the (rare)
                    # case both cross their threshold in the same
                    # cycle, volume is the more directly liquidity-
                    # relevant of the two for this project's CEX-DEX
                    # arbitrage angle -- an arbitrary but documented
                    # tiebreak, not a meaningful ranking claim.
                    reason = HOT_REASON_VOLUME_SPIKE if volume_triggered else HOT_REASON_TVL_SPIKE
                    self._entries[key] = GrowthEntry(hot_reason=reason)
                    promoted.append((venue, addr, reason))
                else:
                    existing.consecutive_below_threshold = 0
                    still_hot.append((venue, addr))
            else:
                if existing is not None:
                    existing.consecutive_below_threshold += 1
                    if existing.consecutive_below_threshold >= DEMOTION_CONSECUTIVE_CYCLES:
                        del self._entries[key]
                        demoted.append((venue, addr))
                    else:
                        still_hot.append((venue, addr))
                # else: not hot, not triggered -- nothing to do, the
                # common case for the overwhelming majority of pools.

        if promoted or demoted:
            log(
                Level.INFO,
                [
                    ("event", "growth_based_rescored"),
                    ("venue", venue),
                    ("promoted", str(len(promoted))),
                    ("demoted", str(len(demoted))),
                    ("still_hot", str(len(still_hot))),
                ],
                src=TIER2_SRC,
            )

        return {"promoted": promoted, "demoted": demoted, "still_hot": still_hot}

    def apply_new_listing(self, venue: str, pool_address: str) -> bool:
        """Promotes (venue, pool_address) with hot_reason='new_listing'
        if not already hot, or resets its hysteresis counter (treats
        it as "still triggering" this cycle) if it already is. Returns
        True if this was a fresh promotion, False if it just refreshed
        an existing entry. See tier2.py's apply_new_listing_promotions
        (module-level, in scheduler.py's call path) for the base_asset/
        base_symbol matching this is fed from, and that function's own
        docstring for the matching's documented limitations.
        """
        key = (venue, pool_address)
        existing = self._entries.get(key)
        if existing is None:
            self._entries[key] = GrowthEntry(hot_reason=HOT_REASON_NEW_LISTING)
            return True
        existing.consecutive_below_threshold = 0
        return False


@dataclass
class Tier2State:
    """Top-level container gluing AgeBasedRegistry + GrowthBasedRegistry
    together for the one thing scheduler.py actually needs from either
    of them at row-write time: "what should this pool's (tier,
    hot_reason) be right now". Age-based takes priority when both
    registries happen to consider the same pool hot at once (a pool
    can be simultaneously within its 72h new-pool window AND
    independently triggering a growth spike) -- documented here rather
    than left as an unstated tiebreak: while a pool is in its fixed
    "new" window, that's treated as its primary, most informative
    reason; the growth-based registry keeps tracking it in parallel
    regardless (see GrowthBasedRegistry.evaluate()'s docstring), so if
    the pool is *still* triggering growth once the 72h window expires,
    it seamlessly continues in Tier 2 under the growth-based reason
    instead of falling back to 'discovery'.
    """

    age_based: AgeBasedRegistry = field(default_factory=AgeBasedRegistry)
    growth_based: GrowthBasedRegistry = field(default_factory=GrowthBasedRegistry)

    def tier_and_reason(self, venue: str, pool_address: str) -> tuple[str, str]:
        if self.age_based.is_hot(venue, pool_address):
            return TIER_HOT, HOT_REASON_NEW_POOL
        growth_reason = self.growth_based.hot_reason(venue, pool_address)
        if growth_reason is not None:
            return TIER_HOT, growth_reason
        return TIER_DISCOVERY, ""

    def current_hot_pools(self) -> set[tuple[str, str]]:
        """Union of both registries' current membership -- what
        Tier 2's 1-minute poller (tier2_poller.py) actually polls.
        """
        hot = set(self.age_based._expires_at.keys())  # noqa: SLF001 -- same module, deliberate internal access
        hot.update(self.growth_based._entries.keys())  # noqa: SLF001
        return hot


def latest_dex_pools_by_base_symbol(client) -> dict[str, list[tuple[str, str]]]:
    """base_symbol -> [(venue, pool_address), ...], built from each
    venue's own latest snapshot_ts (same "latest per venue" pattern
    run_entity_detection.py uses). Feeds apply_new_listing_promotions
    below -- kept as a separate query/function rather than folded into
    that one, since a caller doing several new-listing checks across
    one cycle's CEX exchanges should build this lookup exactly once,
    not re-query it per exchange.
    """
    result = client.query(
        """
        SELECT base_symbol, venue, pool_address
        FROM dex_pool_snapshots
        WHERE (venue, snapshot_ts) IN (
            SELECT venue, max(snapshot_ts) FROM dex_pool_snapshots GROUP BY venue
        )
        """
    )
    by_symbol: dict[str, list[tuple[str, str]]] = {}
    for base_symbol, venue, pool_address in result.result_rows:
        if not base_symbol:
            continue  # section 3.7's "" placeholder for untagged tokens -- not a real symbol to match against
        by_symbol.setdefault(base_symbol, []).append((venue, pool_address))
    return by_symbol


def apply_new_listing_promotions(
    growth_based: GrowthBasedRegistry,
    new_cex_symbols: list[tuple[str, str]],
    dex_pools_by_symbol: dict[str, list[tuple[str, str]]],
) -> list[tuple[str, str]]:
    """For every newly-detected CEX symbol (exchange, base_asset) this
    cycle, promotes every DEX pool whose base_symbol string-equals that
    base_asset. Returns the (venue, pool_address) pairs freshly
    promoted this call (already-hot pools that just got their
    hysteresis counter refreshed are not included, matching
    GrowthBasedRegistry.apply_new_listing's own True/False contract).

    Documented limitation, deliberately not solved here (per this
    session's explicit scope): this is base_asset == base_symbol
    *string* equality, nothing more -- no mint-address verification, no
    awareness of wrapped-token variants, and no resolution of ticker
    collisions (two genuinely different tokens across chains/venues
    that happen to share a symbol, e.g. a bridged token vs. an
    unrelated project using the same ticker, would both match). That
    real problem is Phase 2's full token taxonomy (see
    docs/mev-discovery-plan.md section 4, "Token mint <-> CEX ticker
    matching"), not something to half-build here. A false-positive
    new_listing promotion under this scheme costs at most a handful of
    extra 1-minute polls on an unrelated pool for a while -- annoying,
    not corrupting (the fact tables still record exactly what was
    observed; only Tier 2's attention allocation is briefly misled).
    """
    promoted: list[tuple[str, str]] = []
    for _exchange, base_asset in new_cex_symbols:
        for venue, pool_address in dex_pools_by_symbol.get(base_asset, []):
            if growth_based.apply_new_listing(venue, pool_address):
                promoted.append((venue, pool_address))
    return promoted
