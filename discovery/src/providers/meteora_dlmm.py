"""MeteoraDlmmRestProvider: Meteora's DLMM (bin-based concentrated
liquidity) pools.

Deliberately NOT built on raydium_common.py's RaydiumApiV3RestProviderBase,
per this session's explicit instruction to only share code where the
API shape genuinely matches. It doesn't, in several structural ways
(see below) -- forcing a shared base here would mean more conditional
branching in the base class than code actually saved. Shares only what
is genuinely venue-agnostic: http_client.build_session() and
normalize.canonicalize()/TokenSide.

Confirmed empirically (2026-08-03) against the live API:
- The commonly-documented base URL (dlmm-api.meteora.ag, endpoint
  /pair/all) 404s as of this writing -- Meteora appears to have
  migrated its public API to a new host, `dlmm.datapi.meteora.ag`,
  without a redirect from the old one. Found by locating a newer
  reference to "dlmm.datapi.meteora.ag" and confirming it responds
  ({"service":"Meteora DLMM API","status":"ok"} at the bare root).
- GET /pools -- paginated list. Query params are `page`/`page_size`
  (up to at least 1000 tested), response shape
  `{total, pages, current_page, page_size, data: [...]}` -- page-
  number pagination like Raydium's list endpoint, but a different
  termination signal (`current_page >= pages`, not a `hasNextPage`
  boolean).
- GET /pools/{address} -- single pool detail, returns the pool object
  directly at the top level (not wrapped in `{data: ...}` the way
  Raydium's /pools/info/ids is). No multi-address batch endpoint
  exists (`/pools/multi` returns 400) -- unlike Raydium, there is no
  way to fetch several specific pools in one call, so get_pools below
  is one request per address, each failure isolated to that one pool
  rather than poisoning a whole batch (the opposite failure mode from
  Raydium's ids endpoint).
- A `query` parameter substring-filters by pool name (e.g.
  `query=SOL-USDC` narrows a 122k-pool universe to the ~123 pools
  actually named "SOL-USDC") -- not documented anywhere found, but
  confirmed empirically and used by run_meteora_snapshot.py to find
  the validation pool cheaply instead of walking the full universe.
- Field shape is genuinely different from Orca/Raydium: tokens nest
  under `token_x`/`token_y` (not `tokenA`/`tokenB` or `mintA`/`mintB`),
  each carrying its own `price` (the provider's own USD mark for that
  token, alongside `symbol`/`decimals`/`address`). `current_price` is
  token_x's price in token_y units -- same directional convention as
  Orca/Raydium's price fields, so normalize.canonicalize() applies
  unchanged treating token_x as "token_a" and token_y as "token_b".
- Roughly 80%+ of pools past the first couple of TVL-heavy pages have
  `current_price == 0.0` (with tvl/volume/reserves all 0 too) --
  simply abandoned, empty-bin pools, not a parsing issue. These hit
  the same non-positive-price skip path Orca/Raydium already use for
  their (much rarer) zero-price pools; expect far more parse_skip WARN
  lines here than for those two venues during a broad pull. That's
  normal for this venue, not a regression.
- Missing symbol happens here too (10/10000 sampled, ~0.1% -- smaller
  than Orca/Raydium's ~0.8% but present), same `symbol or ""` handling
  applies.

Rate limiting (Phase 1f retrofit, docs/mev-discovery-plan.md section
5.1/5.4): Meteora is the one DEX venue with a clearly published, fixed
number -- 30 req/s globally per the DLMM API, section 5.1's table
notes it as the only DEX with a clearly published number, easiest to
plan capacity for. Margined to 90% per section 5.4 point 1:
BUCKET_CAPACITY = 30 * 0.9 = 27 req/s. A full unbounded broad pull is
only ~24-26 pages (this venue's universe is ~1700-1800 kept pools at
page_size=1000, plus ghosts on the same pages -- pagination is by
page, not by kept-row count, so page count tracks total pool count,
not post-filter count), so this bucket -- like Orca's and Raydium's --
sits far above the provider's natural pull rate and never meaningfully
throttles a normal cycle; it's a safety ceiling, not an active
constraint at Tier 1's polling frequency.

Fee semantics (see FEE_NOTE below for the full reasoning): DLMM
exposes two separate fee numbers per pool -- `pool_config.base_fee_pct`
(the pool's static fee tier, fixed at pool creation, tied to its
bin_step) and `dynamic_fee_pct` (a volatility-driven variable
component that changes continuously). `fee_bps` here stores ONLY
base_fee_pct.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Optional, Sequence

import requests

from discovery.http_client import DEFAULT_TIMEOUT_S, build_session
from discovery.rate_limiter import TokenBucket
from discovery.src.logger import Level, log
from discovery.src.normalize import TokenSide, canonicalize
from discovery.src.providers.base import PoolMetadata, PoolState, PoolVolume

API_BASE = "https://dlmm.datapi.meteora.ag"
DEFAULT_PAGE_SIZE = 1000
REQUEST_TIMEOUT_S = DEFAULT_TIMEOUT_S

VENUE = "meteora_dlmm"

# Section 5.1's published 30 req/s, margined to 90% per section 5.4
# point 1 -- see module docstring's "Rate limiting" paragraph.
PUBLISHED_LIMIT_PER_S = 30
SAFETY_MARGIN = 0.90
BUCKET_CAPACITY = PUBLISHED_LIMIT_PER_S * SAFETY_MARGIN
BUCKET_REFILL_PERIOD_S = 1.0

# raw["pool_config"]["base_fee_pct"] is a percent number (0.04 == 0.04%
# == 4 bps, 0.25 == 0.25% == 25 bps); fee_bps is out of 10_000, so
# *100 converts one to the other. Verified against SOL/USDC's tightest
# DLMM pool (base_fee_pct=0.04 -> 4 bps, bin_step=4) and its
# base_fee_pct=0.01/bin_step=1 sibling pool (-> 1 bp).
BASE_FEE_PCT_TO_BPS_MULTIPLIER = 100

# fee_bps stores the STATIC fee tier (pool_config.base_fee_pct), not
# the instantaneous dynamic_fee_pct, and this is a deliberate choice,
# not an oversight: Orca and Raydium's fee_bps is a structural,
# unchanging venue/pool characteristic (which fee tier this pool was
# created with), and storing dynamic_fee_pct instead would break that
# cross-venue consistency two ways -- (1) Orca/Raydium have no
# equivalent instantaneous-fee concept to compare it against, so a
# dynamic_fee_pct-based fee_bps would be measuring something
# structurally different from what fee_bps means everywhere else in
# this table, and (2) the same pool's fee_bps would drift between
# otherwise-identical snapshot cycles purely from market volatility,
# not from any actual change to the pool's fee tier -- exactly the
# kind of cross-snapshot noise `fee_bps` (a LowCardinality-friendly,
# effectively-categorical column) isn't meant to carry. The realized
# swap fee at any given instant is closer to
# base_fee_pct + dynamic_fee_pct; if Faza 2/3 ever need that
# instantaneous total, it belongs in a new, separate field, not a
# reinterpretation of this one.
FEE_NOTE = "fee_bps = pool_config.base_fee_pct only, not dynamic_fee_pct -- see module docstring"


@dataclass(frozen=True)
class MeteoraPoolRow:
    """Everything needed for one dex_pool_snapshots row, minus the
    fields (snapshot_ts, tier, hot_reason) that belong to the calling
    scheduler cycle rather than to the pool itself. Same shape as
    OrcaPoolRow/RaydiumPoolRow, kept as its own type since (per this
    module's docstring) the parsing logic behind it doesn't actually
    share anything beyond canonicalize() with either.
    """

    pool_address: str
    base_mint: str
    base_symbol: str
    quote_mint: str
    quote_symbol: str
    fee_bps: int
    tvl_usd: float
    active_liquidity: float
    volume_24h_usd: float
    price_native: float
    price_usd: Optional[float]

    def to_metadata(self) -> PoolMetadata:
        return PoolMetadata(
            venue=VENUE,
            pool_address=self.pool_address,
            base_mint=self.base_mint,
            base_symbol=self.base_symbol,
            quote_mint=self.quote_mint,
            quote_symbol=self.quote_symbol,
            fee_bps=self.fee_bps,
        )

    def to_state(self) -> PoolState:
        return PoolState(
            venue=VENUE,
            pool_address=self.pool_address,
            price_native=self.price_native,
            active_liquidity=self.active_liquidity,
        )

    def to_volume(self) -> PoolVolume:
        return PoolVolume(
            venue=VENUE,
            pool_address=self.pool_address,
            tvl_usd=self.tvl_usd,
            volume_24h_usd=self.volume_24h_usd,
        )

    def to_snapshot_row(self, snapshot_ts, tier: str, hot_reason: str) -> dict:
        return {
            "snapshot_ts": snapshot_ts,
            "venue": VENUE,
            "pool_address": self.pool_address,
            "base_mint": self.base_mint,
            "base_symbol": self.base_symbol,
            "quote_mint": self.quote_mint,
            "quote_symbol": self.quote_symbol,
            "fee_bps": self.fee_bps,
            "tvl_usd": self.tvl_usd,
            "active_liquidity": self.active_liquidity,
            "volume_24h_usd": self.volume_24h_usd,
            "price_native": self.price_native,
            "price_usd": self.price_usd,
            "tier": tier,
            "hot_reason": hot_reason,
        }


def _parse_raw_pool(raw: dict) -> MeteoraPoolRow:
    pool_address = raw["address"]
    token_x, token_y = raw["token_x"], raw["token_y"]
    # Same null/empty-symbol handling as Orca/Raydium, and for the same
    # reason (section 3.7's "widen, don't filter" -- untagged
    # long-tail tokens stay in, coerced to "" rather than dropped or
    # left as None, which would fail the non-nullable
    # LowCardinality(String) insert).
    token_a = TokenSide(mint=token_x["address"], symbol=token_x.get("symbol") or "")
    token_b = TokenSide(mint=token_y["address"], symbol=token_y.get("symbol") or "")

    price_x_in_y = float(raw["current_price"])
    if price_x_in_y <= 0:
        # The common case for this venue (see module docstring) -- an
        # abandoned pool with no liquidity in any bin, not a parsing
        # bug.
        raise ValueError(f"non-positive price: {price_x_in_y}")

    base, quote, price_native = canonicalize(token_a, token_b, price_x_in_y)

    pool_config = raw.get("pool_config") or {}
    fee_bps = round(float(pool_config["base_fee_pct"]) * BASE_FEE_PCT_TO_BPS_MULTIPLIER)
    tvl_usd = float(raw["tvl"])
    volume_24h_usd = float((raw.get("volume") or {}).get("24h") or 0.0)

    # Same reasoning as the other providers: only populate price_usd
    # when the canonicalized quote side is itself a stablecoin.
    price_usd = price_native if quote.symbol in {"USDC", "USDT"} else None

    return MeteoraPoolRow(
        pool_address=pool_address,
        base_mint=base.mint,
        base_symbol=base.symbol,
        quote_mint=quote.mint,
        quote_symbol=quote.symbol,
        fee_bps=fee_bps,
        tvl_usd=tvl_usd,
        # TODO(Phase 1h): replace with true active_liquidity
        # (likvidnost in the currently active bin range), decoded from
        # raw on-chain bin-array state -- that's the Tier 2 RPC
        # PoolStateProvider's job (section 3.3), not built yet. DLMM
        # pools genuinely do have a concentrated bin-range concept
        # analogous to CLMM's tick range (liquidity can sit far from
        # tvl_usd, concentrated in a narrow band around the active
        # bin, or spread thin across a wide one) -- so this is a real,
        # temporary placeholder here, same treatment as
        # RaydiumClmmRestProvider's, NOT the AMM v4 correct-by-design
        # case.
        active_liquidity=tvl_usd,
        volume_24h_usd=volume_24h_usd,
        price_native=price_native,
        price_usd=price_usd,
    )


class MeteoraDlmmRestProvider:
    """Implements PoolMetadataProvider, PoolStateProvider, and
    PoolVolumeProvider against Meteora's DLMM REST API.
    """

    def __init__(self, session: Optional[requests.Session] = None, bucket: Optional[TokenBucket] = None):
        self._session = session or build_session()
        self._bucket = bucket or TokenBucket(BUCKET_CAPACITY, BUCKET_REFILL_PERIOD_S)

    def get_pool(self, pool_address: str) -> MeteoraPoolRow:
        self._bucket.consume()
        resp = self._session.get(
            f"{API_BASE}/pools/{pool_address}", timeout=REQUEST_TIMEOUT_S
        )
        resp.raise_for_status()
        return _parse_raw_pool(resp.json())

    def get_pools(self, pool_addresses: Sequence[str]) -> list[MeteoraPoolRow]:
        """No batch endpoint exists (confirmed: /pools/multi 400s) --
        one request per address. Each failure (network or parse) is
        isolated to that one pool, unlike Raydium's ids endpoint where
        one malformed id fails its whole chunk.
        """
        rows: list[MeteoraPoolRow] = []
        for addr in pool_addresses:
            try:
                rows.append(self.get_pool(addr))
            except (requests.HTTPError, KeyError, TypeError, ValueError, ZeroDivisionError) as exc:
                log(
                    Level.WARN,
                    [("event", "parse_skip"), ("pool", addr), ("err", str(exc))],
                )
        return rows

    def list_pools(
        self,
        page_size: int = DEFAULT_PAGE_SIZE,
        max_pages: Optional[int] = None,
        query: Optional[str] = None,
    ) -> Iterator[MeteoraPoolRow]:
        """Walks the full paginated pool list, terminated by
        `current_page >= pages` (this API doesn't expose a
        `hasNextPage`-style boolean). `query` substring-filters by
        pool name server-side (see module docstring) -- useful for
        finding a specific pair cheaply instead of walking the full
        ~122k-pool universe.
        """
        page = 1
        pages_fetched = 0
        while True:
            params: dict = {"page_size": page_size, "page": page}
            if query:
                params["query"] = query
            self._bucket.consume()
            resp = self._session.get(
                f"{API_BASE}/pools", params=params, timeout=REQUEST_TIMEOUT_S
            )
            resp.raise_for_status()
            body = resp.json()
            for raw in body.get("data", []):
                try:
                    yield _parse_raw_pool(raw)
                except (KeyError, TypeError, ValueError, ZeroDivisionError) as exc:
                    log(
                        Level.WARN,
                        [
                            ("event", "parse_skip"),
                            ("pool", str(raw.get("address", "?"))),
                            ("err", str(exc)),
                        ],
                    )
            pages_fetched += 1
            total_pages = body.get("pages", page)
            page += 1
            if page > total_pages or (max_pages is not None and pages_fetched >= max_pages):
                break

    # --- Protocol-conforming views -----------------------------------

    def fetch_metadata(self) -> Sequence[PoolMetadata]:
        return [row.to_metadata() for row in self.list_pools()]

    def fetch_state(self, pool_addresses: Sequence[str]) -> Sequence[PoolState]:
        return [row.to_state() for row in self.get_pools(pool_addresses)]

    def fetch_volume(self) -> Sequence[PoolVolume]:
        return [row.to_volume() for row in self.list_pools()]
