"""OrcaRestProvider: the one concrete provider built in Phase 1a.

Orca's `/v2/solana/pools` REST endpoint returns mints/fee tier
(metadata), `price`/on-chain `liquidity` (state), and `stats.24h.volume`
/`tvlUsdc` (volume) in a single response -- so, per the note in
providers/base.py, this class implements all three provider Protocols
against one shared HTTP layer instead of three independent ones.

Confirmed empirically (2026-08-03) against the live API:
- Requests without a browser-like User-Agent get a 403 from Cloudflare
  (bare urllib's default UA is blocked; curl and a requests.Session
  with an explicit UA both work) -- see discovery/http_client.py,
  which fixes this once for all REST providers, not just this one.
- Pagination is cursor-based: `meta.cursor.next` from one page is
  passed back as the `after` query param for the next.
- The full Whirlpool universe is ~142k pools across ~142 pages at
  size=1000, ~85s wall-clock for a full walk. That's an accepted cost
  for this phase's one-shot script, not something to optimize yet.

Rate limiting (Phase 1f retrofit, docs/mev-discovery-plan.md section
5.1/5.4): Orca is the one DEX venue with no published rate limit at
all -- section 5.1's table lists it as "Nedefinisan broj, IP-implicitan
... Osloniti se na exponential backoff pri 429" (undefined, IP-implicit;
rely on exponential backoff at 429, which discovery/http_client.py's
build_session() already provides). DEFAULT_BUCKET_CAPACITY below is
therefore a judgment call, not a documented limit being margined down
the way Meteora's or Raydium's is -- picked conservatively (a handful
of req/s) specifically so it sits *above* this provider's natural
pull rate (one full unbounded walk is ~100-120 pages over ~60-90s,
i.e. roughly 1.3-2 req/s sustained) rather than actively throttling
normal operation. It exists as a safety ceiling against a pathological
case (e.g. a bug causing a tight retry loop), not as a cap this
provider is expected to bump against in practice. Same TokenBucket
mechanism and per-provider-instance ownership pattern as every CEX
provider (see e.g. binance_symbols.py) -- one call costs one token,
no per-endpoint weight concept exists for this API the way Binance's
request-weight does.
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

API_BASE = "https://api.orca.so/v2/solana"
DEFAULT_PAGE_SIZE = 1000
REQUEST_TIMEOUT_S = DEFAULT_TIMEOUT_S

VENUE = "orca_whirlpool"

# No published limit for this venue (see module docstring) -- a
# conservative default, not a margined-down documented number. 5 req/s
# sustained comfortably clears this provider's observed natural pace
# (~1.3-2 req/s during a full broad pull) without becoming the
# cycle's bottleneck.
DEFAULT_BUCKET_CAPACITY = 5.0
DEFAULT_BUCKET_REFILL_PERIOD_S = 1.0

# Orca's feeRate is out of FEE_RATE_MUL_VALUE = 1_000_000; fee_bps is
# out of 10_000. feeRate / 100 converts one to the other. Verified
# against the KMNO/USDC pool (feeRate=100 -> 1 bp, matching the doc's
# documented "1bp fee pool") and SOL/USDC (feeRate=400 -> 4 bps).
FEE_RATE_TO_BPS_DIVISOR = 100


@dataclass(frozen=True)
class OrcaPoolRow:
    """Everything needed for one dex_pool_snapshots row, minus the
    fields (snapshot_ts, tier, hot_reason) that belong to the calling
    scheduler cycle rather than to the pool itself.
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


def _parse_raw_pool(raw: dict) -> OrcaPoolRow:
    pool_address = raw["address"]
    # tokenMintA/B are always present (they're the pool's actual on-chain
    # addresses), but symbol is provider-side metadata that's null for
    # ~0.8% of pools (unlisted/untagged tokens -- confirmed empirically
    # across a 20k-pool sample). Per section 3.7's "widen, don't filter"
    # stance these long-tail pools stay in, same as every other pool --
    # coerced to "" rather than None, consistent with the schema's own
    # empty-categorical convention (hot_reason's '' for "no reason").
    # None would otherwise reach dex_pool_snapshots's non-nullable
    # LowCardinality(String) columns and fail the whole batch insert.
    token_a = TokenSide(mint=raw["tokenMintA"], symbol=raw["tokenA"]["symbol"] or "")
    token_b = TokenSide(mint=raw["tokenMintB"], symbol=raw["tokenB"]["symbol"] or "")

    price_a_in_b = float(raw["price"])
    if price_a_in_b <= 0:
        raise ValueError(f"non-positive price: {price_a_in_b}")

    base, quote, price_native = canonicalize(token_a, token_b, price_a_in_b)

    fee_bps = round(raw["feeRate"] / FEE_RATE_TO_BPS_DIVISOR)
    tvl_usd = float(raw["tvlUsdc"])
    volume_24h_usd = float((raw.get("stats", {}).get("24h", {}) or {}).get("volume") or 0.0)

    # price_usd is only populated when it's cheap/honest to do so: if
    # the canonicalized quote side is itself a stablecoin, price_native
    # already *is* a USD price (quote ~= $1) without needing any
    # separate provider-side conversion. Otherwise left NULL rather
    # than fabricating one -- see the schema's note on why price_usd
    # must never get silently backed into spread computation.
    price_usd = price_native if quote.symbol in {"USDC", "USDT"} else None

    return OrcaPoolRow(
        pool_address=pool_address,
        base_mint=base.mint,
        base_symbol=base.symbol,
        quote_mint=quote.mint,
        quote_symbol=quote.symbol,
        fee_bps=fee_bps,
        tvl_usd=tvl_usd,
        # tvl_usd placeholder, still real for THIS row: Tier 1's own
        # REST pull never decodes raw on-chain `liquidity` (Orca's API
        # doesn't expose it as a distinct field, and there's no reason
        # for a 5-minute-cadence baseline pull to pay for an extra RPC
        # round-trip just for this). True active_liquidity (likvidnost
        # in the currently active tick range) IS now decoded, but only
        # for Tier 2 hot pools, via solana_rpc_state.py's RPC
        # PoolStateProvider (Phase 1h, section 3.3) -- see
        # tier2_poller.py's _poll_venue_via_rpc, which overwrites this
        # placeholder with the true value before writing a Tier-2-hot
        # pool's row. A pool that's never promoted to Tier 2 keeps this
        # placeholder in every row it ever gets, same as before Phase
        # 1h -- acceptable because Phase 1a/1g don't act on this field
        # outside Tier 2, not because the two are actually equivalent
        # for a concentrated-liquidity pool.
        active_liquidity=tvl_usd,
        volume_24h_usd=volume_24h_usd,
        price_native=price_native,
        price_usd=price_usd,
    )


class OrcaRestProvider:
    """Implements PoolMetadataProvider, PoolStateProvider, and
    PoolVolumeProvider against Orca's Whirlpool REST API.
    """

    def __init__(self, session: Optional[requests.Session] = None, bucket: Optional[TokenBucket] = None):
        self._session = session or build_session()
        self._bucket = bucket or TokenBucket(DEFAULT_BUCKET_CAPACITY, DEFAULT_BUCKET_REFILL_PERIOD_S)

    def get_pool(self, pool_address: str) -> OrcaPoolRow:
        self._bucket.consume()
        resp = self._session.get(
            f"{API_BASE}/pools/{pool_address}", timeout=REQUEST_TIMEOUT_S
        )
        resp.raise_for_status()
        return _parse_raw_pool(resp.json()["data"])

    def get_pools(self, pool_addresses: Sequence[str]) -> list[OrcaPoolRow]:
        """No multi-address batch endpoint exists for this venue
        (same situation as Meteora's get_pools -- see that method's
        docstring) -- one GET per address, each failure isolated to
        that one pool rather than poisoning a whole batch. Added in
        Phase 1g for Tier 2's 1-minute polling (docs/mev-discovery-plan.md
        section 3.2), which needs to re-fetch a small, specific set of
        currently-hot pool_addresses on every poll -- get_pool alone
        would work but forces the caller to loop and catch exceptions
        itself; centralizing that here matches every other provider's
        get_pools shape (RaydiumApiV3RestProviderBase.get_pools,
        MeteoraDlmmRestProvider.get_pools).
        """
        rows: list[OrcaPoolRow] = []
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
        self, page_size: int = DEFAULT_PAGE_SIZE, max_pages: Optional[int] = None
    ) -> Iterator[OrcaPoolRow]:
        """Walks the full paginated Whirlpool list. Skips (with a
        WARN log) any pool whose payload doesn't parse cleanly, rather
        than aborting the whole pull for one bad record -- matches
        section 5.4's "one source's problem shouldn't block the rest"
        principle, applied at the record level here.
        """
        cursor = None
        pages = 0
        while True:
            params: dict = {"size": page_size}
            if cursor:
                params["after"] = cursor
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
            pages += 1
            cursor = (body.get("meta", {}).get("cursor", {}) or {}).get("next")
            if not cursor or (max_pages is not None and pages >= max_pages):
                break

    # --- Protocol-conforming views -----------------------------------

    def fetch_metadata(self) -> Sequence[PoolMetadata]:
        return [row.to_metadata() for row in self.list_pools()]

    def fetch_state(self, pool_addresses: Sequence[str]) -> Sequence[PoolState]:
        return [self.get_pool(addr).to_state() for addr in pool_addresses]

    def fetch_volume(self) -> Sequence[PoolVolume]:
        return [row.to_volume() for row in self.list_pools()]
