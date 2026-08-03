"""Shared plumbing for Raydium api-v3 REST providers.

AMM v4 (Standard, constant-product) and CLMM (Concentrated) pools come
through the exact same api-v3 endpoints -- /pools/info/ids and
/pools/info/list -- filtered by `poolType`, with an identical response
shape (mints, price, feeRate, tvl, day.volume). The two pool types
differ in exactly one place that matters to this codebase: what
active_liquidity means (see each provider module's
_compute_active_liquidity). Everything else -- HTTP calls, pagination,
batch lookup, field parsing, canonicalization -- lives here once
rather than duplicated per pool type.

Confirmed empirically (2026-08-03), same findings as the AMM v4
session, now confirmed to hold for CLMM too:
- No User-Agent issue (unlike Orca).
- `/pools/info/ids?ids=<comma-separated>` batches up to ~100 ids/call
  and fails the *entire* call (`success: false`) if any id in the
  batch is malformed -- a well-formed but nonexistent id instead
  succeeds with `null` in that position of `data`.
- `/pools/info/list` is page-number pagination via `hasNextPage`, not
  cursor-based.
- `price` is mintA's price in mintB units, same direction as Orca and
  AMM v4 -- reuses normalize.canonicalize() unchanged.
- `feeRate` is a fraction (e.g. 0.0005 == 5 bps for TRX/USDC's CLMM
  pool), not Orca's out-of-1e6 integer.
- Missing symbol shows up here too (34/4216 CLMM pools sampled, same
  ~0.8% order of magnitude as AMM v4's 23/15000 and Orca's ~0.8%) --
  same `symbol or ""` handling applies unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterator, Optional, Sequence

import requests

from discovery.http_client import DEFAULT_TIMEOUT_S, build_session
from discovery.src.logger import Level, log
from discovery.src.normalize import TokenSide, canonicalize
from discovery.src.providers.base import PoolMetadata, PoolState, PoolVolume

API_BASE = "https://api-v3.raydium.io"
DEFAULT_PAGE_SIZE = 1000
REQUEST_TIMEOUT_S = DEFAULT_TIMEOUT_S

# api-v3 doesn't publish an official cap for /pools/info/ids; kept
# conservative and aligned with the RPC getMultipleAccounts convention
# CLAUDE.md already documents (batch up to 100 addresses per call).
IDS_BATCH_LIMIT = 100

# raw["feeRate"] is a fraction (0.0025 == 0.25%, 0.0005 == 0.05%);
# fee_bps is out of 10_000. Verified against RAY/USDC (feeRate=0.0025
# -> 25 bps) and TRX/USDC's CLMM pool (feeRate=0.0005 -> 5 bps,
# matching this pool's documented 0.05% fee).
FEE_FRACTION_TO_BPS_MULTIPLIER = 10_000


@dataclass(frozen=True)
class RaydiumPoolRow:
    """Everything needed for one dex_pool_snapshots row, minus the
    fields (snapshot_ts, tier, hot_reason) that belong to the calling
    scheduler cycle rather than to the pool itself. Shared verbatim
    between AMM v4 and CLMM -- the two pool types produce identically
    shaped rows, just with a different `venue` and a different
    `active_liquidity` derivation upstream (see
    RaydiumApiV3RestProviderBase._compute_active_liquidity).
    """

    venue: str
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
            venue=self.venue,
            pool_address=self.pool_address,
            base_mint=self.base_mint,
            base_symbol=self.base_symbol,
            quote_mint=self.quote_mint,
            quote_symbol=self.quote_symbol,
            fee_bps=self.fee_bps,
        )

    def to_state(self) -> PoolState:
        return PoolState(
            venue=self.venue,
            pool_address=self.pool_address,
            price_native=self.price_native,
            active_liquidity=self.active_liquidity,
        )

    def to_volume(self) -> PoolVolume:
        return PoolVolume(
            venue=self.venue,
            pool_address=self.pool_address,
            tvl_usd=self.tvl_usd,
            volume_24h_usd=self.volume_24h_usd,
        )

    def to_snapshot_row(self, snapshot_ts, tier: str, hot_reason: str) -> dict:
        return {
            "snapshot_ts": snapshot_ts,
            "venue": self.venue,
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


def parse_raw_pool(
    raw: dict, venue: str, compute_active_liquidity: Callable[[float], float]
) -> RaydiumPoolRow:
    """Parses one api-v3 pool record (Standard or Concentrated -- the
    two share this exact field layout) into a RaydiumPoolRow.
    `compute_active_liquidity` is the one pool-type-specific hook: AMM
    v4 and CLMM both currently return tvl_usd here, but for different
    reasons (see each provider's own callback), so that decision is
    deliberately left to the caller rather than hardcoded here.
    """
    pool_address = raw["id"]
    mint_a, mint_b = raw["mintA"], raw["mintB"]
    # Section 3.7's "widen, don't filter" -- untagged long-tail tokens
    # stay in, coerced to "" rather than dropped or left as None (which
    # would fail the non-nullable LowCardinality(String) insert).
    token_a = TokenSide(mint=mint_a["address"], symbol=mint_a["symbol"] or "")
    token_b = TokenSide(mint=mint_b["address"], symbol=mint_b["symbol"] or "")

    price_a_in_b = float(raw["price"])
    if price_a_in_b <= 0:
        raise ValueError(f"non-positive price: {price_a_in_b}")

    base, quote, price_native = canonicalize(token_a, token_b, price_a_in_b)

    fee_bps = round(raw["feeRate"] * FEE_FRACTION_TO_BPS_MULTIPLIER)
    tvl_usd = float(raw["tvl"])
    volume_24h_usd = float((raw.get("day") or {}).get("volume") or 0.0)

    # Only populate price_usd when the canonicalized quote side is
    # itself a stablecoin (quote ~= $1, so price_native already *is*
    # USD) -- otherwise leave it NULL rather than fabricate a
    # conversion.
    price_usd = price_native if quote.symbol in {"USDC", "USDT"} else None

    return RaydiumPoolRow(
        venue=venue,
        pool_address=pool_address,
        base_mint=base.mint,
        base_symbol=base.symbol,
        quote_mint=quote.mint,
        quote_symbol=quote.symbol,
        fee_bps=fee_bps,
        tvl_usd=tvl_usd,
        active_liquidity=compute_active_liquidity(tvl_usd),
        volume_24h_usd=volume_24h_usd,
        price_native=price_native,
        price_usd=price_usd,
    )


class RaydiumApiV3RestProviderBase:
    """Base for Raydium api-v3 REST providers (AMM v4, CLMM). Handles
    everything pool-type-agnostic: session setup, batch lookup,
    paginated listing, and the Protocol-conforming
    fetch_metadata/fetch_state/fetch_volume views. Subclasses set
    `VENUE` and `POOL_TYPE` and implement `_compute_active_liquidity`
    -- the one piece of behavior that genuinely differs between pool
    types.
    """

    VENUE: str
    POOL_TYPE: str

    def __init__(self, session: Optional[requests.Session] = None):
        self._session = session or build_session()

    def _compute_active_liquidity(self, tvl_usd: float) -> float:
        raise NotImplementedError

    def _parse(self, raw: dict) -> RaydiumPoolRow:
        return parse_raw_pool(raw, self.VENUE, self._compute_active_liquidity)

    def get_pools(self, pool_addresses: Sequence[str]) -> list[RaydiumPoolRow]:
        """Batch lookup via /pools/info/ids. A malformed id fails the
        whole chunk it's in (see module docstring) -- logged as a
        chunk-level WARN, distinct from the per-pool parse_skip WARN
        used for individual rows that don't parse.
        """
        rows: list[RaydiumPoolRow] = []
        addresses = list(pool_addresses)
        for i in range(0, len(addresses), IDS_BATCH_LIMIT):
            chunk = addresses[i : i + IDS_BATCH_LIMIT]
            resp = self._session.get(
                f"{API_BASE}/pools/info/ids",
                params={"ids": ",".join(chunk)},
                timeout=REQUEST_TIMEOUT_S,
            )
            resp.raise_for_status()
            body = resp.json()
            if not body.get("success"):
                log(
                    Level.WARN,
                    [
                        ("event", "batch_skip"),
                        ("venue", self.VENUE),
                        ("chunk_size", str(len(chunk))),
                        ("err", str(body.get("msg", "unknown error"))),
                    ],
                )
                continue
            for raw in body.get("data") or []:
                if raw is None:
                    continue  # well-formed id, pool not found
                try:
                    rows.append(self._parse(raw))
                except (KeyError, TypeError, ValueError, ZeroDivisionError) as exc:
                    log(
                        Level.WARN,
                        [
                            ("event", "parse_skip"),
                            ("venue", self.VENUE),
                            ("pool", str(raw.get("id", "?"))),
                            ("err", str(exc)),
                        ],
                    )
        return rows

    def get_pool(self, pool_address: str) -> RaydiumPoolRow:
        rows = self.get_pools([pool_address])
        if not rows:
            raise ValueError(f"pool not found or failed to parse: {pool_address}")
        return rows[0]

    def list_pools(
        self, page_size: int = DEFAULT_PAGE_SIZE, max_pages: Optional[int] = None
    ) -> Iterator[RaydiumPoolRow]:
        """Walks the full paginated pool list for this provider's
        POOL_TYPE (page-number pagination, terminated by
        `hasNextPage`). Skips (with a WARN log) any pool whose payload
        doesn't parse cleanly.
        """
        page = 1
        pages_fetched = 0
        while True:
            params = {
                "poolType": self.POOL_TYPE,
                "poolSortField": "default",
                "sortType": "desc",
                "pageSize": page_size,
                "page": page,
            }
            resp = self._session.get(
                f"{API_BASE}/pools/info/list", params=params, timeout=REQUEST_TIMEOUT_S
            )
            resp.raise_for_status()
            body = resp.json()["data"]
            for raw in body.get("data", []):
                try:
                    yield self._parse(raw)
                except (KeyError, TypeError, ValueError, ZeroDivisionError) as exc:
                    log(
                        Level.WARN,
                        [
                            ("event", "parse_skip"),
                            ("venue", self.VENUE),
                            ("pool", str(raw.get("id", "?"))),
                            ("err", str(exc)),
                        ],
                    )
            pages_fetched += 1
            page += 1
            if not body.get("hasNextPage") or (max_pages is not None and pages_fetched >= max_pages):
                break

    # --- Protocol-conforming views -----------------------------------

    def fetch_metadata(self) -> Sequence[PoolMetadata]:
        return [row.to_metadata() for row in self.list_pools()]

    def fetch_state(self, pool_addresses: Sequence[str]) -> Sequence[PoolState]:
        return [row.to_state() for row in self.get_pools(pool_addresses)]

    def fetch_volume(self) -> Sequence[PoolVolume]:
        return [row.to_volume() for row in self.list_pools()]
