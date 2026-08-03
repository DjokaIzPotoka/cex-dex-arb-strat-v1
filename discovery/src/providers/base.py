"""Provider abstraction, per docs/mev-discovery-plan.md section 3.3.

The abstraction is split by *what kind of data a source can deliver*,
not by venue -- no single source (REST, RPC, or Geyser) covers all
three equally well:

    PoolMetadataProvider   -- slow-changing: mints, fee tier, existence
    PoolStateProvider      -- fast-changing: price, active_liquidity,
                               read straight off raw on-chain state
    PoolVolumeProvider     -- aggregated: volume_24h_usd, TVL, computed
                               by an off-chain indexer from swap history

This split matters starting in Tier 2 (Phase 1b+): a future RPC
provider (`getMultipleAccounts`) can implement PoolStateProvider alone
-- it can decode price/active_liquidity directly from account bytes,
but not volume_24h_usd, which raw account state simply doesn't carry.
A REST provider (this phase's OrcaRestProvider) implements all three,
because a venue's own aggregation endpoint returns them together in a
single call. Deliberately not generalized further than this
three-way split (YAGNI, per the doc's own note in 3.3).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence


@dataclass(frozen=True)
class PoolMetadata:
    venue: str
    pool_address: str
    base_mint: str
    base_symbol: str
    quote_mint: str
    quote_symbol: str
    fee_bps: int


@dataclass(frozen=True)
class PoolState:
    venue: str
    pool_address: str
    price_native: float
    active_liquidity: float


@dataclass(frozen=True)
class PoolVolume:
    venue: str
    pool_address: str
    tvl_usd: float
    volume_24h_usd: float


class PoolMetadataProvider(Protocol):
    def fetch_metadata(self) -> Sequence[PoolMetadata]: ...


class PoolStateProvider(Protocol):
    def fetch_state(self, pool_addresses: Sequence[str]) -> Sequence[PoolState]: ...


class PoolVolumeProvider(Protocol):
    def fetch_volume(self) -> Sequence[PoolVolume]: ...
