"""RaydiumClmmRestProvider: Raydium's Concentrated Liquidity (CLMM)
pools. All HTTP/pagination/parsing logic is shared with
RaydiumAmmV4RestProvider via raydium_common.py -- see that module's
docstring for details confirmed empirically against the live API. This
file only supplies what's specific to CLMM: the venue tag, the
poolType filter, and active_liquidity's (current lack of a) meaning.

REST-only in this session, deliberately: CLMM pools' api-v3 `price`
field is already a computed, human-readable price (same as AMM v4 and
Orca), so no raw sqrt_price_x64 account decoding is needed to satisfy
PoolMetadataProvider/PoolVolumeProvider, or even a first-pass
PoolStateProvider.price_native. What raw account decoding *would* buy
is true active_liquidity (see _compute_active_liquidity below) and a
Tier 2-appropriate polling cadence independent of api-v3's own refresh
rate -- that's the Tier 2 RPC PoolStateProvider's job (section 3.3),
explicitly deferred to Phase 1h, not built here.
"""
from __future__ import annotations

from discovery.src.providers.raydium_common import RaydiumApiV3RestProviderBase


class RaydiumClmmRestProvider(RaydiumApiV3RestProviderBase):
    """Implements PoolMetadataProvider, PoolStateProvider, and
    PoolVolumeProvider against Raydium's api-v3 Concentrated (CLMM)
    REST API.
    """

    VENUE = "raydium_clmm"
    POOL_TYPE = "concentrated"

    def _compute_active_liquidity(self, tvl_usd: float) -> float:
        # TODO(Phase 1h): replace with true active_liquidity
        # (likvidnost in the currently active tick range), decoded
        # from raw on-chain `liquidity` against the pool's
        # sqrt_price_x64 -- that's the Tier 2 RPC PoolStateProvider's
        # job (see section 3.3), not built yet. Unlike AMM v4, CLMM
        # pools genuinely do have a concentrated tick range where
        # liquidity can sit far from tvl_usd (thin range near the
        # current price vs. deep TVL parked elsewhere, or vice versa)
        # -- so this tvl_usd placeholder is a real, temporary gap here,
        # not a correct-by-design value the way it is for AMM v4's
        # RaydiumAmmV4RestProvider._compute_active_liquidity.
        return tvl_usd
