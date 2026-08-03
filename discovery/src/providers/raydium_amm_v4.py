"""RaydiumAmmV4RestProvider: Raydium's Standard-AMM (constant-product,
"AMM v4") pools. All HTTP/pagination/parsing logic is shared with
RaydiumClmmRestProvider via raydium_common.py -- see that module's
docstring for the details confirmed empirically against the live API
(batch-by-ids semantics, pagination shape, fee/price field
conventions, the null-symbol pattern). This file only supplies what's
actually specific to AMM v4: the venue tag, the poolType filter, and
active_liquidity's meaning for a constant-product pool.
"""
from __future__ import annotations

from discovery.src.providers.raydium_common import RaydiumApiV3RestProviderBase


class RaydiumAmmV4RestProvider(RaydiumApiV3RestProviderBase):
    """Implements PoolMetadataProvider, PoolStateProvider, and
    PoolVolumeProvider against Raydium's api-v3 Standard-AMM REST API.
    """

    VENUE = "raydium_amm_v4"
    POOL_TYPE = "standard"

    def _compute_active_liquidity(self, tvl_usd: float) -> float:
        # Not a TODO(Phase 1h) placeholder, unlike CLMM: a
        # constant-product AMM v4 pool has no concentrated tick range
        # to begin with -- its entire reserve curve is "active" by
        # construction, so tvl_usd genuinely *is* active_liquidity
        # here, exactly as section 3.5's schema note specifies
        # ("active_liquidity ... = tvl_usd za AMM v4 poolove"). There's
        # nothing here for the future RPC PoolStateProvider to replace.
        return tvl_usd
