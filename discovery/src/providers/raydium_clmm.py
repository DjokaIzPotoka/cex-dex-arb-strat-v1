"""RaydiumClmmRestProvider: Raydium's Concentrated Liquidity (CLMM)
pools. All HTTP/pagination/parsing logic is shared with
RaydiumAmmV4RestProvider via raydium_common.py -- see that module's
docstring for details confirmed empirically against the live API. This
file only supplies what's specific to CLMM: the venue tag, the
poolType filter, and active_liquidity's (still-limited) meaning here.

REST-only, deliberately: CLMM pools' api-v3 `price` field is already a
computed, human-readable price (same as AMM v4 and Orca), so no raw
sqrt_price_x64 account decoding is needed here to satisfy
PoolMetadataProvider/PoolVolumeProvider, or even Tier 1's own baseline
PoolStateProvider.price_native. What raw account decoding buys is true
active_liquidity and a Tier 2-appropriate polling cadence independent
of api-v3's own refresh rate -- that's solana_rpc_state.py's
RaydiumClmmRpcStateProvider's job (section 3.3, Phase 1h, this
session), used by tier2_poller.py for Tier 2 hot pools specifically,
not by this REST provider or Tier 1's baseline pull.
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
        # tvl_usd placeholder, still real for THIS (REST/Tier 1) row:
        # Phase 1h (this session) implemented true active_liquidity
        # (likvidnost in the currently active tick range), decoded
        # from raw on-chain `liquidity` against the pool's
        # sqrt_price_x64 -- but only in
        # solana_rpc_state.RaydiumClmmRpcStateProvider, which
        # overwrites this placeholder for Tier 2 hot pools (see
        # tier2_poller.py's _poll_venue_via_rpc). This REST provider's
        # own baseline Tier 1 pull still returns tvl_usd here
        # unchanged: paying for a raw account decode on every pool in
        # the whole universe every 5-minute cycle, just for a field
        # Tier 1 doesn't otherwise act on, isn't worth it -- Tier 2's
        # narrower hot-pool set is exactly where that cost is
        # justified. Unlike AMM v4, CLMM pools genuinely do have a
        # concentrated tick range where liquidity can sit far from
        # tvl_usd (thin range near the current price vs. deep TVL
        # parked elsewhere, or vice versa) -- so this placeholder
        # remains a real, known gap for any CLMM pool that never gets
        # promoted to Tier 2, not a correct-by-design value the way it
        # is for AMM v4's RaydiumAmmV4RestProvider._compute_active_liquidity.
        return tvl_usd
