"""Base/quote canonicalization, per docs/mev-discovery-plan.md section
3.5's price normalization note: a provider's raw token-A/token-B order
is not a reliable indicator of which side is "quote" -- the same
logical pair (e.g. TOKEN/SOL) can come in reversed order even within
one venue. Rule, in priority order: a known stablecoin (USDC/USDT) is
always quote; failing that, SOL is quote; otherwise (neither side is a
stablecoin or SOL) the provider's own order is kept as-is -- that
"exotic pair" case is explicitly left unresolved here and deferred to
Phase 2's full token taxonomy (see the doc's section 4).
"""
from __future__ import annotations

from dataclasses import dataclass

STABLE_SYMBOLS = {"USDC", "USDT"}
SOL_SYMBOLS = {"SOL"}


@dataclass(frozen=True)
class TokenSide:
    mint: str
    symbol: str


def _quote_rank(symbol: str) -> int:
    """Lower rank = stronger claim to being the quote side."""
    if symbol in STABLE_SYMBOLS:
        return 0
    if symbol in SOL_SYMBOLS:
        return 1
    return 2


def canonicalize(
    token_a: TokenSide, token_b: TokenSide, price_a_in_b: float
) -> tuple[TokenSide, TokenSide, float]:
    """Returns (base, quote, price_native), where price_native is the
    price of one unit of base in quote units. `price_a_in_b` is the
    provider's raw price of one unit of token_a in token_b units.
    """
    if _quote_rank(token_a.symbol) < _quote_rank(token_b.symbol):
        # token_a has the stronger quote-side claim -> swap, and invert
        # the price since it was expressed the other way around.
        return token_b, token_a, 1.0 / price_a_in_b
    return token_a, token_b, price_a_in_b
