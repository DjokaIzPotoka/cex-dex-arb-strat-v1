"""CEX symbol-provider abstraction, per docs/mev-discovery-plan.md
section 3.3's closing note: "Isti princip primenjuje se simetricno na
CEX stranu (SymbolProvider, buduci CexPriceProvider za kasniju spread
fazu) -- REST exchangeInfo poziv se ne hardkoduje direktno u
scheduler, vec ide kroz isti tip apstrakcije."

Just one Protocol for now. `CexPriceProvider` is explicitly a
later-phase concern per that same note -- a persistent CEX price log
is one of Phase 2's open questions (see the doc's section 3.5 closing
paragraph on price_native normalization), not something this session
builds.

Mirrors the DEX side's providers/base.py in spirit (a plain
dataclass + a Protocol, no shared base class forcing venue-specific
adapters into a common inheritance chain), but the CEX side doesn't
need base.py's three-way Metadata/State/Volume split: exchangeInfo-
shaped endpoints return everything cex_symbol_snapshots needs (symbol,
base/quote asset, status) in one call, with no analogue to DEX's
"fast-changing state vs slow-changing metadata vs aggregated volume"
distinction -- a listed symbol's base/quote/status don't have
meaningfully different update cadences the way a pool's price and TVL
do. One Protocol is the right amount of abstraction here, not three;
forcing the DEX split onto CEX data that doesn't have it would just be
YAGNI in the other direction.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Sequence


@dataclass(frozen=True)
class CexSymbol:
    exchange: str
    symbol: str
    base_asset: str
    quote_asset: str
    status: str

    def to_snapshot_row(self, snapshot_ts) -> dict:
        return {
            "snapshot_ts": snapshot_ts,
            "exchange": self.exchange,
            "symbol": self.symbol,
            "base_asset": self.base_asset,
            "quote_asset": self.quote_asset,
            "status": self.status,
        }


class SymbolProvider(Protocol):
    def fetch_symbols(self) -> Sequence[CexSymbol]: ...
