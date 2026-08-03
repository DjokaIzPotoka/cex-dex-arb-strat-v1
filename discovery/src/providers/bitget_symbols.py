"""BitgetSymbolProvider: fetches GET /api/v2/spot/public/symbols and
adapts it to the generic SymbolProvider protocol (see cex_base.py).
Fourth of five CEX symbol providers.

Confirmed empirically (2026-08-04) against the live API:
- `baseCoin`/`quoteCoin` are separate fields on every returned symbol
  object, same shape as Binance/Bybit -- this provider never splits
  the `symbol` string itself.
- No pagination: an unfiltered call returned the full spot list (1208
  entries) in one response, no cursor/page field on this endpoint.
- `status` values seen live: `online` (1207) and `halt` (1) -- unlike
  Bybit, this endpoint actually surfaced a non-trading symbol in this
  snapshot, confirming section 3.7's "widen, don't filter" stance
  matters concretely here, not just in theory. Both are written as-is.
- Response headers carry `x-mbx-used-remain-limit` (confirmed live,
  value 19 out of the 20 req/s budget from section 5.2 immediately
  after one call) -- despite the `mbx` prefix normally associated with
  Binance, this is genuinely Bitget's own header name on this
  endpoint, not a copy-paste artifact of this codebase; verified by
  isolating a single call and grepping the raw header dump. No
  separate `X-...-Limit` (max) header accompanies it, only the
  remaining count, so only that value is logged.
- No documented per-call weight multiplier for this endpoint -- one
  request costs one token against the 20 req/s bucket.
"""
from __future__ import annotations

from typing import Optional, Sequence

import requests

from discovery.http_client import DEFAULT_TIMEOUT_S, build_session
from discovery.rate_limiter import TokenBucket
from discovery.src.logger import Level, log
from discovery.src.providers.cex_base import CexSymbol

EXCHANGE = "bitget"
API_URL = "https://api.bitget.com/api/v2/spot/public/symbols"
REQUEST_TIMEOUT_S = DEFAULT_TIMEOUT_S

# Published limit per docs/mev-discovery-plan.md section 5.2: 20
# req/s, unified market data. Sized to 90% as a safety margin, per
# section 5.4 point 1.
PUBLISHED_LIMIT_PER_WINDOW = 20
SAFETY_MARGIN = 0.90
BUCKET_CAPACITY = PUBLISHED_LIMIT_PER_WINDOW * SAFETY_MARGIN
BUCKET_REFILL_PERIOD_S = 1.0

# No per-endpoint weight multiplier documented for this endpoint (see
# module docstring) -- one request costs one token.
SYMBOLS_COST = 1


class BitgetSymbolProvider:
    """Implements SymbolProvider against Bitget's v2 spot public
    symbols REST endpoint.
    """

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        bucket: Optional[TokenBucket] = None,
    ):
        self._session = session or build_session()
        self._bucket = bucket or TokenBucket(BUCKET_CAPACITY, BUCKET_REFILL_PERIOD_S)

    def fetch_symbols(self) -> Sequence[CexSymbol]:
        self._bucket.consume(SYMBOLS_COST)
        resp = self._session.get(API_URL, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()

        log(
            Level.INFO,
            [
                ("event", "symbols_fetched"),
                ("exchange", EXCHANGE),
                ("used_remain_limit", str(resp.headers.get("x-mbx-used-remain-limit", "n/a"))),
            ],
        )

        body = resp.json()
        if body.get("code") != "00000":
            log(
                Level.ERROR,
                [
                    ("event", "api_error"),
                    ("exchange", EXCHANGE),
                    ("code", str(body.get("code"))),
                    ("msg", str(body.get("msg"))),
                ],
            )
            return []

        rows: list[CexSymbol] = []
        for raw in body.get("data", []):
            try:
                rows.append(self._parse_symbol(raw))
            except (KeyError, TypeError) as exc:
                log(
                    Level.WARN,
                    [
                        ("event", "parse_skip"),
                        ("exchange", EXCHANGE),
                        ("symbol", str(raw.get("symbol", "?"))),
                        ("err", str(exc)),
                    ],
                )
        return rows

    @staticmethod
    def _parse_symbol(raw: dict) -> CexSymbol:
        return CexSymbol(
            exchange=EXCHANGE,
            symbol=raw["symbol"],
            base_asset=raw["baseCoin"],
            quote_asset=raw["quoteCoin"],
            status=raw["status"],
        )
