"""BybitSymbolProvider: fetches GET /v5/market/instruments-info
(category=spot) and adapts it to the generic SymbolProvider protocol
(see cex_base.py). Second of five CEX symbol providers (Binance was
first) -- see cex_base.py's docstring for what's shared vs. what's
expected to differ per exchange (this file's URL, rate-limit
accounting, and response parsing).

Confirmed empirically (2026-08-04) against the live API:
- `baseCoin`/`quoteCoin` are separate fields on every returned
  instrument object, same shape as Binance's baseAsset/quoteAsset --
  this provider never splits the `symbol` string itself.
- `category=spot` alone returns the full spot list in one page: live
  response carried 566 entries with `result.nextPageCursor` empty
  ("") -- no pagination loop needed for the spot category today. If a
  future response ever carries a non-empty cursor, that's a sign the
  spot list grew past Bybit's single-page cap and this provider needs
  a follow-up-page loop; not needed as of this snapshot.
- Every one of those 566 live entries had `status: "Trading"` -- no
  halted/delisted spot symbols appeared in this endpoint's response at
  all (unlike Binance, which does return BREAK). Per section 3.7's
  "widen, don't filter" stance this provider still doesn't filter by
  status client-side -- it writes whatever the endpoint returns,
  as-is -- but there was nothing to filter out here empirically.
- Response headers carried no `X-Bapi-Limit-Status`/`X-Bapi-Limit`
  pair on this endpoint (confirmed by isolating a single unauthenticated
  call and grepping the full header dump case-insensitively for
  "bapi"/"limit"/"ratelimit" -- nothing matched). Those headers appear
  to be scoped to authenticated/private endpoints, not public market
  data. Logged as "n/a" rather than a bogus placeholder value so this
  absence is visible in the log rather than silently omitted.
- No per-call weight/cost accounting exists for Bybit's public market
  endpoints the way Binance has request-weight -- the docs describe a
  flat requests-per-window cap (600 req/5s) with no per-endpoint
  weight multiplier. So unlike Binance's EXCHANGE_INFO_WEIGHT constant,
  this provider consumes exactly 1 token per call.
"""
from __future__ import annotations

from typing import Optional, Sequence

import requests

from discovery.http_client import DEFAULT_TIMEOUT_S, build_session
from discovery.rate_limiter import TokenBucket
from discovery.src.logger import Level, log
from discovery.src.providers.cex_base import CexSymbol

EXCHANGE = "bybit"
API_URL = "https://api.bybit.com/v5/market/instruments-info"
REQUEST_TIMEOUT_S = DEFAULT_TIMEOUT_S

# Published limit per docs/mev-discovery-plan.md section 5.2: 600
# req/5s, default bucket, per IP. Sized to 90% as a safety margin, per
# section 5.4 point 1.
PUBLISHED_LIMIT_PER_WINDOW = 600
SAFETY_MARGIN = 0.90
BUCKET_CAPACITY = PUBLISHED_LIMIT_PER_WINDOW * SAFETY_MARGIN
BUCKET_REFILL_PERIOD_S = 5.0

# No per-endpoint weight multiplier on Bybit's public market-data
# surface (see module docstring) -- one request costs one token.
INSTRUMENTS_INFO_COST = 1


class BybitSymbolProvider:
    """Implements SymbolProvider against Bybit's v5 instruments-info
    REST endpoint, spot category.
    """

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        bucket: Optional[TokenBucket] = None,
    ):
        self._session = session or build_session()
        self._bucket = bucket or TokenBucket(BUCKET_CAPACITY, BUCKET_REFILL_PERIOD_S)

    def fetch_symbols(self) -> Sequence[CexSymbol]:
        rows: list[CexSymbol] = []
        cursor = ""
        while True:
            self._bucket.consume(INSTRUMENTS_INFO_COST)
            params = {"category": "spot"}
            if cursor:
                params["cursor"] = cursor
            resp = self._session.get(API_URL, params=params, timeout=REQUEST_TIMEOUT_S)
            resp.raise_for_status()

            log(
                Level.INFO,
                [
                    ("event", "instruments_info_fetched"),
                    ("exchange", EXCHANGE),
                    ("bapi_limit_status", str(resp.headers.get("X-Bapi-Limit-Status", "n/a"))),
                    ("bapi_limit", str(resp.headers.get("X-Bapi-Limit", "n/a"))),
                ],
            )

            body = resp.json()
            if body.get("retCode") != 0:
                log(
                    Level.ERROR,
                    [
                        ("event", "api_error"),
                        ("exchange", EXCHANGE),
                        ("ret_code", str(body.get("retCode"))),
                        ("ret_msg", str(body.get("retMsg"))),
                    ],
                )
                break

            result = body.get("result", {})
            for raw in result.get("list", []):
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

            cursor = result.get("nextPageCursor", "")
            if not cursor:
                break
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
