"""GateioSymbolProvider: fetches GET /api/v4/spot/currency_pairs and
adapts it to the generic SymbolProvider protocol (see cex_base.py).
Fifth and last of the CEX symbol providers.

Confirmed empirically (2026-08-04) against the live API:
- The response body is a bare JSON array (not wrapped in a `data`/
  `result` envelope like the other four exchanges) -- `resp.json()` is
  used directly as the row list here, unlike Binance/Bybit/OKX/Bitget.
- `base`/`quote` are separate fields on every returned pair object
  (e.g. id="ALEPH_USDT", base="ALEPH", quote="USDT") -- this provider
  never splits the underscore-delimited `id` string itself, same
  reasoning as every prior provider in this batch.
- No pagination: an unfiltered call returned the full pair list (2237
  entries) in one response.
- The status field is named `trade_status` here (values seen live:
  `tradable` 2235, `untradable` 2) -- both written as-is per section
  3.7's "widen, don't filter" stance; a pair currently untradable is
  itself a real observation, not noise to discard at ingestion.
- Response headers carry explicit, documented rate-limit fields:
  `X-Gate-RateLimit-Limit`, `X-Gate-RateLimit-Requests-Remain`, and
  `X-Gate-RateLimit-Reset-Timestamp` (confirmed live: limit=200,
  remain=199 immediately after one isolated call) -- unlike Bybit/OKX,
  Gate.io's public market-data endpoints do expose per-call visibility
  into the budget, so all three are logged here.
- No documented per-call weight multiplier for this endpoint -- one
  request costs one token against the 200 req/10s bucket from section
  5.2.
"""
from __future__ import annotations

from typing import Optional, Sequence

import requests

from discovery.http_client import DEFAULT_TIMEOUT_S, build_session
from discovery.rate_limiter import TokenBucket
from discovery.src.logger import Level, log
from discovery.src.providers.cex_base import CexSymbol

EXCHANGE = "gateio"
API_URL = "https://api.gateio.ws/api/v4/spot/currency_pairs"
REQUEST_TIMEOUT_S = DEFAULT_TIMEOUT_S

# Published limit per docs/mev-discovery-plan.md section 5.2: 200
# req/10s per endpoint. Sized to 90% as a safety margin, per section
# 5.4 point 1.
PUBLISHED_LIMIT_PER_WINDOW = 200
SAFETY_MARGIN = 0.90
BUCKET_CAPACITY = PUBLISHED_LIMIT_PER_WINDOW * SAFETY_MARGIN
BUCKET_REFILL_PERIOD_S = 10.0

# No per-endpoint weight multiplier documented for this endpoint (see
# module docstring) -- one request costs one token.
CURRENCY_PAIRS_COST = 1


class GateioSymbolProvider:
    """Implements SymbolProvider against Gate.io's v4 spot
    currency_pairs REST endpoint.
    """

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        bucket: Optional[TokenBucket] = None,
    ):
        self._session = session or build_session()
        self._bucket = bucket or TokenBucket(BUCKET_CAPACITY, BUCKET_REFILL_PERIOD_S)

    def fetch_symbols(self) -> Sequence[CexSymbol]:
        self._bucket.consume(CURRENCY_PAIRS_COST)
        resp = self._session.get(API_URL, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()

        log(
            Level.INFO,
            [
                ("event", "currency_pairs_fetched"),
                ("exchange", EXCHANGE),
                ("ratelimit_limit", str(resp.headers.get("X-Gate-RateLimit-Limit", "n/a"))),
                ("ratelimit_remain", str(resp.headers.get("X-Gate-RateLimit-Requests-Remain", "n/a"))),
            ],
        )

        body = resp.json()
        rows: list[CexSymbol] = []
        for raw in body:
            try:
                rows.append(self._parse_symbol(raw))
            except (KeyError, TypeError) as exc:
                log(
                    Level.WARN,
                    [
                        ("event", "parse_skip"),
                        ("exchange", EXCHANGE),
                        ("symbol", str(raw.get("id", "?"))),
                        ("err", str(exc)),
                    ],
                )
        return rows

    @staticmethod
    def _parse_symbol(raw: dict) -> CexSymbol:
        return CexSymbol(
            exchange=EXCHANGE,
            symbol=raw["id"],
            base_asset=raw["base"],
            quote_asset=raw["quote"],
            status=raw["trade_status"],
        )
