"""OkxSymbolProvider: fetches GET /api/v5/public/instruments
(instType=SPOT) and adapts it to the generic SymbolProvider protocol
(see cex_base.py). Third of five CEX symbol providers.

Confirmed empirically (2026-08-04) against the live API:
- `baseCcy`/`quoteCcy` are separate fields on every returned
  instrument object -- this provider never splits the `instId` string
  (e.g. "BTC-USDT", "USDC-TEV") itself, even though OKX's dash
  delimiter looks safe to split on at a glance. Verified against the
  first 50 live entries: `baseCcy + "-" + quoteCcy == instId` held for
  all of them, but the explicit fields are used regardless, per the
  same reasoning as Binance's baseAsset/quoteAsset -- trusting a
  provided field beats re-deriving it from a string every time the
  option exists.
- The status field is named `state` here, not `status` (e.g. "live").
  A single unfiltered call (no `uly`/`instFamily` filter) returned
  1335 live spot instruments, `state` was `"live"` for all of them in
  this snapshot -- OKX evidently doesn't surface halted/suspended spot
  instruments through this same "live" bucket the way Binance's BREAK
  does. Per section 3.7 this provider still writes whatever `state`
  the API returns rather than filtering, so a future state value would
  pass through unchanged.
- No pagination: `instType=SPOT` with no additional filter returns the
  complete spot instrument list in a single response (no cursor/page
  field exists on this endpoint per OKX's docs, and the full count
  came back in one call).
- Response headers carried no `x-ratelimit-limit`/`x-ratelimit-remaining`
  pair on this endpoint (confirmed by isolating a single call and
  grepping the full header dump case-insensitively for "ratelimit" --
  nothing matched). Those headers are documented as scoped to
  authenticated trading endpoints, not public market data; logged as
  "n/a" here so the absence is visible rather than silently omitted.
- OKX's public instruments endpoint has no documented per-call weight
  multiplier (unlike Binance) -- one request costs one token against
  the 20 req/2s per-endpoint limit from section 5.2. The separate 250
  req/s *global* ceiling mentioned in the design doc is not
  independently enforced here: it's far above what a single endpoint's
  20 req/2s (=10 req/s) bucket could ever produce on its own, so a
  second bucket would never bind in practice for this provider.
"""
from __future__ import annotations

from typing import Optional, Sequence

import requests

from discovery.http_client import DEFAULT_TIMEOUT_S, build_session
from discovery.rate_limiter import TokenBucket
from discovery.src.logger import Level, log
from discovery.src.providers.cex_base import CexSymbol

EXCHANGE = "okx"
API_URL = "https://www.okx.com/api/v5/public/instruments"
REQUEST_TIMEOUT_S = DEFAULT_TIMEOUT_S

# Published limit per docs/mev-discovery-plan.md section 5.2: ~20
# req/2s per endpoint type. Sized to 90% as a safety margin, per
# section 5.4 point 1. (The doc's separate 250 req/s global ceiling
# doesn't bind for this single-endpoint provider -- see module
# docstring.)
PUBLISHED_LIMIT_PER_WINDOW = 20
SAFETY_MARGIN = 0.90
BUCKET_CAPACITY = PUBLISHED_LIMIT_PER_WINDOW * SAFETY_MARGIN
BUCKET_REFILL_PERIOD_S = 2.0

# No per-endpoint weight multiplier documented for OKX's public
# instruments endpoint (see module docstring) -- one request costs one
# token.
INSTRUMENTS_COST = 1


class OkxSymbolProvider:
    """Implements SymbolProvider against OKX's public instruments REST
    endpoint, SPOT instType.
    """

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        bucket: Optional[TokenBucket] = None,
    ):
        self._session = session or build_session()
        self._bucket = bucket or TokenBucket(BUCKET_CAPACITY, BUCKET_REFILL_PERIOD_S)

    def fetch_symbols(self) -> Sequence[CexSymbol]:
        self._bucket.consume(INSTRUMENTS_COST)
        resp = self._session.get(API_URL, params={"instType": "SPOT"}, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()

        log(
            Level.INFO,
            [
                ("event", "instruments_fetched"),
                ("exchange", EXCHANGE),
                ("ratelimit_limit", str(resp.headers.get("x-ratelimit-limit", "n/a"))),
                ("ratelimit_remaining", str(resp.headers.get("x-ratelimit-remaining", "n/a"))),
            ],
        )

        body = resp.json()
        if body.get("code") != "0":
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
                        ("symbol", str(raw.get("instId", "?"))),
                        ("err", str(exc)),
                    ],
                )
        return rows

    @staticmethod
    def _parse_symbol(raw: dict) -> CexSymbol:
        return CexSymbol(
            exchange=EXCHANGE,
            symbol=raw["instId"],
            base_asset=raw["baseCcy"],
            quote_asset=raw["quoteCcy"],
            status=raw["state"],
        )
