"""BinanceSymbolProvider: fetches GET /api/v3/exchangeInfo and adapts
it to the generic SymbolProvider protocol (see cex_base.py). First of
five planned CEX symbol providers (Bybit/OKX/Bitget/Gate.io come in a
later session) -- see cex_base.py's docstring for what's shared
(SymbolProvider Protocol, CexSymbol) vs. what's expected to differ per
exchange (this file's URL, weight accounting, and response parsing).

Confirmed empirically (2026-08-04) against the live API:
- baseAsset/quoteAsset are separate fields on every returned symbol
  object -- Binance's API never requires splitting the `symbol` string
  ourselves, which matters because that split is genuinely ambiguous
  for some real listed pairs: TUSDUSDT (TUSD/USDT) or USDCUSDT
  (USDC/USDT) can't be reliably split by suffix-stripping without an
  exact, correctly-prioritized list of every quote asset -- and
  getting the priority order wrong silently produces a wrong split
  rather than an error. Verified against the full live symbol set
  (3670 symbols): every `symbol` string equals `baseAsset + quoteAsset`
  concatenated with no separator, so trusting the provided fields
  directly is both simpler and the only reliable approach; this
  provider never touches the `symbol` string except to store it as-is.
- No pre-filtering by status: `TRADING` and `BREAK` (halted) both
  appear in the live response and both are kept here, per section
  3.7's "widen, don't filter" stance -- a symbol transitioning to
  BREAK is itself a real observation for later analysis, not noise to
  discard at ingestion.
- An unfiltered call (all ~3670 symbols, no `symbols=` query param)
  costs 20 request-weight -- nowhere near the 6000/min limit on its
  own. (`X-MBX-USED-WEIGHT-1M` is a rolling-window *cumulative*
  counter across every call from this IP, not a per-call cost -- an
  early measurement here mistakenly read the delta between two calls
  in the same window as one call's weight and landed on 40; isolating
  a single call in an otherwise-quiet window confirmed 20. Worth
  remembering for the next four CEX providers: always isolate the
  call before trusting this header as a per-endpoint cost.) The rate
  limiter below won't meaningfully throttle this session's one-shot
  validation script; it's wired in now so every future repeated-
  polling use (and the next four CEX providers, which can reuse
  TokenBucket directly) inherits the same protection rather than each
  hand-rolling it later.
- Response headers carry both `X-MBX-USED-WEIGHT` (cumulative on this
  connection) and `X-MBX-USED-WEIGHT-1M` (the one that actually
  matters -- weight used in the current rolling 1-minute window, what
  the 6000/min limit is measured against). Both logged per section
  5.4 point 3 ("read and log rate-limit headers ... for visibility
  before hitting 429").
- Retry-After on a 429 doesn't need separate handling here:
  discovery/http_client.py's build_session() already configures
  urllib3's Retry with its default `respect_retry_after_header=True`
  (confirmed against the installed urllib3), so a 429 is retried with
  the server-specified backoff automatically, for every REST provider
  in this project, not just this one.
"""
from __future__ import annotations

from typing import Optional, Sequence

import requests

from discovery.http_client import DEFAULT_TIMEOUT_S, build_session
from discovery.rate_limiter import TokenBucket
from discovery.src.logger import Level, log
from discovery.src.providers.cex_base import CexSymbol

EXCHANGE = "binance"
API_URL = "https://api.binance.com/api/v3/exchangeInfo"
REQUEST_TIMEOUT_S = DEFAULT_TIMEOUT_S

# Binance's published REQUEST_WEIGHT limit (confirmed live: the
# response body's own `rateLimits` array reports the same number,
# matching CLAUDE.md section 5.2's table). Sized to 90% of that as a
# safety margin, per section 5.4 point 1 -- not the full published
# limit.
PUBLISHED_WEIGHT_LIMIT_PER_MIN = 6000
SAFETY_MARGIN = 0.90
BUCKET_CAPACITY = PUBLISHED_WEIGHT_LIMIT_PER_MIN * SAFETY_MARGIN
BUCKET_REFILL_PERIOD_S = 60.0

# Cost of one unfiltered GET /api/v3/exchangeInfo call, confirmed live
# by isolating a single call in an otherwise-quiet rolling window (see
# the module docstring's note on why the naive reading of this number
# is easy to get wrong). Binance's weight cost is a fixed,
# published-per-endpoint number, not something the response tells you
# to spend in advance -- so this has to be a constant we pre-consume
# from the bucket before the request, not read back after the fact.
EXCHANGE_INFO_WEIGHT = 20


class BinanceSymbolProvider:
    """Implements SymbolProvider against Binance's exchangeInfo REST
    endpoint.
    """

    def __init__(
        self,
        session: Optional[requests.Session] = None,
        bucket: Optional[TokenBucket] = None,
    ):
        self._session = session or build_session()
        self._bucket = bucket or TokenBucket(BUCKET_CAPACITY, BUCKET_REFILL_PERIOD_S)

    def fetch_symbols(self) -> Sequence[CexSymbol]:
        self._bucket.consume(EXCHANGE_INFO_WEIGHT)
        resp = self._session.get(API_URL, timeout=REQUEST_TIMEOUT_S)
        resp.raise_for_status()

        log(
            Level.INFO,
            [
                ("event", "exchange_info_fetched"),
                ("exchange", EXCHANGE),
                ("used_weight_1m", str(resp.headers.get("X-MBX-USED-WEIGHT-1M", "?"))),
                ("used_weight", str(resp.headers.get("X-MBX-USED-WEIGHT", "?"))),
            ],
        )

        body = resp.json()
        rows: list[CexSymbol] = []
        for raw in body.get("symbols", []):
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
            base_asset=raw["baseAsset"],
            quote_asset=raw["quoteAsset"],
            status=raw["status"],
        )
