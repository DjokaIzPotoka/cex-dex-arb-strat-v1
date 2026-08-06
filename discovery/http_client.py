"""Shared HTTP client for discovery REST providers.

Extracted out of OrcaRestProvider so the next REST adapters (Raydium,
Meteora) reuse this instead of rediscovering the same issue Orca's API
surfaced during Phase 1a: Cloudflare-fronted venue APIs 403 requests
that don't carry a browser-like User-Agent, even for public,
unauthenticated GET endpoints (confirmed empirically -- bare urllib's
default UA gets blocked, curl and a requests.Session with an explicit
UA both work). Every provider hitting one of these APIs needs the same
fix, so it belongs here, not duplicated per-provider.

Also centralizes basic retry/backoff on transient failures (connection
errors, 429, 5xx) via urllib3's Retry, since that's the same
boilerplate every REST provider would otherwise have to hand-roll.
"""
from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_USER_AGENT = "zora-discovery/0.1 (github.com/DjokaIzPotoka/cex-dex-arb-strat-v1)"
DEFAULT_TIMEOUT_S = 15

DEFAULT_RETRY_TOTAL = 3
DEFAULT_BACKOFF_FACTOR = 0.5  # urllib3 sleeps backoff_factor * (2 ** (retry_num - 1)): 0.5s, 1s, 2s
RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)


def build_session(
    user_agent: str = DEFAULT_USER_AGENT,
    retry_total: int = DEFAULT_RETRY_TOTAL,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    allowed_methods: tuple[str, ...] = ("GET",),
) -> requests.Session:
    """A requests.Session preconfigured with a User-Agent header and
    retry/backoff. Build one per provider instance and reuse it across
    calls (connection pooling) rather than calling this per-request.

    `allowed_methods` defaults to GET-only, matching every existing
    REST provider's usage (idempotent reads only, so blanket retry is
    safe). solana_rpc_state.py is the first caller that needs POST
    included: Solana's JSON-RPC interface is POST-only, and
    getMultipleAccounts specifically is a read (no state mutation), so
    retrying it on a transient failure is exactly as safe as retrying a
    GET -- this parameter exists so that caller can opt in without
    loosening the default for every other provider's session.
    """
    session = requests.Session()
    session.headers["User-Agent"] = user_agent

    retry = Retry(
        total=retry_total,
        backoff_factor=backoff_factor,
        status_forcelist=RETRYABLE_STATUS_CODES,
        allowed_methods=allowed_methods,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session
