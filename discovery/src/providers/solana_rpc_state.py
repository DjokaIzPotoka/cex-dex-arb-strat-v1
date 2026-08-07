"""Tier 2 RPC PoolStateProvider, per docs/mev-discovery-plan.md section
3.3: a `getMultipleAccounts`-based provider (batch up to 100 addresses
per call) that decodes `price_native`/`active_liquidity` directly from
raw on-chain account bytes for Orca Whirlpool and Raydium CLMM pools,
replacing the one-request-per-pool REST polling Phase 1g's scalability
finding flagged as Tier 2's bottleneck (see tier2_poller.py's module
docstring for that finding). Implements PoolStateProvider only, not
PoolMetadataProvider/PoolVolumeProvider -- `volume_24h_usd` is an
off-chain aggregation raw account state simply doesn't carry, so it
stays on the existing REST providers per section 3.3's hybrid design
(RPC on Tier 2's 1-min cadence for state, REST on Tier 1's baseline
5-min cadence for volume). Meteora DLMM's bin-based state is NOT
covered here -- separate, later concern per this session's scope.

Byte offsets and the sqrt_price_x64 -> price formula are ported 1:1
from the ZORA DEX ingestor's already-production-validated Rust code,
not re-derived:
  - ingestor-dex-solana/src/clmm.rs (Raydium CLMM `PoolState`)
  - ingestor-dex-solana/src/orca.rs (Orca `Whirlpool`)
  - ingestor-dex-solana/src/sqrt_price.rs (shared Q64.64 -> price math)
See those modules' own doc comments for how each offset/discriminator
was independently confirmed against a live account (not just taken from
upstream docs) -- that confirmation is not repeated here since it's the
exact same byte layout, just decoded in a different language. What
*is* independently repeated in this session is a live three-way
cross-check (this provider vs. sqrt_price.rs's actual live output, via
ingestor-dex-solana/src/bin/live_price_check.rs, vs. the existing REST
provider) for the same two validation pools (KMNO/USDC Orca, TRX/USDC
Raydium CLMM) -- see discovery/src/run_rpc_state_crosscheck.py, whose
output is referenced in this session's summary.

One deliberate difference from the Rust side: clmm.rs/orca.rs don't
read `token_mint_0`/`token_mint_1` (or `_a`/`_b`) at runtime, because
ZORA's DEX ingestor only ever polls a fixed, hand-configured 6-pair
roster where which mint is "base" was confirmed once, offline, per
pool (see each Rust module's doc comment). Tier 2's hot-pool set is the
opposite: any pool in the whole discovered universe can get promoted
dynamically, so there is no fixed roster to hardcode orientation
against. This provider therefore *does* decode `token_mint_0`/
`token_mint_1` (`_a`/`_b`) from the account bytes (same struct, offsets
already known -- see CLMM_OFFSET_TOKEN_MINT_0 etc. below) and determines
`base_is_mint_0` by comparing the decoded mint against a caller-supplied
`base_mint` (the most recent REST-derived `dex_pool_snapshots.base_mint`
for that pool -- Tier 1's own `normalize.canonicalize()` output,
reused rather than re-derived here) instead of a hardcoded per-pool
config table.

A second, related difference: Raydium CLMM's `PoolState` account
stores `mint_decimals_0`/`mint_decimals_1` directly (see clmm.rs's
offset table), so no extra RPC round-trip is needed there. Orca's
`Whirlpool` account does NOT cache its mints' decimals anywhere in the
struct (see orca.rs's offset table -- there's a gap between
`fee_growth_global_a` and `token_mint_b` and no decimals field at all),
so this provider fetches them once via a second batched
`getMultipleAccounts` call against the SPL Token Mint accounts
themselves (decimals live at a fixed byte offset in every SPL Token
Mint account -- see SPL_MINT_OFFSET_DECIMALS below), and caches the
result forever on `self` (a mint's decimals never change), so a given
hot pool's mints are only ever resolved once across this process's
whole lifetime, not once per Tier 2 poll.

getMultipleAccounts batch size: Phase 1h's own validation (the
three-way cross-check referenced above) only ever exercised
single-address batches, so it never actually exercised this method at
the size Tier 2 polling uses in production -- MAX_POOLS_PER_VENUE_PER_POLL
(tier2_poller.py) is 100, and this module chunked at
GET_MULTIPLE_ACCOUNTS_BATCH_LIMIT=100 (the Solana JSON-RPC protocol's
own documented cap for this method), meaning a full 100-pool venue
batch went out as one 100-address call. Live Tier 2 operation surfaced
HTTP 413 on exactly that call shape. Confirmed empirically against the
real configured QuickNode endpoint (not assumed from documentation,
same convention as every other rate-limit figure in this codebase):
stepping down from 100 (100, 75, 50, 25, 15, 10 all failed, every
time, with the identical body) surfaced QuickNode's own explicit
error -- `{"code":-32615,"message":"getMultipleAccounts is limited to
a 5 range, upgrade from discover plan..."}` -- and the boundary was
then pinned exactly: 5 addresses succeeds consistently (multiple
trials, real Orca/Raydium CLMM pool_addresses pulled from
dex_pool_snapshots, not synthetic ones -- a fake/nonexistent address
returns null account data and can't reproduce a size-driven failure
the way a real one can), 6 fails consistently, same error every time.
This is a plan-tier restriction on the specific API key configured for
this deployment (QuickNode's free "Discover" tier, per their own error
text and billing-page link), not a Solana RPC protocol limit --
GET_MULTIPLE_ACCOUNTS_BATCH_LIMIT (100) stays defined below as
documentation of that actual protocol ceiling, but
QUICKNODE_BATCH_LIMIT (4, one below the confirmed-working 5, same
margin-below-a-confirmed-number convention as every other rate limit
in this codebase) is what `_get_multiple_accounts_raw` actually chunks
at. A hundred-pool Tier 2 poll batch for one venue now costs 25
chunked calls instead of 1 -- see this session's summary for the
timed, live-verified confirmation that this still comfortably fits
inside the ~60s Tier 2 poll interval.

Rate limiting: this session's live check (see this session's summary)
found the configured SOLANA_RPC_URL host (QuickNode) documents a
free/"Discover"-tier limit of 15 req/s
(https://www.quicknode.com/builders-guide/best/top-10-free-rpc-endpoints-for-developers,
checked 2026-08-06) -- the actual provisioned plan for this
deployment's API key is unknown to this session (the key itself is
never inspected, per the security note below), so this figure is used
as a conservative floor, not an assumed ceiling. The already-running
ZORA Rust DEX ingestor (ingestor-dex-solana/src/rpc.rs) polls this
exact same host continuously in production at a proven-safe ~2.5 req/s
(400ms interval, POLL_INTERVAL) via the identical getMultipleAccounts
method. This provider's own steady-state need is far smaller still --
one getMultipleAccounts call per venue per ~60s Tier 2 poll (mint
decimals are fetched once, then cached) -- but its bucket is sized as a
defensive ceiling, not a reflection of expected throughput: each of
this module's two provider instances (Orca, Raydium CLMM) gets its own
RPC_BUCKET_CAPACITY_PER_PROVIDER=3 req/s bucket. Combined with the
Rust ingestor's proven ~2.5 req/s, a simultaneous worst case across all
three is ~8.5 req/s -- comfortably under the 15 req/s figure with real
margin, even though ZORA's paper-trading pipeline ('zora' tmux
session) and the MEV discovery subsystem ('zora-discovery' tmux
session) are independent processes that could both be hitting this
host at once. Per-instance (not shared) buckets are only correct
because tier2_poller.py's own poll loop calls each venue's provider
sequentially, never concurrently -- identical caveat to
raydium_common.py's same-host note for Raydium AMM v4/CLMM, and it
would need revisiting under the same conditions (a future move to
concurrent scheduler execution).

SECURITY NOTE: SOLANA_RPC_URL routinely carries an embedded API key in
its path (see CLAUDE.md's Environment section) that must never end up
in logs or be committed. This module never logs the URL itself, only
`urllib.parse.urlsplit(rpc_url).netloc` (the host) where logging is
needed at all -- same convention run_raydium_snapshot.py already
established (see that script's "Log only the host" comment, the fix
for the incident CLAUDE.md's security note refers to).
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Optional, Sequence

import base58
import requests

from discovery.http_client import DEFAULT_TIMEOUT_S, build_session
from discovery.rate_limiter import TokenBucket
from discovery.src.logger import Level, log
from discovery.src.providers.base import PoolState

VENUE_ORCA = "orca_whirlpool"
VENUE_RAYDIUM_CLMM = "raydium_clmm"

# Solana JSON-RPC's own documented hard cap for getMultipleAccounts --
# not a convention this project chose, a limit the *protocol* enforces.
# Kept here as documentation of that ceiling; NOT what this module
# actually chunks at -- see QUICKNODE_BATCH_LIMIT below and the module
# docstring's "getMultipleAccounts batch size" section for why those
# two numbers are very different in practice.
GET_MULTIPLE_ACCOUNTS_BATCH_LIMIT = 100

# The QuickNode-specific constraint this module actually has to chunk
# at, confirmed empirically against the live configured endpoint (see
# module docstring): this deployment's API key/plan enforces a 5-address
# ceiling per getMultipleAccounts call, well under the protocol's own
# 100-address cap above. One below that confirmed-working boundary
# (5 passed consistently, 6 failed consistently, every trial) as a
# small safety margin -- same margin-below-a-confirmed-number
# convention as this codebase's other rate limits (e.g. Meteora's/
# Raydium's 90% margins), even though the absolute gap is necessarily
# tiny here since the confirmed number itself is already small. This
# is a QuickNode plan-tier restriction, not a Solana RPC protocol
# limit -- upgrading the configured account's QuickNode plan would
# raise this, at which point this constant (and the empirical test
# behind it) should be re-run, not just bumped by guesswork.
QUICKNODE_BATCH_LIMIT = 4

# See module docstring's "Rate limiting" section for the 15 req/s
# citation and the ~8.5 req/s combined-worst-case reasoning this is
# margined against.
RPC_BUCKET_CAPACITY_PER_PROVIDER = 3.0  # tokens (~requests)
RPC_BUCKET_REFILL_PERIOD_S = 1.0

# --- Raydium CLMM `PoolState` offsets, ported 1:1 from clmm.rs -------
# (bytes into the *full* account data, discriminator included; see
# that module's doc comment for the live cross-check that pinned
# these).
CLMM_POOL_STATE_DISCRIMINATOR = bytes.fromhex("f7ede3f5d7c3de46")
CLMM_OFFSET_TOKEN_MINT_0 = 73
CLMM_OFFSET_TOKEN_MINT_1 = 105
CLMM_OFFSET_MINT_DECIMALS_0 = 233
CLMM_OFFSET_MINT_DECIMALS_1 = 234
CLMM_OFFSET_LIQUIDITY = 237
CLMM_OFFSET_SQRT_PRICE_X64 = 253
CLMM_OFFSET_TICK_CURRENT = 269
CLMM_MIN_ACCOUNT_LEN = CLMM_OFFSET_TICK_CURRENT + 4

# --- Orca `Whirlpool` offsets, ported 1:1 from orca.rs ----------------
WHIRLPOOL_DISCRIMINATOR = bytes.fromhex("3f95d10ce1806309")
WHIRLPOOL_OFFSET_LIQUIDITY = 49
WHIRLPOOL_OFFSET_SQRT_PRICE = 65
WHIRLPOOL_OFFSET_TICK_CURRENT_INDEX = 81
WHIRLPOOL_OFFSET_TOKEN_MINT_A = 101
WHIRLPOOL_OFFSET_TOKEN_MINT_B = 181
# orca.rs itself only reads through tick_current_index (offset 85) --
# this provider reads further, through token_mint_b, so its minimum
# length check must cover that too (still well under the real
# account's 653-byte length, see orca.rs's doc comment).
WHIRLPOOL_MIN_ACCOUNT_LEN = WHIRLPOOL_OFFSET_TOKEN_MINT_B + 32

# Standard SPL Token `Mint` account layout (spl-token's `state::Mint`,
# `Pack`-serialized): COption<Pubkey> mint_authority (4 + 32 = 36) +
# supply: u64 (8) + decimals: u8 (1) + is_initialized: bool (1) +
# COption<Pubkey> freeze_authority (36) = 82 bytes. Token-2022 mints
# share this identical 82-byte base layout before any TLV extension
# data appended after it, so no separate branch is needed for that
# program -- only the first 82 bytes are ever read here.
SPL_MINT_OFFSET_DECIMALS = 44
SPL_MINT_MIN_ACCOUNT_LEN = 82


# --- Price math, ported 1:1 from sqrt_price.rs ------------------------


def sqrt_price_to_price(sqrt_price_x64: int, decimals_0: int, decimals_1: int) -> float:
    """See sqrt_price.rs::sqrt_price_to_price's doc comment for the
    derivation. `sqrt_price_x64` is sqrt(token_1_raw / token_0_raw) in
    Q64.64 fixed point; this returns real token_1 units per real
    token_0 unit.
    """
    price_raw = (sqrt_price_x64 / (1 << 64)) ** 2
    return price_raw * (10 ** (decimals_0 - decimals_1))


def price_in_quote_per_base(
    sqrt_price_x64: int, decimals_0: int, decimals_1: int, base_is_mint_0: bool
) -> float:
    """See sqrt_price.rs::price_in_quote_per_base's doc comment. Zero
    handled explicitly to match Rust's IEEE-754 `1.0 / 0.0 == +inf`
    semantics (`price_in_quote_per_base_zero_is_zero_either_orientation`)
    -- plain Python float division raises ZeroDivisionError instead of
    producing +inf, so that case can't just be ported unchanged.
    """
    price_1_per_0 = sqrt_price_to_price(sqrt_price_x64, decimals_0, decimals_1)
    if base_is_mint_0:
        return price_1_per_0
    if price_1_per_0 == 0.0:
        return float("inf")
    return 1.0 / price_1_per_0


# --- Raw account parsing ----------------------------------------------


@dataclass(frozen=True)
class _RawPoolFields:
    """Whatever a pool account's raw bytes give us directly, before any
    cross-referencing against caller-supplied metadata (base_mint) or
    a separate mint-decimals lookup. `decimals_0`/`decimals_1` are
    `None` when the pool's own account doesn't carry them (Orca) --
    resolved separately via `_resolve_missing_decimals` before a
    `PoolState` can be produced.
    """

    mint_0: str
    mint_1: str
    decimals_0: Optional[int]
    decimals_1: Optional[int]
    liquidity: int
    sqrt_price_x64: int
    tick_current: int


def _parse_clmm_account(data: bytes) -> _RawPoolFields:
    """Ported 1:1 from clmm.rs::parse_clmm_account, extended to also
    decode token_mint_0/token_mint_1 (see module docstring for why)."""
    if len(data) < CLMM_MIN_ACCOUNT_LEN:
        raise ValueError(f"CLMM account too short: {len(data)} bytes, need at least {CLMM_MIN_ACCOUNT_LEN}")
    if data[0:8] != CLMM_POOL_STATE_DISCRIMINATOR:
        raise ValueError(
            f"account discriminator {data[0:8].hex()} doesn't match PoolState "
            f"{CLMM_POOL_STATE_DISCRIMINATOR.hex()} -- wrong pubkey or upstream layout change"
        )
    mint_0 = base58.b58encode(data[CLMM_OFFSET_TOKEN_MINT_0 : CLMM_OFFSET_TOKEN_MINT_0 + 32]).decode()
    mint_1 = base58.b58encode(data[CLMM_OFFSET_TOKEN_MINT_1 : CLMM_OFFSET_TOKEN_MINT_1 + 32]).decode()
    decimals_0 = data[CLMM_OFFSET_MINT_DECIMALS_0]
    decimals_1 = data[CLMM_OFFSET_MINT_DECIMALS_1]
    liquidity = int.from_bytes(data[CLMM_OFFSET_LIQUIDITY : CLMM_OFFSET_LIQUIDITY + 16], "little")
    sqrt_price_x64 = int.from_bytes(data[CLMM_OFFSET_SQRT_PRICE_X64 : CLMM_OFFSET_SQRT_PRICE_X64 + 16], "little")
    tick_current = int.from_bytes(
        data[CLMM_OFFSET_TICK_CURRENT : CLMM_OFFSET_TICK_CURRENT + 4], "little", signed=True
    )
    return _RawPoolFields(
        mint_0=mint_0,
        mint_1=mint_1,
        decimals_0=decimals_0,
        decimals_1=decimals_1,
        liquidity=liquidity,
        sqrt_price_x64=sqrt_price_x64,
        tick_current=tick_current,
    )


def _parse_whirlpool_account(data: bytes) -> _RawPoolFields:
    """Ported 1:1 from orca.rs::parse_whirlpool_account, extended to
    also decode token_mint_a/token_mint_b (see module docstring for
    why). decimals_0/decimals_1 come back None -- Orca's account
    doesn't carry them (see module docstring)."""
    if len(data) < WHIRLPOOL_MIN_ACCOUNT_LEN:
        raise ValueError(f"Whirlpool account too short: {len(data)} bytes, need at least {WHIRLPOOL_MIN_ACCOUNT_LEN}")
    if data[0:8] != WHIRLPOOL_DISCRIMINATOR:
        raise ValueError(
            f"account discriminator {data[0:8].hex()} doesn't match Whirlpool "
            f"{WHIRLPOOL_DISCRIMINATOR.hex()} -- wrong pubkey or upstream layout change"
        )
    mint_a = base58.b58encode(data[WHIRLPOOL_OFFSET_TOKEN_MINT_A : WHIRLPOOL_OFFSET_TOKEN_MINT_A + 32]).decode()
    mint_b = base58.b58encode(data[WHIRLPOOL_OFFSET_TOKEN_MINT_B : WHIRLPOOL_OFFSET_TOKEN_MINT_B + 32]).decode()
    liquidity = int.from_bytes(data[WHIRLPOOL_OFFSET_LIQUIDITY : WHIRLPOOL_OFFSET_LIQUIDITY + 16], "little")
    sqrt_price = int.from_bytes(data[WHIRLPOOL_OFFSET_SQRT_PRICE : WHIRLPOOL_OFFSET_SQRT_PRICE + 16], "little")
    tick_current_index = int.from_bytes(
        data[WHIRLPOOL_OFFSET_TICK_CURRENT_INDEX : WHIRLPOOL_OFFSET_TICK_CURRENT_INDEX + 4], "little", signed=True
    )
    return _RawPoolFields(
        mint_0=mint_a,
        mint_1=mint_b,
        decimals_0=None,
        decimals_1=None,
        liquidity=liquidity,
        sqrt_price_x64=sqrt_price,
        tick_current=tick_current_index,
    )


# --- Provider ------------------------------------------------------------


class SolanaRpcStateProviderBase:
    """Shared getMultipleAccounts batching, mint-decimals caching, and
    JSON-RPC plumbing for both concentrated-liquidity RPC state
    providers below. Subclasses set VENUE and `_parse_account` (the
    one thing that genuinely differs: offsets, discriminator, whether
    decimals are already on the pool account).

    `fetch_state`'s signature takes an extra `base_mint_by_pool`
    argument beyond `PoolStateProvider`'s strict
    `fetch_state(self, pool_addresses)` -- see module docstring for why
    orientation can't be derived from the account bytes alone. This
    makes the class not strictly conformant to that Protocol for a
    type-checked call through it, but tier2_poller.py calls it via its
    concrete type, never through the abstract Protocol, so this is a
    deliberate, documented, narrow deviation, not an oversight.
    """

    VENUE: str

    def __init__(
        self,
        rpc_url: str,
        session: Optional[requests.Session] = None,
        bucket: Optional[TokenBucket] = None,
    ):
        self._rpc_url = rpc_url
        self._session = session or build_session(allowed_methods=("POST",))
        self._bucket = bucket or TokenBucket(RPC_BUCKET_CAPACITY_PER_PROVIDER, RPC_BUCKET_REFILL_PERIOD_S)
        # Mint pubkey -> decimals. Never invalidated: a mint's decimals
        # are immutable once created, so this is safe to cache forever
        # on this long-lived instance (see build_tier2_providers()'s
        # docstring on Tier 2 providers being constructed once, not
        # rebuilt per cycle the way Tier 1's REST providers are).
        self._mint_decimals_cache: dict[str, int] = {}

    def _parse_account(self, data: bytes) -> _RawPoolFields:
        raise NotImplementedError

    def _get_multiple_accounts_raw(self, addresses: list[str]) -> dict[str, Optional[bytes]]:
        """Returns {address: raw_account_bytes_or_None (account not
        found)}, batching in chunks of QUICKNODE_BATCH_LIMIT -- the
        empirically-confirmed limit this deployment's endpoint actually
        accepts, NOT GET_MULTIPLE_ACCOUNTS_BATCH_LIMIT (the protocol's
        own, much larger ceiling) -- see module docstring's
        "getMultipleAccounts batch size" section for why those differ
        and how QUICKNODE_BATCH_LIMIT was found. A chunk-level RPC
        error (malformed request, node-side failure, a renewed 413 if
        this constant is ever wrong again) is logged and that chunk's
        addresses are simply absent from the result -- same
        failure-isolation shape as raydium_common.py's batch_skip, so
        one bad chunk doesn't take down pools in other chunks.
        """
        result: dict[str, Optional[bytes]] = {}
        for i in range(0, len(addresses), QUICKNODE_BATCH_LIMIT):
            chunk = addresses[i : i + QUICKNODE_BATCH_LIMIT]
            self._bucket.consume()
            try:
                resp = self._session.post(
                    self._rpc_url,
                    json={
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "getMultipleAccounts",
                        "params": [chunk, {"encoding": "base64", "commitment": "confirmed"}],
                    },
                    timeout=DEFAULT_TIMEOUT_S,
                )
                resp.raise_for_status()
                body = resp.json()
            except requests.exceptions.RequestException as exc:
                # requests' own exceptions (HTTPError, ConnectionError,
                # Timeout, the JSONDecodeError subclass raised by
                # resp.json() on a malformed body) embed the request
                # URL in their message -- which routinely carries an
                # embedded API key here (see module docstring's
                # SECURITY NOTE). Re-raised sanitized so nothing
                # upstream that logs str(exc) (tier2_poller.py's
                # poll_source_FAILED handler, for one) can leak it.
                # `from None` additionally suppresses the original
                # exception's context in any traceback this propagates
                # into.
                status = getattr(getattr(exc, "response", None), "status_code", None)
                raise RuntimeError(
                    f"getMultipleAccounts request failed (status={status}): {type(exc).__name__}"
                ) from None
            if "error" in body:
                log(
                    Level.ERROR,
                    [
                        ("event", "rpc_error"),
                        ("method", "getMultipleAccounts"),
                        ("venue", self.VENUE),
                        ("chunk_size", str(len(chunk))),
                        ("err", str(body["error"])),
                    ],
                )
                continue
            values = body["result"]["value"]
            for addr, account in zip(chunk, values):
                if account is None:
                    result[addr] = None
                    continue
                data_b64 = account["data"][0]
                result[addr] = base64.b64decode(data_b64)
        return result

    def _resolve_missing_decimals(self, mint_addresses: set[str]) -> None:
        to_fetch = sorted(m for m in mint_addresses if m not in self._mint_decimals_cache)
        if not to_fetch:
            return
        raw_by_mint = self._get_multiple_accounts_raw(to_fetch)
        for mint, data in raw_by_mint.items():
            if data is None or len(data) < SPL_MINT_MIN_ACCOUNT_LEN:
                log(
                    Level.WARN,
                    [
                        ("event", "mint_decimals_lookup_failed"),
                        ("mint", mint),
                        ("reason", "missing_or_short_account"),
                    ],
                )
                continue
            self._mint_decimals_cache[mint] = data[SPL_MINT_OFFSET_DECIMALS]

    def fetch_state(self, pool_addresses: Sequence[str], base_mint_by_pool: dict[str, str]) -> list[PoolState]:
        """Returns one PoolState per address that parsed cleanly and
        had a resolvable base_mint/decimals -- shorter than
        `pool_addresses` whenever an individual pool's account is
        missing, malformed, or its decimals/orientation couldn't be
        resolved. Never raises for a single bad pool (see
        `_get_multiple_accounts_raw`'s and this method's own WARN
        logging) -- only a network-level failure (connection error,
        non-2xx after retries) propagates, same failure-isolation
        contract every other provider in this codebase follows.
        """
        addresses = list(pool_addresses)
        if not addresses:
            return []

        raw_by_address = self._get_multiple_accounts_raw(addresses)

        parsed: dict[str, _RawPoolFields] = {}
        for addr in addresses:
            data = raw_by_address.get(addr)
            if data is None:
                log(Level.WARN, [("event", "parse_skip"), ("venue", self.VENUE), ("pool", addr), ("err", "account_not_found")])
                continue
            try:
                parsed[addr] = self._parse_account(data)
            except ValueError as exc:
                log(Level.WARN, [("event", "parse_skip"), ("venue", self.VENUE), ("pool", addr), ("err", str(exc))])

        missing_mints: set[str] = set()
        for fields in parsed.values():
            if fields.decimals_0 is None:
                missing_mints.add(fields.mint_0)
            if fields.decimals_1 is None:
                missing_mints.add(fields.mint_1)
        if missing_mints:
            self._resolve_missing_decimals(missing_mints)

        states: list[PoolState] = []
        for addr, fields in parsed.items():
            base_mint = base_mint_by_pool.get(addr)
            if base_mint is None:
                log(Level.WARN, [("event", "parse_skip"), ("venue", self.VENUE), ("pool", addr), ("err", "no_base_mint_supplied")])
                continue
            if base_mint == fields.mint_0:
                base_is_mint_0 = True
            elif base_mint == fields.mint_1:
                base_is_mint_0 = False
            else:
                log(Level.WARN, [("event", "parse_skip"), ("venue", self.VENUE), ("pool", addr), ("err", "base_mint_matches_neither_token")])
                continue

            decimals_0 = fields.decimals_0 if fields.decimals_0 is not None else self._mint_decimals_cache.get(fields.mint_0)
            decimals_1 = fields.decimals_1 if fields.decimals_1 is not None else self._mint_decimals_cache.get(fields.mint_1)
            if decimals_0 is None or decimals_1 is None:
                log(Level.WARN, [("event", "parse_skip"), ("venue", self.VENUE), ("pool", addr), ("err", "decimals_unresolved")])
                continue

            price_native = price_in_quote_per_base(fields.sqrt_price_x64, decimals_0, decimals_1, base_is_mint_0)
            states.append(
                PoolState(
                    venue=self.VENUE,
                    pool_address=addr,
                    price_native=price_native,
                    active_liquidity=float(fields.liquidity),
                )
            )
        return states


class RaydiumClmmRpcStateProvider(SolanaRpcStateProviderBase):
    """Orientation and decimals for Raydium CLMM `PoolState` accounts
    -- decimals come straight off the account (see module docstring),
    no mint lookup ever needed for this venue.
    """

    VENUE = VENUE_RAYDIUM_CLMM

    def _parse_account(self, data: bytes) -> _RawPoolFields:
        return _parse_clmm_account(data)


class OrcaWhirlpoolRpcStateProvider(SolanaRpcStateProviderBase):
    """Orca `Whirlpool` accounts -- decimals are NOT on the account
    (see module docstring), so this venue's pools always go through
    `_resolve_missing_decimals`'s mint lookup at least once.
    """

    VENUE = VENUE_ORCA

    def _parse_account(self, data: bytes) -> _RawPoolFields:
        return _parse_whirlpool_account(data)
