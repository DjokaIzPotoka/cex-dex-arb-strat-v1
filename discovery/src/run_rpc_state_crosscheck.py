#!/usr/bin/env python3
"""Three-way cross-check for solana_rpc_state.py (Tier 2 RPC
PoolStateProvider), per this session's critical-validation requirement
(docs/mev-discovery-plan.md section 3.3): for the same live pool, the
new Python RPC provider's `price_native` must agree with (i) the
existing REST provider's `price_native` (independent data source and
independent code path) AND (ii) the ZORA DEX ingestor's actual
production Rust code (clmm.rs/orca.rs/sqrt_price.rs), run live via
ingestor-dex-solana/src/bin/live_price_check.rs -- not the Rust unit
tests' frozen snapshot constants, a genuine simultaneous independent
reading in the other language. A silent offset/units bug in the ported
byte-offset parsing would show up as a *systematic* mismatch here
(10x, reciprocal, or similarly structural), not as ordinary
cross-source price drift -- see this module's TOLERANCE constants for
what "drift" is allowed to look like before something is flagged.

Validation pools: KMNO/USDC (Orca Whirlpool) and TRX/USDC (Raydium
CLMM) -- the same two pools sqrt_price.rs/orca.rs/clmm.rs's own doc
comments and tests were validated against originally.

Usage:
    python3 discovery/src/run_rpc_state_crosscheck.py
"""
from __future__ import annotations

import os
import pathlib
import subprocess
import sys
import urllib.parse

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from discovery.src.logger import Level, log  # noqa: E402
from discovery.src.providers.raydium_clmm import RaydiumClmmRestProvider  # noqa: E402
from discovery.src.providers.orca_rest import OrcaRestProvider  # noqa: E402
from discovery.src.providers.solana_rpc_state import (  # noqa: E402
    OrcaWhirlpoolRpcStateProvider,
    RaydiumClmmRpcStateProvider,
)

KMNO_USDC_WHIRLPOOL = "3ndjN1nJVUKGrJBc1hhVpER6kWTZKHdyDrPyCJyX3CXK"
TRX_USDC_CLMM = "HpgV2jnzgrGfrZjeZGkHgTnEgRFAhtzCVuk3BRFTFJwk"

DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com"

# RPC vs REST: reads aren't taken at literally the same instant (three
# separate round trips -- REST, then the live Rust binary, then this
# provider), so ordinary price drift between them is expected. 2%
# mirrors sqrt_price.rs's own test tolerance for these same two pools
# (TRX/KMNO both use 2% there) -- wide enough for normal drift, narrow
# enough to catch a structural bug (10x decimals error, reciprocal
# orientation error), which would blow well past this.
CROSSCHECK_TOLERANCE_PCT = 2.0

# RPC (Python) vs the live Rust binary: same underlying account read
# via getMultipleAccounts vs getAccountInfo -- both against "confirmed"
# commitment, seconds apart, so this should track far tighter than the
# REST comparison (same source data, same formula, different
# language). A wide gap here specifically -- not just vs. REST -- means
# the *port* has a bug, not just ordinary market drift.
RPC_VS_RUST_TOLERANCE_PCT = 0.05

LIVE_PRICE_CHECK_BIN = (
    pathlib.Path(__file__).resolve().parents[2] / "ingestor-dex-solana" / "target" / "release" / "live_price_check"
)


def run_live_rust_check(rpc_url: str) -> dict[str, float]:
    """Shells out to the actual compiled Rust production functions
    (see ingestor-dex-solana/src/bin/live_price_check.rs's docstring)
    and parses its two `price_native=` values out of stdout. This is
    the (ii) leg of the three-way check -- not a reimplementation, the
    real clmm.rs/orca.rs/sqrt_price.rs code, run standalone.
    """
    if not LIVE_PRICE_CHECK_BIN.exists():
        raise FileNotFoundError(
            f"{LIVE_PRICE_CHECK_BIN} not built -- run "
            "`cargo build --release --bin live_price_check` in ingestor-dex-solana/ first"
        )
    env = dict(os.environ)
    env["SOLANA_RPC_URL"] = rpc_url
    result = subprocess.run(
        [str(LIVE_PRICE_CHECK_BIN)], capture_output=True, text=True, env=env, timeout=30, check=True
    )
    prices: dict[str, float] = {}
    for line in result.stdout.splitlines():
        if line.startswith("KMNO/USDC"):
            prices["kmno_usdc"] = float(line.rsplit("price_native=", 1)[1])
        elif line.startswith("TRX/USDC"):
            prices["trx_usdc"] = float(line.rsplit("price_native=", 1)[1])
    if "kmno_usdc" not in prices or "trx_usdc" not in prices:
        raise RuntimeError(f"couldn't parse both prices out of live_price_check output:\n{result.stdout}")
    return prices


def _pct_diff(a: float, b: float) -> float:
    return abs(a - b) / b * 100 if b else float("inf")


def main() -> None:
    rpc_url = os.environ.get("SOLANA_RPC_URL") or DEFAULT_RPC_URL
    # Log only the host, never the full URL -- see CLAUDE.md's
    # Environment section / this module's security note.
    rpc_host = urllib.parse.urlsplit(rpc_url).netloc or "?"
    log(Level.INFO, [("event", "crosscheck_start"), ("rpc_host", rpc_host)])

    # --- (i) REST provider: supplies price_native AND the base_mint
    # this provider needs to resolve orientation (see
    # solana_rpc_state.py's module docstring) -- reusing Tier 1's own
    # canonicalize() output rather than hardcoding mint addresses here.
    orca_rest = OrcaRestProvider().get_pool(KMNO_USDC_WHIRLPOOL)
    clmm_rest = RaydiumClmmRestProvider().get_pool(TRX_USDC_CLMM)
    log(
        Level.INFO,
        [
            ("event", "rest_fetched"),
            ("kmno_usdc_price_native", f"{orca_rest.price_native:.10f}"),
            ("trx_usdc_price_native", f"{clmm_rest.price_native:.10f}"),
        ],
    )

    # --- (ii) live Rust production code
    rust_prices = run_live_rust_check(rpc_url)
    log(
        Level.INFO,
        [
            ("event", "rust_live_fetched"),
            ("kmno_usdc_price_native", f"{rust_prices['kmno_usdc']:.10f}"),
            ("trx_usdc_price_native", f"{rust_prices['trx_usdc']:.10f}"),
        ],
    )

    # --- (iii) new Python RPC provider
    orca_rpc = OrcaWhirlpoolRpcStateProvider(rpc_url)
    clmm_rpc = RaydiumClmmRpcStateProvider(rpc_url)
    orca_states = orca_rpc.fetch_state([KMNO_USDC_WHIRLPOOL], {KMNO_USDC_WHIRLPOOL: orca_rest.base_mint})
    clmm_states = clmm_rpc.fetch_state([TRX_USDC_CLMM], {TRX_USDC_CLMM: clmm_rest.base_mint})
    if not orca_states:
        raise RuntimeError("Python RPC provider returned nothing for KMNO/USDC -- see WARN logs above")
    if not clmm_states:
        raise RuntimeError("Python RPC provider returned nothing for TRX/USDC -- see WARN logs above")
    py_kmno = orca_states[0]
    py_trx = clmm_states[0]
    log(
        Level.INFO,
        [
            ("event", "python_rpc_fetched"),
            ("kmno_usdc_price_native", f"{py_kmno.price_native:.10f}"),
            ("kmno_usdc_active_liquidity", str(py_kmno.active_liquidity)),
            ("trx_usdc_price_native", f"{py_trx.price_native:.10f}"),
            ("trx_usdc_active_liquidity", str(py_trx.active_liquidity)),
        ],
    )

    print()
    print("=== Three-way cross-check ===")
    all_ok = True
    for label, rest_price, rust_price, py_price in (
        ("KMNO/USDC", orca_rest.price_native, rust_prices["kmno_usdc"], py_kmno.price_native),
        ("TRX/USDC", clmm_rest.price_native, rust_prices["trx_usdc"], py_trx.price_native),
    ):
        rpc_vs_rust_pct = _pct_diff(py_price, rust_price)
        rpc_vs_rest_pct = _pct_diff(py_price, rest_price)
        rpc_vs_rust_ok = rpc_vs_rust_pct <= RPC_VS_RUST_TOLERANCE_PCT
        rpc_vs_rest_ok = rpc_vs_rest_pct <= CROSSCHECK_TOLERANCE_PCT
        all_ok = all_ok and rpc_vs_rust_ok and rpc_vs_rest_ok
        print(f"--- {label} ---")
        print(f"  REST price_native:        {rest_price:.10f}")
        print(f"  Rust (live) price_native:  {rust_price:.10f}")
        print(f"  Python RPC price_native:   {py_price:.10f}")
        print(
            f"  Python RPC vs Rust (live):  {rpc_vs_rust_pct:.6f}% "
            f"({'OK' if rpc_vs_rust_ok else 'FAIL'}, tolerance {RPC_VS_RUST_TOLERANCE_PCT}%)"
        )
        print(
            f"  Python RPC vs REST:         {rpc_vs_rest_pct:.4f}% "
            f"({'OK' if rpc_vs_rest_ok else 'FAIL'}, tolerance {CROSSCHECK_TOLERANCE_PCT}%)"
        )

    print()
    if all_ok:
        print("RESULT: PASS -- Python RPC provider agrees with both the live Rust production code and REST")
    else:
        print("RESULT: FAIL -- see above; do not wire this provider into tier2_poller.py until this passes")
        sys.exit(1)


if __name__ == "__main__":
    main()
