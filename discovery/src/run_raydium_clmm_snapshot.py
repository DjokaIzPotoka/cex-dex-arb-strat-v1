#!/usr/bin/env python3
"""Phase 1a one-shot Raydium CLMM discovery snapshot.

Same shape as run_raydium_snapshot.py (AMM v4), for the new
RaydiumClmmRestProvider. Not a running service -- see that file's
docstring for the general Phase 1a scope note, which applies here
unchanged.

Validation here cross-checks against Binance's public TRXUSDT ticker
rather than on-chain vault reserves: unlike AMM v4 (plain token
vaults), a CLMM pool's on-chain state is sqrt_price_x64 packed into a
tick-indexed account layout, and decoding that is explicitly Phase 1h
scope (see RaydiumClmmRestProvider's docstring), not this session's.
Binance is already ZORA's primary CEX venue (see CLAUDE.md), so this
reuses a source the project already trusts rather than introducing a
new one.

SECURITY NOTE: this script does NOT touch SOLANA_RPC_URL at all --
unlike the AMM v4 validation script, there's no on-chain call here, so
there's no embedded-API-key risk to guard against in the first place.
Keep it that way: if a later change adds an RPC call here, it must log
only the hostname, never the full URL (see run_raydium_snapshot.py's
fetch_vault_balance/compute_onchain_price for the pattern, and the
incident that made it necessary).

Usage:
    python3 discovery/src/run_raydium_clmm_snapshot.py                  # TRX/USDC + full broad pull, writes to ClickHouse
    python3 discovery/src/run_raydium_clmm_snapshot.py --dry-run         # same fetch/print/cross-check, skips the ClickHouse write
    python3 discovery/src/run_raydium_clmm_snapshot.py --no-broad        # TRX/USDC validation only
    python3 discovery/src/run_raydium_clmm_snapshot.py --max-pages 2     # bound the broad pull for faster iteration
"""
from __future__ import annotations

import argparse
import datetime
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from discovery.http_client import build_session  # noqa: E402
from discovery.src.logger import Level, log  # noqa: E402
from discovery.src.providers.raydium_clmm import RaydiumClmmRestProvider  # noqa: E402

# TRX/USDC's Raydium CLMM pool -- same pubkey ZORA's existing Rust DEX
# ingestor already polls for price (see ingestor-dex-solana/src/main.rs's
# TRX_CLMM_POOL_ID), 0.05% fee tier.
TRX_CLMM_POOL_ID = "HpgV2jnzgrGfrZjeZGkHgTnEgRFAhtzCVuk3BRFTFJwk"
EXPECTED_FEE_BPS = 5

BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/price"
BINANCE_SYMBOL = "TRXUSDT"

# Two independent sources (a Raydium-side API snapshot vs. a live
# Binance CEX quote), not the same-venue on-chain-vs-REST comparison
# AMM v4's script does -- genuine cross-venue price drift (not just
# measurement-instant drift) is expected here, so this stays wider
# than that script's 1%. Still tight enough to catch a real unit/
# formula bug (e.g. a missed canonicalization swap, which would be off
# by orders of magnitude, not a few percent).
PRICE_TOLERANCE_PCT = 5.0

# TVL sanity band around this project's earlier-observed ~$12.1M for
# this exact pool (see the task context this script was written for)
# -- wide enough to absorb ordinary multi-day market movement, tight
# enough to catch an order-of-magnitude parsing bug (e.g. a missed
# decimals conversion).
TVL_SANITY_MIN_USD = 1_000_000.0
TVL_SANITY_MAX_USD = 100_000_000.0


def fetch_binance_trx_usdt_price(session) -> float:
    resp = session.get(BINANCE_TICKER_URL, params={"symbol": BINANCE_SYMBOL}, timeout=10)
    resp.raise_for_status()
    return float(resp.json()["price"])


def write_rows(rows: list[dict], dry_run: bool) -> None:
    if dry_run:
        log(
            Level.INFO,
            [("event", "dry_run"), ("rows", str(len(rows))), ("note", "skipped_clickhouse_write")],
        )
        return
    from discovery.src.db import get_client

    client = get_client()
    columns = list(rows[0].keys())
    data = [[row[c] for c in columns] for row in rows]
    client.insert("dex_pool_snapshots", data, column_names=columns)
    log(
        Level.INFO,
        [("event", "clickhouse_insert"), ("table", "dex_pool_snapshots"), ("rows", str(len(rows)))],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="fetch/print/cross-check only, skip the ClickHouse write")
    parser.add_argument("--no-broad", action="store_true", help="skip the full Concentrated pool list pull")
    parser.add_argument("--max-pages", type=int, default=None, help="cap the broad pull's page count (page size 1000)")
    args = parser.parse_args()

    provider = RaydiumClmmRestProvider()
    snapshot_ts = datetime.datetime.now(datetime.timezone.utc)

    log(Level.INFO, [("event", "fetch_start"), ("pool", TRX_CLMM_POOL_ID)])
    trx_row = provider.get_pool(TRX_CLMM_POOL_ID)
    trx_snapshot = trx_row.to_snapshot_row(snapshot_ts=snapshot_ts, tier="discovery", hot_reason="")

    print("--- TRX/USDC CLMM validation row ---")
    for key, value in trx_snapshot.items():
        print(f"  {key}: {value}")

    fee_sane = trx_snapshot["fee_bps"] == EXPECTED_FEE_BPS
    tvl_sane = TVL_SANITY_MIN_USD <= trx_snapshot["tvl_usd"] <= TVL_SANITY_MAX_USD
    active_liquidity_placeholder = trx_snapshot["active_liquidity"] == trx_snapshot["tvl_usd"]
    log(
        Level.INFO if fee_sane else Level.ERROR,
        [
            ("event", "trx_fee_validation"),
            ("fee_bps", str(trx_snapshot["fee_bps"])),
            ("expected", str(EXPECTED_FEE_BPS)),
            ("fee_sane", str(fee_sane)),
        ],
    )
    log(
        Level.INFO if tvl_sane else Level.ERROR,
        [
            ("event", "trx_tvl_validation"),
            ("tvl_usd", f"{trx_snapshot['tvl_usd']:.2f}"),
            ("sanity_band", f"[{TVL_SANITY_MIN_USD:.0f}, {TVL_SANITY_MAX_USD:.0f}]"),
            ("tvl_sane", str(tvl_sane)),
        ],
    )
    log(
        Level.INFO,
        [
            ("event", "trx_active_liquidity_placeholder"),
            ("active_liquidity_eq_tvl", str(active_liquidity_placeholder)),
            ("note", "TODO(Phase 1h) placeholder, not correct-by-design (unlike AMM v4)"),
        ],
    )
    if not (fee_sane and tvl_sane):
        log(Level.ERROR, [("event", "trx_validation_FLAGGED"), ("note", "fee or TVL outside expected range -- investigate before trusting this pool's row")])

    log(Level.INFO, [("event", "binance_crosscheck_start"), ("symbol", BINANCE_SYMBOL)])
    cross_session = build_session()
    try:
        binance_price = fetch_binance_trx_usdt_price(cross_session)
    except (RuntimeError, ValueError, KeyError) as exc:
        log(Level.ERROR, [("event", "binance_crosscheck_failed"), ("err", str(exc))])
        binance_price = None

    if binance_price is not None:
        rest_price = trx_snapshot["price_native"]
        diff_pct = abs(rest_price - binance_price) / binance_price * 100
        within_tolerance = diff_pct <= PRICE_TOLERANCE_PCT
        log(
            Level.INFO if within_tolerance else Level.ERROR,
            [
                ("event", "binance_crosscheck"),
                ("rest_price_native", f"{rest_price:.8f}"),
                ("binance_trxusdt", f"{binance_price:.8f}"),
                ("diff_pct", f"{diff_pct:.4f}"),
                ("tolerance_pct", f"{PRICE_TOLERANCE_PCT}"),
                ("within_tolerance", str(within_tolerance)),
            ],
        )
        if not within_tolerance:
            log(
                Level.ERROR,
                [
                    ("event", "binance_crosscheck_FLAGGED"),
                    ("note", "REST price_native diverges from Binance TRXUSDT beyond tolerance -- do not trust this provider's price_native until investigated"),
                ],
            )

    rows = [trx_snapshot]

    if not args.no_broad:
        log(Level.INFO, [("event", "broad_pull_start"), ("max_pages", str(args.max_pages))])
        t0 = time.monotonic()
        kept, dropped_ghost = 0, 0
        for row in provider.list_pools(max_pages=args.max_pages):
            if row.volume_24h_usd <= 0:
                dropped_ghost += 1
                continue
            rows.append(row.to_snapshot_row(snapshot_ts=snapshot_ts, tier="discovery", hot_reason=""))
            kept += 1
        elapsed = time.monotonic() - t0
        log(
            Level.INFO,
            [
                ("event", "broad_pull_done"),
                ("pools_kept", str(kept)),
                ("pools_dropped_ghost", str(dropped_ghost)),
                ("elapsed_s", f"{elapsed:.1f}"),
            ],
        )

    write_rows(rows, args.dry_run)


if __name__ == "__main__":
    main()
