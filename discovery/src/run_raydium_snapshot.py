#!/usr/bin/env python3
"""Phase 1a one-shot Raydium AMM v4 discovery snapshot.

Same shape as run_orca_snapshot.py, for the new RaydiumAmmV4RestProvider.
Not a running service -- see that file's docstring for the general
Phase 1a scope note, which applies here unchanged.

Validation here is stronger than Orca's: rather than only sanity-checking
against documented ballpark numbers, this cross-checks the REST
provider's price_native against a live, independent computation from
raw on-chain vault reserves -- the exact same math ZORA's existing Rust
DEX ingestor uses (pool::calculate_price in ingestor-dex-solana/src/pool.rs),
reimplemented here in Python against a direct getTokenAccountBalance
RPC call rather than by running the Rust binary. Pool address and vault
pubkeys are RAY/USDC's, copied from ingestor-dex-solana/src/main.rs
(the only Standard-AMM v4 pool ZORA's ingestor still tracks -- SOL/USDC
moved to a Raydium CLMM pool, which is out of scope for this provider).

Usage:
    python3 discovery/src/run_raydium_snapshot.py                  # RAY/USDC + full broad pull, writes to ClickHouse
    python3 discovery/src/run_raydium_snapshot.py --dry-run         # same fetch/print/cross-check, skips the ClickHouse write
    python3 discovery/src/run_raydium_snapshot.py --no-broad        # RAY/USDC validation only
    python3 discovery/src/run_raydium_snapshot.py --max-pages 5     # bound the broad pull for faster iteration
"""
from __future__ import annotations

import argparse
import datetime
import os
import pathlib
import sys
import time
import urllib.parse

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from discovery.http_client import build_session  # noqa: E402
from discovery.src.logger import Level, log  # noqa: E402
from discovery.src.providers.raydium_amm_v4 import RaydiumAmmV4RestProvider  # noqa: E402

# ingestor-dex-solana/src/main.rs's `PoolConfig` for RAY/USDC -- the
# only Standard-AMM v4 pool the existing ingestor still polls via
# vault balances (see that file's comment on why SOL/USDC/BONK/ZEC
# moved to CLMM/Whirlpool instead).
RAY_USDC_POOL_ID = "6UmmUiYoBjSrhakAobJw8BvkmJtDVxaeBtbt7rxWo1mg"
RAY_VAULT = "FdmKUE4UMiJYFK5ogCngHzShuVKrFXBamPWcewDr31th"
USDC_VAULT = "Eqrhxd7bDUCH3MepKmdVkgwazXRzY6iHhEoBpY7yAohk"

DEFAULT_RPC_URL = "https://api.mainnet-beta.solana.com"

# The REST API snapshot and the two vault RPC calls aren't taken at
# literally the same instant, and normal trading moves the pool
# between them -- 1% comfortably covers that ordinary drift while
# still catching an actual unit/formula bug (which would be off by a
# much larger, structural factor, e.g. 10x from a decimals mismatch).
PRICE_TOLERANCE_PCT = 1.0


def fetch_vault_balance(session, rpc_url: str, vault_pubkey: str) -> float:
    """Returns the vault's ui-normalized (decimals-adjusted) balance,
    via the same getTokenAccountBalance RPC call
    ingestor-dex-solana/src/rpc.rs's batched getMultipleAccounts
    ultimately derives its reserve numbers from (that call fetches raw
    account bytes for many accounts at once; this fetches one account
    already decimals-adjusted by the RPC node -- same underlying
    on-chain state, cheaper to reproduce for a one-off check).
    """
    resp = session.post(
        rpc_url,
        json={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTokenAccountBalance",
            "params": [vault_pubkey],
        },
        timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    if "error" in body:
        raise RuntimeError(f"RPC error for {vault_pubkey}: {body['error']}")
    return float(body["result"]["value"]["uiAmountString"])


def compute_onchain_price(session, rpc_url: str) -> float:
    """Reproduces pool.rs's calculate_price: usdc_normalized / base_normalized."""
    reserve_ray = fetch_vault_balance(session, rpc_url, RAY_VAULT)
    reserve_usdc = fetch_vault_balance(session, rpc_url, USDC_VAULT)
    if reserve_ray == 0.0:
        raise ValueError("RAY vault balance is zero -- can't compute a price")
    return reserve_usdc / reserve_ray


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
    parser.add_argument("--no-broad", action="store_true", help="skip the full Standard-AMM pool list pull")
    parser.add_argument("--max-pages", type=int, default=None, help="cap the broad pull's page count (page size 1000)")
    args = parser.parse_args()

    rpc_url = os.environ.get("SOLANA_RPC_URL") or DEFAULT_RPC_URL
    provider = RaydiumAmmV4RestProvider()
    snapshot_ts = datetime.datetime.now(datetime.timezone.utc)

    log(Level.INFO, [("event", "fetch_start"), ("pool", RAY_USDC_POOL_ID)])
    ray_row = provider.get_pool(RAY_USDC_POOL_ID)
    ray_snapshot = ray_row.to_snapshot_row(snapshot_ts=snapshot_ts, tier="discovery", hot_reason="")

    print("--- RAY/USDC validation row ---")
    for key, value in ray_snapshot.items():
        print(f"  {key}: {value}")

    # Log only the host, never the full URL: SOLANA_RPC_URL routinely
    # carries an embedded API key in its path (see CLAUDE.md's
    # Environment section) that must never end up in logs, same as it
    # must never be committed.
    rpc_host = urllib.parse.urlsplit(rpc_url).netloc or "?"
    log(Level.INFO, [("event", "onchain_crosscheck_start"), ("rpc_host", rpc_host)])
    rpc_session = build_session()
    try:
        onchain_price = compute_onchain_price(rpc_session, rpc_url)
    except (RuntimeError, ValueError, KeyError) as exc:
        log(Level.ERROR, [("event", "onchain_crosscheck_failed"), ("err", str(exc))])
        onchain_price = None

    if onchain_price is not None:
        rest_price = ray_snapshot["price_native"]
        diff_pct = abs(rest_price - onchain_price) / onchain_price * 100
        within_tolerance = diff_pct <= PRICE_TOLERANCE_PCT
        log(
            Level.INFO if within_tolerance else Level.ERROR,
            [
                ("event", "onchain_crosscheck"),
                ("rest_price_native", f"{rest_price:.8f}"),
                ("onchain_price", f"{onchain_price:.8f}"),
                ("diff_pct", f"{diff_pct:.4f}"),
                ("tolerance_pct", f"{PRICE_TOLERANCE_PCT}"),
                ("within_tolerance", str(within_tolerance)),
            ],
        )
        if not within_tolerance:
            log(
                Level.ERROR,
                [
                    ("event", "onchain_crosscheck_FLAGGED"),
                    ("note", "REST price diverges from live vault reserves beyond tolerance -- do not trust this provider's price_native until investigated"),
                ],
            )

    fee_sane = ray_snapshot["fee_bps"] == 25  # RAY/USDC's documented 0.25% (CLAUDE.md)
    active_liquidity_correct = ray_snapshot["active_liquidity"] == ray_snapshot["tvl_usd"]
    log(
        Level.INFO,
        [
            ("event", "ray_validation"),
            ("fee_bps", str(ray_snapshot["fee_bps"])),
            ("fee_sane", str(fee_sane)),
            ("active_liquidity_eq_tvl", str(active_liquidity_correct)),
        ],
    )

    rows = [ray_snapshot]

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
