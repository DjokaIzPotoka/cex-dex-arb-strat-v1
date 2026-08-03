#!/usr/bin/env python3
"""Phase 1c one-shot Bybit CEX symbol snapshot -- second CEX provider
(after Binance), same cex_symbol_snapshots table.

Validation mirrors run_binance_snapshot.py:
1. Confirms known pairs resolve correctly, including KMNOUSDT.
2. Spot-checks base/quote parsing against a pair that would be
   genuinely ambiguous to split by suffix-stripping alone (USDCUSDT) --
   trusts baseCoin/quoteCoin instead, same as every provider in this
   batch.
3. Prints row counts, including a status breakdown -- see
   BybitSymbolProvider's docstring: the live spot list carried only
   `Trading` in this snapshot (no halted symbols observed), unlike
   Binance/Bitget, so a single-status breakdown here is expected, not
   a bug.

SECURITY NOTE: this script does NOT touch SOLANA_RPC_URL -- it's
CEX-only, no Solana RPC call exists here at all.

Usage:
    python3 discovery/src/run_bybit_snapshot.py               # fetch, validate, write to ClickHouse
    python3 discovery/src/run_bybit_snapshot.py --dry-run      # same fetch/validate, skips the ClickHouse write
"""
from __future__ import annotations

import argparse
import datetime
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from discovery.src.logger import Level, log  # noqa: E402
from discovery.src.providers.bybit_symbols import BybitSymbolProvider  # noqa: E402

EXPECTED_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "KMNOUSDT"]

# Confirmed live to exist and resolve correctly via the API's own
# baseCoin/quoteCoin fields, not string-parsing (Bybit's symbol string
# has no separator, same ambiguity shape as Binance's).
AMBIGUOUS_SPLIT_CHECKS = {
    "USDCUSDT": ("USDC", "USDT"),
    "TUSDUSDT": ("TUSD", "USDT"),
}


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
    client.insert("cex_symbol_snapshots", data, column_names=columns)
    log(
        Level.INFO,
        [("event", "clickhouse_insert"), ("table", "cex_symbol_snapshots"), ("rows", str(len(rows)))],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="fetch/validate only, skip the ClickHouse write")
    args = parser.parse_args()

    provider = BybitSymbolProvider()
    snapshot_ts = datetime.datetime.now(datetime.timezone.utc)

    log(Level.INFO, [("event", "fetch_start"), ("exchange", "bybit")])
    symbols = provider.fetch_symbols()
    log(Level.INFO, [("event", "fetch_done"), ("count", str(len(symbols)))])

    by_symbol = {s.symbol: s for s in symbols}

    print("--- known-pair validation ---")
    all_expected_ok = True
    for sym in EXPECTED_SYMBOLS:
        row = by_symbol.get(sym)
        if row is None:
            print(f"  {sym}: MISSING")
            all_expected_ok = False
        else:
            print(f"  {sym}: base={row.base_asset} quote={row.quote_asset} status={row.status}")
    log(Level.INFO if all_expected_ok else Level.ERROR, [("event", "known_pair_validation"), ("all_present", str(all_expected_ok))])

    print("--- ambiguous-split spot-check ---")
    all_splits_ok = True
    for sym, (expected_base, expected_quote) in AMBIGUOUS_SPLIT_CHECKS.items():
        row = by_symbol.get(sym)
        if row is None:
            print(f"  {sym}: MISSING (can't verify)")
            all_splits_ok = False
            continue
        ok = row.base_asset == expected_base and row.quote_asset == expected_quote
        print(f"  {sym}: base={row.base_asset} quote={row.quote_asset} (expected {expected_base}/{expected_quote}) -> {'OK' if ok else 'MISMATCH'}")
        all_splits_ok = all_splits_ok and ok
    log(Level.INFO if all_splits_ok else Level.ERROR, [("event", "ambiguous_split_validation"), ("all_correct", str(all_splits_ok))])

    status_counts: dict[str, int] = {}
    for s in symbols:
        status_counts[s.status] = status_counts.get(s.status, 0) + 1
    print("--- status breakdown (all written, none filtered) ---")
    for status, count in sorted(status_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {status}: {count}")
    log(Level.INFO, [("event", "status_breakdown"), ("statuses", str(status_counts))])

    if not (all_expected_ok and all_splits_ok):
        log(Level.ERROR, [("event", "validation_FLAGGED"), ("note", "one or more checks failed -- investigate before trusting this snapshot")])

    rows = [s.to_snapshot_row(snapshot_ts) for s in symbols]
    write_rows(rows, args.dry_run)


if __name__ == "__main__":
    main()
