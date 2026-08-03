#!/usr/bin/env python3
"""Phase 1a one-shot Orca Whirlpool discovery snapshot.

Not a running service -- Tier 1/2 scheduling (docs/mev-discovery-plan.md
section 3.2) is explicitly out of scope until Phase 1b. This script:

1. Fetches the known KMNO/USDC Whirlpool pool as a concrete validation
   case and prints the parsed row (tvl_usd, price_native, fee_bps)
   against the doc's expected ballpark (~$0.019, 1bp fee).
2. Optionally walks the full Whirlpool universe (~142k pools, ~85s;
   see providers/orca_rest.py) for the broader discovery use case,
   dropping pools with zero 24h volume per section 3.7's "obvious
   ghosts only" filter -- the KMNO validation row is always kept
   regardless of that filter, since it's fetched for validation, not
   discovery breadth.
3. Writes everything to dex_pool_snapshots, all rows sharing one
   snapshot_ts (one Tier 1 cycle = one point-in-time snapshot).

Usage:
    python3 discovery/src/run_orca_snapshot.py                  # KMNO + full broad pull, writes to ClickHouse
    python3 discovery/src/run_orca_snapshot.py --dry-run         # same fetch/print, skips the ClickHouse write
    python3 discovery/src/run_orca_snapshot.py --no-broad        # KMNO validation only
    python3 discovery/src/run_orca_snapshot.py --max-pages 5     # bound the broad pull for faster iteration
"""
from __future__ import annotations

import argparse
import datetime
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from discovery.src.logger import Level, log  # noqa: E402
from discovery.src.providers.orca_rest import OrcaRestProvider  # noqa: E402

KMNO_POOL_ADDRESS = "3ndjN1nJVUKGrJBc1hhVpER6kWTZKHdyDrPyCJyX3CXK"


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
    parser.add_argument("--dry-run", action="store_true", help="fetch and print only, skip the ClickHouse write")
    parser.add_argument("--no-broad", action="store_true", help="skip the full Whirlpool universe pull")
    parser.add_argument("--max-pages", type=int, default=None, help="cap the broad pull's page count (page size 1000)")
    args = parser.parse_args()

    provider = OrcaRestProvider()
    snapshot_ts = datetime.datetime.now(datetime.timezone.utc)

    log(Level.INFO, [("event", "fetch_start"), ("pool", KMNO_POOL_ADDRESS)])
    kmno_row = provider.get_pool(KMNO_POOL_ADDRESS)
    kmno_snapshot = kmno_row.to_snapshot_row(snapshot_ts=snapshot_ts, tier="discovery", hot_reason="")

    print("--- KMNO validation row ---")
    for key, value in kmno_snapshot.items():
        print(f"  {key}: {value}")

    price_sane = 0.005 < kmno_snapshot["price_native"] < 0.05
    tvl_sane = kmno_snapshot["tvl_usd"] > 0
    fee_sane = kmno_snapshot["fee_bps"] == 1
    log(
        Level.INFO,
        [
            ("event", "kmno_validation"),
            ("price_native", f"{kmno_snapshot['price_native']:.8f}"),
            ("price_sane", str(price_sane)),
            ("tvl_usd", f"{kmno_snapshot['tvl_usd']:.2f}"),
            ("tvl_sane", str(tvl_sane)),
            ("fee_bps", str(kmno_snapshot["fee_bps"])),
            ("fee_sane", str(fee_sane)),
        ],
    )
    if not (price_sane and tvl_sane and fee_sane):
        log(Level.WARN, [("event", "kmno_validation_failed")])

    rows = [kmno_snapshot]

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
