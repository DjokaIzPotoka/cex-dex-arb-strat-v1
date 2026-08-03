#!/usr/bin/env python3
"""Phase 1a one-shot Meteora DLMM discovery snapshot.

Same shape as run_raydium_clmm_snapshot.py, for the new
MeteoraDlmmRestProvider. Not a running service -- see
run_orca_snapshot.py's docstring for the general Phase 1a scope note,
which applies here unchanged.

Unlike every prior validation script in this project, no pool address
is hardcoded here: none was confirmed reliable for this session, and
Meteora's DLMM universe routinely spins up and abandons pools (see
MeteoraDlmmRestProvider's docstring on how common zero-liquidity ghost
pools are here). Instead, this discovers the highest-TVL SOL-USDC pool
at run time via the provider's `query` name-filter -- a targeted
search (~123 candidates) rather than a full ~122k-pool walk, since the
goal here is finding one good validation pool cheaply, not exercising
the broad-pull path (that's covered separately, right after, by
list_pools() with no filter).

Validation cross-checks against Binance's public SOLUSDT ticker, same
pattern and reasoning as run_raydium_clmm_snapshot.py's TRX/USDC check
(Binance is already ZORA's primary CEX venue -- see CLAUDE.md).

SECURITY NOTE: this script does NOT touch SOLANA_RPC_URL at all -- same
as run_raydium_clmm_snapshot.py, there's no on-chain call here, so
there's no embedded-API-key risk in the first place. Keep it that way;
see that file's own security note for the pattern to follow if this
ever changes.

Usage:
    python3 discovery/src/run_meteora_snapshot.py                  # SOL/USDC + full broad pull, writes to ClickHouse
    python3 discovery/src/run_meteora_snapshot.py --dry-run         # same fetch/print/cross-check, skips the ClickHouse write
    python3 discovery/src/run_meteora_snapshot.py --no-broad        # SOL/USDC validation only
    python3 discovery/src/run_meteora_snapshot.py --max-pages 5     # bound the broad pull for faster iteration
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
from discovery.src.providers.meteora_dlmm import MeteoraDlmmRestProvider  # noqa: E402

# Tried in order until one yields at least one pool with positive TVL
# -- SOL-USDC is expected to resolve on the first try (confirmed
# during this session: 123 candidates, top one ~$5M TVL), but a named
# fallback is cheap insurance against this venue's high abandoned-pool
# rate (see provider docstring) leaving the primary query's results
# all zero-liquidity on some future run.
CANDIDATE_QUERIES = ["SOL-USDC", "SOL-USDT"]

BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/price"
BINANCE_SYMBOL_BY_QUOTE = {"USDC": "SOLUSDT", "USDT": "SOLUSDT"}  # USDC ~= USDT for this cross-check's purposes

# Cross-venue comparison (Meteora REST snapshot vs. live Binance CEX
# quote), not same-venue on-chain-vs-REST -- same reasoning and same
# tolerance as run_raydium_clmm_snapshot.py's TRX/USDC check.
PRICE_TOLERANCE_PCT = 5.0


def find_best_pool(provider: MeteoraDlmmRestProvider):
    """Searches CANDIDATE_QUERIES in order, returns (query_used, best_row)
    for the first query with at least one positive-TVL, exact-pair-match
    result, sorted by tvl_usd descending. Raises RuntimeError if none
    of the candidates resolve to anything usable.

    Matched against post-canonicalization {base_symbol, quote_symbol}
    rather than the raw API `name` string (which MeteoraPoolRow doesn't
    even retain): canonicalize() already normalizes base/quote order
    consistently regardless of which side Meteora's `token_x`/`token_y`
    happened to put SOL on, so a set comparison is both simpler and the
    semantically correct check here ("is this a SOL/USDC pair" -- not
    "does this pool's raw name string happen to read left-to-right the
    same way the query string does").
    """
    for query in CANDIDATE_QUERIES:
        wanted = set(query.split("-"))
        candidates = [
            row
            for row in provider.list_pools(page_size=200, query=query)
            if {row.base_symbol, row.quote_symbol} == wanted
        ]
        if not candidates:
            log(Level.WARN, [("event", "candidate_query_empty"), ("query", query)])
            continue
        candidates.sort(key=lambda r: r.tvl_usd, reverse=True)
        return query, candidates[0]
    raise RuntimeError(f"none of {CANDIDATE_QUERIES} resolved to a usable pool")


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
    parser.add_argument("--no-broad", action="store_true", help="skip the full DLMM pool list pull")
    parser.add_argument("--max-pages", type=int, default=None, help="cap the broad pull's page count (page size 1000)")
    args = parser.parse_args()

    provider = MeteoraDlmmRestProvider()
    snapshot_ts = datetime.datetime.now(datetime.timezone.utc)

    log(Level.INFO, [("event", "discover_start"), ("candidates", ",".join(CANDIDATE_QUERIES))])
    query_used, best_row = find_best_pool(provider)
    log(
        Level.INFO,
        [
            ("event", "discover_done"),
            ("query", query_used),
            ("pool", best_row.pool_address),
            ("tvl_usd", f"{best_row.tvl_usd:.2f}"),
        ],
    )

    snapshot = best_row.to_snapshot_row(snapshot_ts=snapshot_ts, tier="discovery", hot_reason="")

    print(f"--- {query_used} (highest TVL) validation row ---")
    for key, value in snapshot.items():
        print(f"  {key}: {value}")

    active_liquidity_placeholder = snapshot["active_liquidity"] == snapshot["tvl_usd"]
    log(
        Level.INFO,
        [
            ("event", "active_liquidity_placeholder"),
            ("active_liquidity_eq_tvl", str(active_liquidity_placeholder)),
            ("note", "TODO(Phase 1h) placeholder, not correct-by-design (unlike AMM v4)"),
        ],
    )

    binance_symbol = BINANCE_SYMBOL_BY_QUOTE.get(snapshot["quote_symbol"])
    if binance_symbol is None:
        log(Level.WARN, [("event", "crosscheck_skipped"), ("reason", f"no Binance mapping for quote={snapshot['quote_symbol']}")])
    else:
        log(Level.INFO, [("event", "binance_crosscheck_start"), ("symbol", binance_symbol)])
        cross_session = build_session()
        try:
            resp = cross_session.get(BINANCE_TICKER_URL, params={"symbol": binance_symbol}, timeout=10)
            resp.raise_for_status()
            binance_price = float(resp.json()["price"])
        except (RuntimeError, ValueError, KeyError) as exc:
            log(Level.ERROR, [("event", "binance_crosscheck_failed"), ("err", str(exc))])
            binance_price = None

        if binance_price is not None:
            rest_price = snapshot["price_native"]
            diff_pct = abs(rest_price - binance_price) / binance_price * 100
            within_tolerance = diff_pct <= PRICE_TOLERANCE_PCT
            log(
                Level.INFO if within_tolerance else Level.ERROR,
                [
                    ("event", "binance_crosscheck"),
                    ("rest_price_native", f"{rest_price:.8f}"),
                    ("binance_price", f"{binance_price:.8f}"),
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
                        ("note", "REST price_native diverges from Binance beyond tolerance -- do not trust this provider's price_native until investigated"),
                    ],
                )

    rows = [snapshot]

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
