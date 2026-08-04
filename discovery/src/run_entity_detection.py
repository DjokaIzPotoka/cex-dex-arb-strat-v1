#!/usr/bin/env python3
"""One-shot entity-detection run against dex_pool_snapshots and
cex_symbol_snapshots' latest snapshot per venue/exchange, per
docs/mev-discovery-plan.md section 3.6. Not a running service -- same
scope note as every prior run_*.py script in this project; the
scheduler that would call this on every Tier 1 cycle is explicit
later-session scope (this session builds only the detection step
itself), not built here.

"Latest snapshot_ts per venue/exchange" stands in for what a real Tier
1 cycle would call "currently observed": dex_pool_snapshots/
cex_symbol_snapshots today hold exactly one snapshot_ts per venue
(each provider-validation session ran its snapshot script once) -- a
future scheduler replaces "latest" with "this cycle's own snapshot_ts"
directly, the query shape doesn't otherwise change.

Usage:
    python3 discovery/src/run_entity_detection.py                 # real detection pass over latest snapshots
    python3 discovery/src/run_entity_detection.py --synthetic-test # inject one fake identifier into a real venue's batch, verify only it gets flagged, then clean up

SECURITY NOTE: this script does NOT touch SOLANA_RPC_URL -- it only
reads/writes ClickHouse, no Solana RPC call exists here at all.
"""
from __future__ import annotations

import argparse
import datetime
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from discovery.src.db import get_client  # noqa: E402
from discovery.src.detection import (  # noqa: E402
    ENTITY_TYPE_CEX_SYMBOL,
    ENTITY_TYPE_DEX_POOL,
    EntityCandidate,
    build_cex_candidate,
    build_dex_candidate,
    detect_new_entities,
)
from discovery.src.logger import Level, log  # noqa: E402

# Deliberately not a realistic-looking pool address (real base58
# pubkeys don't contain underscores) -- this identifier must never be
# mistakable for genuine on-chain data if the cleanup step below ever
# failed to run.
SYNTHETIC_IDENTIFIER = "ZORA_SYNTHETIC_TEST_POOL_DO_NOT_USE"
SYNTHETIC_VENUE = "orca_whirlpool"


def latest_dex_candidates_by_venue(client) -> dict[str, list[EntityCandidate]]:
    result = client.query(
        """
        SELECT venue, pool_address, base_symbol, quote_symbol, fee_bps, tvl_usd
        FROM dex_pool_snapshots
        WHERE (venue, snapshot_ts) IN (
            SELECT venue, max(snapshot_ts) FROM dex_pool_snapshots GROUP BY venue
        )
        """
    )
    by_venue: dict[str, list[EntityCandidate]] = {}
    for row in result.named_results():
        by_venue.setdefault(row["venue"], []).append(build_dex_candidate(row))
    return by_venue


def latest_cex_candidates_by_exchange(client) -> dict[str, list[EntityCandidate]]:
    result = client.query(
        """
        SELECT exchange, symbol, base_asset, quote_asset, status
        FROM cex_symbol_snapshots
        WHERE (exchange, snapshot_ts) IN (
            SELECT exchange, max(snapshot_ts) FROM cex_symbol_snapshots GROUP BY exchange
        )
        """
    )
    by_exchange: dict[str, list[EntityCandidate]] = {}
    for row in result.named_results():
        by_exchange.setdefault(row["exchange"], []).append(build_cex_candidate(row))
    return by_exchange


def run_detection_pass(client) -> None:
    detected_ts = datetime.datetime.now(datetime.timezone.utc)

    dex_by_venue = latest_dex_candidates_by_venue(client)
    cex_by_exchange = latest_cex_candidates_by_exchange(client)

    total_checked = 0
    total_new = 0

    print("--- DEX venues ---")
    for venue, candidates in sorted(dex_by_venue.items()):
        result = detect_new_entities(client, ENTITY_TYPE_DEX_POOL, venue, candidates, detected_ts)
        print(f"  {venue}: checked={result.checked} new={result.new_count}")
        total_checked += result.checked
        total_new += result.new_count

    print("--- CEX exchanges ---")
    for exchange, candidates in sorted(cex_by_exchange.items()):
        result = detect_new_entities(client, ENTITY_TYPE_CEX_SYMBOL, exchange, candidates, detected_ts)
        print(f"  {exchange}: checked={result.checked} new={result.new_count}")
        total_checked += result.checked
        total_new += result.new_count

    log(
        Level.INFO,
        [
            ("event", "detection_pass_done"),
            ("total_checked", str(total_checked)),
            ("total_new", str(total_new)),
        ],
    )


def run_synthetic_test(client) -> None:
    """Injects one identifier that has never appeared in
    dex_pool_snapshots into a real venue's already-known batch (3 real
    pool_addresses pulled live from that venue's own data), and
    confirms:
    (a) the synthetic one alone is flagged new and lands in both
        known_entities and new_entity_events,
    (b) the real, already-known ones in the same batch are correctly
        skipped, not re-flagged.

    Requires the main detection pass to have already run at least once
    (so the 3 real pool_addresses pulled here are already in
    known_entities) -- aborts loudly rather than producing a
    meaningless result if that's not true yet.

    Cleans up the synthetic rows from both tables afterward: leaving
    them would permanently dirty new_entity_events, which
    docs/mev-discovery-plan.md section 3.8 keeps forever (no TTL).
    """
    real_rows = client.query(
        "SELECT pool_address, base_symbol, quote_symbol, fee_bps, tvl_usd "
        "FROM dex_pool_snapshots WHERE venue = {venue:String} LIMIT 3",
        parameters={"venue": SYNTHETIC_VENUE},
    )
    real_candidates = [
        EntityCandidate(
            identifier=row[0],
            metadata={
                "venue": SYNTHETIC_VENUE,
                "base_symbol": row[1],
                "quote_symbol": row[2],
                "fee_bps": row[3],
                "tvl_usd": row[4],
            },
        )
        for row in real_rows.result_rows
    ]
    if not real_candidates:
        log(
            Level.ERROR,
            [
                ("event", "synthetic_test_ABORTED"),
                ("reason", f"no existing rows for venue={SYNTHETIC_VENUE} -- run the main detection pass first"),
            ],
        )
        return

    already_known = client.query(
        "SELECT count(*) FROM known_entities WHERE entity_type = {et:String} AND venue = {venue:String} "
        "AND identifier IN {ids:Array(String)}",
        parameters={
            "et": ENTITY_TYPE_DEX_POOL,
            "venue": SYNTHETIC_VENUE,
            "ids": [c.identifier for c in real_candidates],
        },
    ).result_rows[0][0]
    if already_known != len(real_candidates):
        log(
            Level.ERROR,
            [
                ("event", "synthetic_test_ABORTED"),
                ("reason", f"expected all {len(real_candidates)} sampled real pools already in known_entities, found {already_known} -- run the main detection pass first"),
            ],
        )
        return

    synthetic_candidate = EntityCandidate(
        identifier=SYNTHETIC_IDENTIFIER,
        metadata={"venue": SYNTHETIC_VENUE, "base_symbol": "SYNX", "quote_symbol": "USDC", "fee_bps": 30, "tvl_usd": 1.0},
    )
    batch = real_candidates + [synthetic_candidate]

    print(f"--- synthetic-entity test: venue={SYNTHETIC_VENUE}, batch of {len(batch)} ({len(real_candidates)} already-known real + 1 synthetic) ---")
    detected_ts = datetime.datetime.now(datetime.timezone.utc)
    result = detect_new_entities(client, ENTITY_TYPE_DEX_POOL, SYNTHETIC_VENUE, batch, detected_ts)

    print(f"  new_identifiers: {result.new_identifiers}")
    ok = result.new_identifiers == [SYNTHETIC_IDENTIFIER]
    log(
        Level.INFO if ok else Level.ERROR,
        [
            ("event", "synthetic_test_result"),
            ("expected_new", SYNTHETIC_IDENTIFIER),
            ("actual_new", str(result.new_identifiers)),
            ("real_entries_correctly_skipped", str(len(real_candidates))),
            ("PASS", str(ok)),
        ],
    )

    ke_count = client.query(
        "SELECT count(*) FROM known_entities WHERE entity_type = {et:String} AND venue = {venue:String} AND identifier = {id:String}",
        parameters={"et": ENTITY_TYPE_DEX_POOL, "venue": SYNTHETIC_VENUE, "id": SYNTHETIC_IDENTIFIER},
    ).result_rows[0][0]
    nee_count = client.query(
        "SELECT count(*) FROM new_entity_events WHERE entity_type = {et:String} AND venue = {venue:String} AND identifier = {id:String}",
        parameters={"et": ENTITY_TYPE_DEX_POOL, "venue": SYNTHETIC_VENUE, "id": SYNTHETIC_IDENTIFIER},
    ).result_rows[0][0]
    print(f"  known_entities rows for synthetic identifier (direct DB query): {ke_count}")
    print(f"  new_entity_events rows for synthetic identifier (direct DB query): {nee_count}")
    log(
        Level.INFO,
        [
            ("event", "synthetic_test_db_verify"),
            ("known_entities_rows", str(ke_count)),
            ("new_entity_events_rows", str(nee_count)),
        ],
    )

    client.command(
        "ALTER TABLE known_entities DELETE WHERE entity_type = {et:String} AND venue = {venue:String} AND identifier = {id:String}",
        parameters={"et": ENTITY_TYPE_DEX_POOL, "venue": SYNTHETIC_VENUE, "id": SYNTHETIC_IDENTIFIER},
    )
    client.command(
        "ALTER TABLE new_entity_events DELETE WHERE entity_type = {et:String} AND venue = {venue:String} AND identifier = {id:String}",
        parameters={"et": ENTITY_TYPE_DEX_POOL, "venue": SYNTHETIC_VENUE, "id": SYNTHETIC_IDENTIFIER},
    )
    log(Level.INFO, [("event", "synthetic_test_cleanup_done")])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--synthetic-test",
        action="store_true",
        help="run the synthetic-new-entity injection test instead of the real detection pass",
    )
    args = parser.parse_args()

    client = get_client()
    if args.synthetic_test:
        run_synthetic_test(client)
    else:
        run_detection_pass(client)


if __name__ == "__main__":
    main()
