"""known_entities / new_entity_events detection, per
docs/mev-discovery-plan.md section 3.6.

The registry check-then-insert pattern lives here exactly once,
parameterized by (entity_type, venue), rather than duplicated per DEX
venue or per CEX exchange -- the doc's section 3.6 describes one
detection mechanism that both sides share, and DEX pool identifiers
(pool_address) vs. CEX symbol identifiers (symbol) differ only in
which column feeds `identifier` and what goes in `metadata`, not in
the check-then-insert logic itself.

Deliberately NOT an anti-join against dex_pool_snapshots/
cex_symbol_snapshots history (the two problems section 3.6 calls out:
a transient REST miss would re-flag an already-known pool as "new" the
next time it reappears; and a query over a fact table that grows
without bound -- and is itself only 90-day TTL'd -- gets slower over
time and produces false "new" reports for anything quiet longer than
90 days). `known_entities` is a small, append-only, insert-once
registry whose only job is answering "have we ever seen this
identifier for this (entity_type, venue)" -- see that table's own
schema comment in migrations/001_create_tables.sql.

Known limitation carried over from the design doc verbatim: this
check-then-insert is only safe under a single sequential caller. Two
concurrent callers could both observe "not yet known" for the same
identifier and both insert, duplicating it in both tables. Not an
issue for this phase (nothing here runs concurrently -- there is no
scheduler yet, per this session's explicit scope), but worth carrying
forward if a later phase parallelizes detection per venue.
"""
from __future__ import annotations

import datetime
import json
from dataclasses import dataclass
from typing import Sequence

from discovery.src.logger import Level, log

ENTITY_TYPE_DEX_POOL = "dex_pool"
ENTITY_TYPE_CEX_SYMBOL = "cex_symbol"

# ClickHouse's HTTP interface enforces a per-form-field byte-size cap
# on parameterized-query bind values (confirmed empirically against a
# live 24.8 server during Phase 1e's full-universe seed: an
# Array(String) bind of 1711 identifiers, ~77KB, succeeded in one
# query; 3970 identifiers, ~178KB, failed with "HTML Form Exception:
# Field value too long" -- consistent with Poco's ~128KiB default
# field-value limit, which clickhouse-connect's automatic
# GET-to-POST-form fallback for large bind params doesn't route
# around, since the cap is enforced server-side on the form field
# itself, not on URL length). Raising that limit is a ClickHouse
# server-config change (http_max_field_value_size), out of scope here
# -- flagged rather than silently worked around. Instead, the
# existence check chunks its IN-list into sub-batches safely under
# that ceiling. Sized for the worst case on either side of this
# table: 44-char base58 Solana pool addresses (CEX symbols are much
# shorter) -- 1000 * ~50 bytes (address + per-element quoting
# overhead) ~= 50KB, comfortably under ~128KB. This keeps the common
# case (every CEX exchange, most DEX venues) at exactly one query per
# venue as section 3.6 describes, and degrades to a small, fixed
# number of chunked queries only for the handful of DEX venues large
# enough to need it -- orders of magnitude fewer than the
# one-query-per-identifier approach section 3.6 actually guards
# against.
EXISTENCE_CHECK_CHUNK_SIZE = 1000


@dataclass(frozen=True)
class EntityCandidate:
    """One currently-observed identifier for a given (entity_type,
    venue), carrying enough context to populate new_entity_events.metadata
    directly if it turns out to be new -- per section 3.6's requirement
    that metadata be useful later without a join back to the snapshot
    tables. `metadata` is a plain JSON-serializable dict; this module
    doesn't care what's in it, only that it can be json.dumps'd (see
    build_dex_candidate/build_cex_candidate below for what each side
    actually puts there).
    """

    identifier: str
    metadata: dict


@dataclass(frozen=True)
class DetectionResult:
    entity_type: str
    venue: str
    checked: int
    new_identifiers: list[str]

    @property
    def new_count(self) -> int:
        return len(self.new_identifiers)


def build_dex_candidate(row: dict) -> EntityCandidate:
    """Builds an EntityCandidate from one dex_pool_snapshots row (as a
    dict keyed by column name). metadata captures venue/base_symbol/
    quote_symbol/fee_bps/tvl_usd at detection time, per this session's
    requirement -- enough for later analysis to know what kind of pool
    this was without re-querying dex_pool_snapshots.
    """
    return EntityCandidate(
        identifier=row["pool_address"],
        metadata={
            "venue": row["venue"],
            "base_symbol": row["base_symbol"],
            "quote_symbol": row["quote_symbol"],
            "fee_bps": row["fee_bps"],
            "tvl_usd": row["tvl_usd"],
        },
    )


def build_cex_candidate(row: dict) -> EntityCandidate:
    """Builds an EntityCandidate from one cex_symbol_snapshots row (as
    a dict keyed by column name). metadata captures exchange/
    base_asset/quote_asset/status at detection time.
    """
    return EntityCandidate(
        identifier=row["symbol"],
        metadata={
            "exchange": row["exchange"],
            "base_asset": row["base_asset"],
            "quote_asset": row["quote_asset"],
            "status": row["status"],
        },
    )


def detect_new_entities(
    client,
    entity_type: str,
    venue: str,
    candidates: Sequence[EntityCandidate],
    detected_ts: datetime.datetime,
) -> DetectionResult:
    """Check-then-insert against known_entities for one venue's
    currently-observed identifier set, per section 3.6:

    1. A batched existence query (not one query per identifier -- see
       this session's scope note on keeping known_entities fast
       regardless of fact-table growth) asks which of `candidates`'
       identifiers already exist in known_entities for this
       (entity_type, venue). One query for venues under
       EXISTENCE_CHECK_CHUNK_SIZE identifiers (the common case); a
       small, fixed number of chunked queries above that, to stay
       under ClickHouse's HTTP form-field size cap -- see that
       constant's module-level comment for why.
    2. Whichever don't: insert once into known_entities (append-only,
       never updated -- see that table's schema comment) and insert a
       matching row into new_entity_events with metadata captured at
       detection time.
    3. Whichever do: skipped entirely, registry untouched -- section
       3.6 point 3, "za one koji vec postoje: nema upisa".

    Duplicate identifiers within `candidates` (the same pool_address
    appearing twice in one Tier-1-shaped batch, e.g. a venue's
    dedicated validation pool also turning up in its own broad pull)
    collapse to one candidate -- last one wins -- since this is keyed
    into a dict before the existence check; `checked` reports the
    deduplicated count, not len(candidates).

    Empty `candidates` is a no-op (nothing to check, nothing to
    insert), not an error -- an empty currently-observed set for a
    venue is a legitimate (if unusual) outcome, not a caller bug.
    """
    if not candidates:
        return DetectionResult(entity_type=entity_type, venue=venue, checked=0, new_identifiers=[])

    by_identifier = {c.identifier: c for c in candidates}
    all_identifiers = list(by_identifier.keys())

    known_identifiers: set[str] = set()
    for start in range(0, len(all_identifiers), EXISTENCE_CHECK_CHUNK_SIZE):
        chunk = all_identifiers[start : start + EXISTENCE_CHECK_CHUNK_SIZE]
        existing = client.query(
            "SELECT identifier FROM known_entities "
            "WHERE entity_type = {entity_type:String} AND venue = {venue:String} "
            "AND identifier IN {identifiers:Array(String)}",
            parameters={
                "entity_type": entity_type,
                "venue": venue,
                "identifiers": chunk,
            },
        )
        known_identifiers.update(row[0] for row in existing.result_rows)

    new_identifiers = [ident for ident in all_identifiers if ident not in known_identifiers]

    if new_identifiers:
        known_entities_rows = [
            [entity_type, venue, ident, detected_ts] for ident in new_identifiers
        ]
        client.insert(
            "known_entities",
            known_entities_rows,
            column_names=["entity_type", "venue", "identifier", "first_seen_ts"],
        )

        new_entity_rows = [
            [detected_ts, entity_type, venue, ident, json.dumps(by_identifier[ident].metadata)]
            for ident in new_identifiers
        ]
        client.insert(
            "new_entity_events",
            new_entity_rows,
            column_names=["detected_ts", "entity_type", "venue", "identifier", "metadata"],
        )

    log(
        Level.INFO,
        [
            ("event", "entity_detection"),
            ("entity_type", entity_type),
            ("venue", venue),
            ("checked", str(len(all_identifiers))),
            ("new", str(len(new_identifiers))),
        ],
    )

    return DetectionResult(
        entity_type=entity_type,
        venue=venue,
        checked=len(all_identifiers),
        new_identifiers=new_identifiers,
    )
