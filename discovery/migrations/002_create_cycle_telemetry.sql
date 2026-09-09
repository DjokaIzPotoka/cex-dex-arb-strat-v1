-- Phase 2a run telemetry (see discovery/src/telemetry.py's module
-- docstring). NOT part of docs/mev-discovery-plan.md section 3.5 --
-- that document predates this work and doesn't cover telemetry at
-- all, so there's no schema to keep in sync with it here, unlike
-- 001_create_tables.sql's own tables. One row per (cycle, segment):
-- roughly 13 rows per 5-minute Tier 1 cycle (9 sources + ~3 Tier 2
-- polls + 1 detection pass).
--
-- IF NOT EXISTS (deliberately unlike 001_create_tables.sql's plain
-- CREATE TABLE, kept verbatim from the design doc on purpose): this
-- table has no design-doc source of truth to diverge from, and
-- RunTelemetry's own no-op-able contract means a scheduler restart
-- should never fail loudly just because this table already exists.
--
-- No TTL, no ReplacingMergeTree -- same insert-once-per-row reasoning
-- as known_entities/new_entity_events (001_create_tables.sql): each
-- segment is written exactly once, from cycle_end()'s single buffered
-- INSERT for that cycle, never updated afterward.
CREATE TABLE IF NOT EXISTS cycle_telemetry (
    run_id            String,
    cycle_n           UInt32,
    cycle_started_at  DateTime64(3, 'UTC'),
    segment_idx       UInt16,
    segment_name      LowCardinality(String),
    segment_kind      LowCardinality(String),  -- source | tier2_poll | detection
    duration_s        Float32,
    rows_written      UInt32,
    failed            UInt8,
    error_class       LowCardinality(String),  -- '' when not failed
    extra             String                   -- JSON blob, segment-specific detail (Meteora page range, RPC chunk count, cap hits)
) ENGINE = MergeTree
ORDER BY (run_id, cycle_n, segment_idx);
