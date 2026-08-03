-- Verbatim from docs/mev-discovery-plan.md section 3.5, with one
-- forced deviation: ClickHouse 24.8 rejects a TTL expression whose
-- result type is DateTime64 (BAD_TTL_EXPRESSION -- confirmed against a
-- real 24.8 server, not a hypothetical), so `snapshot_ts` on its own
-- doesn't work as a TTL expression even though it's fine everywhere
-- else (PARTITION BY, ORDER BY). Wrapped in toDateTime(...) below,
-- which only affects TTL bucketing (day-level, as the doc intends for
-- a 90-day retention window) -- the column itself stays DateTime64(3)
-- exactly as specified, millisecond precision intact for every other
-- use. Otherwise do not add IF NOT EXISTS or diverge further -- if the
-- schema needs to change beyond this, change it in the design doc
-- first, then add a new numbered migration here.

CREATE TABLE dex_pool_snapshots (
    snapshot_ts        DateTime64(3),
    venue               LowCardinality(String),  -- orca_whirlpool, raydium_amm_v4, raydium_clmm, meteora_dlmm
    pool_address        String,
    base_mint            String,
    base_symbol          LowCardinality(String),
    quote_mint           String,
    quote_symbol         LowCardinality(String),
    fee_bps             UInt16,
    tvl_usd             Float64,
    active_liquidity    Float64,   -- likvidnost u aktivnom tick/bin range-u (CLMM/DLMM); = tvl_usd za AMM v4
    volume_24h_usd       Float64,
    price_native         Float64,   -- cena 1 base u quote jedinicama, direktno iz pool state-a, bez eksterne konverzije — autoritativna za Fazu 2 matching
    price_usd            Nullable(Float64),  -- provider-ova USD konverzija (ako je API nudi); samo za display/Tier1 filtriranje, NIJE osnova za spread computation
    tier                LowCardinality(String),  -- 'discovery' | 'hot'
    hot_reason           LowCardinality(String)   -- '' | 'new_pool' | 'volume_spike' | 'tvl_spike' | 'new_listing'
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (venue, pool_address, snapshot_ts)
TTL toDateTime(snapshot_ts) + INTERVAL 90 DAY;

CREATE TABLE cex_symbol_snapshots (
    snapshot_ts     DateTime64(3),
    exchange        LowCardinality(String),  -- binance, bybit, okx, bitget, gateio
    symbol          String,
    base_asset      LowCardinality(String),
    quote_asset     LowCardinality(String),
    status          LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_ts)
ORDER BY (exchange, symbol, snapshot_ts)
TTL toDateTime(snapshot_ts) + INTERVAL 90 DAY;

CREATE TABLE new_entity_events (
    detected_ts     DateTime64(3),
    entity_type     LowCardinality(String),  -- dex_pool | cex_symbol
    venue            LowCardinality(String),
    identifier       String,                  -- pool_address ili symbol
    metadata         String                   -- JSON blob
) ENGINE = MergeTree()
ORDER BY (detected_ts, entity_type);
-- bez TTL, čuva se trajno

CREATE TABLE known_entities (
    entity_type     LowCardinality(String),  -- dex_pool | cex_symbol
    venue            LowCardinality(String),
    identifier       String,
    first_seen_ts    DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (entity_type, venue, identifier);
-- append-only registry, insert samo jednom po identifier-u (nikad update),
-- bez TTL, bez last_seen polja — jedina odgovornost je "da li smo ovo ikad videli"
