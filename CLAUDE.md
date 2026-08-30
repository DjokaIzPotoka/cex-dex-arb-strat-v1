# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

ZORA: a CEX-DEX arbitrage system for Solana. Three cooperating processes read/write POSIX shared memory to detect price discrepancies between Binance (CEX) and Raydium (DEX) pools, size trades, and record them to CSV (paper trading only — no real orders are submitted).

## Build & Run

```bash
make build          # builds all three: CEX (C++), DEX (Rust), spread detector (Rust)
make start          # launches everything in a tmux session (sources .env for RPC URL)
make stop           # SIGTERM → wait 3s → SIGKILL, tears down tmux
make status         # PID check for all three processes
make attach         # re-attach to existing tmux session
make rebuild        # clean + build
```

### Building individual components

```bash
# CEX ingestor (C++17, requires Boost, OpenSSL, simdjson)
mkdir -p build && cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .

# DEX ingestor (Rust, outside the workspace)
cd ingestor-dex-solana && cargo build --release

# Spread detector (Rust, workspace member)
cargo build --release --manifest-path spread-detector/Cargo.toml
```

### Running tests

```bash
# Spread detector (the only component with tests)
cargo test --manifest-path spread-detector/Cargo.toml

# Single test
cargo test --manifest-path spread-detector/Cargo.toml -- test_name
```

The DEX ingestor has no tests. The CEX ingestor (C++) has no test target.

## Architecture

### Process communication via POSIX shared memory

All three processes communicate through POSIX shared memory segments (`shm_open`/`mmap`). Each trading pair gets two segments: one written by the CEX ingestor, one by the DEX ingestor, both read by the spread detector.

**Segment names** are defined in three places that must stay in sync:
- `shared/include/zora_ipc.h` — C macros (`ZORA_SHM_CEX_NAME`, etc.)
- `ingestor-dex-solana/src/types.rs` — Rust constants (DEX names only)
- `spread-detector/src/types.rs` — Rust constants (both CEX and DEX names)

**Wire structs** are independently authored in each language to match the same byte layout:
- `ZoraCexPrice` (56 bytes): defined in `zora_ipc.h` (C), duplicated in `spread-detector/src/types.rs` (Rust)
- `ZoraDexPrice` (48 bytes): defined in `ingestor-dex-solana/src/types.rs`, duplicated in `spread-detector/src/types.rs`

Layout tests in `spread-detector/src/types.rs` pin the exact `size_of`/`align_of` for both structs. If you change any struct field, update all three codebases and verify the layout tests pass.

**Synchronization**: writers do a single `atomic_store(release)` on the `sequence` field after all payload fields are written. Readers use a sequence-lock pattern (read sequence before/after, retry if they differ). This is NOT a textbook seqlock — there is no odd/even parity check.

### CEX ingestor (C++, `src/`)

Connects to Binance's combined WebSocket stream for all pairs. Parses bookTicker JSON with simdjson's on-demand API. Routes each symbol to the correct `ShmWriter` via a static table of `PairRoute`s in `BookTickerHandler`.

Key files: `src/main.cpp` (pair setup, stream path), `src/handlers/book_ticker_handler.cpp` (parsing + routing), `src/ipc/shm_writer.cpp` (SHM write logic).

### DEX ingestor (Rust, `ingestor-dex-solana/`)

Polls Raydium pool vault balances via `getMultipleAccounts` RPC (all pools in one batched call). Computes price from reserves using the constant-product formula. Deliberately kept outside the Cargo workspace (has its own `target/` directory).

Key files: `src/main.rs` (pool configs with vault pubkeys/decimals), `src/rpc.rs` (batched RPC + per-pool parsing), `src/pool.rs` (price calculation).

The RPC URL defaults to the public Solana endpoint but is overridable via `SOLANA_RPC_URL` env var (set in `.env`, sourced by `scripts/zora.sh`).

### Spread detector (Rust, `spread-detector/`)

Reads all pairs' CEX+DEX shared memory in a tight spin loop. Computes gross/net spread, classifies as Watching/Interesting/Opportunity, sizes profitable trades via binary search on the constant-product curve, and records to CSV.

Key files: `src/main.rs` (poll loop, staleness handling, pair contexts), `src/detector.rs` (spread math, classification), `src/sizing.rs` (optimal trade sizing), `src/paper.rs` (CSV recording), `src/shm.rs` (SHM reader with seqlock).

## Adding a New Trading Pair

Requires changes in all three components:

1. **CEX ingestor**: add SHM name macro to `zora_ipc.h`, add `ShmWriter` + `PairRoute` + signal_gap/shutdown in `src/main.cpp`, append to WebSocket stream path
2. **DEX ingestor**: add SHM name constant to `types.rs`, add `PoolConfig` entry in `main.rs` with correct pool ID, vault pubkeys, and `base_decimals`
3. **Spread detector**: add both CEX and DEX SHM name constants to `types.rs`, add `ShmReader::try_open` call and `PairContext` entry in `main.rs`

Token decimals matter: SOL=9, RAY=6, BONK=5, ZEC=8, TRX=6, KMNO=6. Price calculation in `pool.rs` (Standard-AMM pairs) or `sqrt_price.rs` (CLMM/Whirlpool pairs) uses these to normalize reserves/price correctly.

For very low-priced pairs (like BONK at ~$0.000003), set `min_abs_price_delta` on the `PairContext` to filter quoting-noise false signals. Price display uses 8 decimal places throughout.

Each pair also carries its own DEX pool fee (`detector::total_costs_pct` in the spread detector) rather than one global assumption -- fees vary by an order of magnitude or more across pools/programs (Standard-AMM Raydium pools are typically 0.25%, CLMM/Whirlpool pools range from 0.01% to 0.25%, and thin/unusual pools can be far higher). Check the pool's actual fee via Raydium's/Orca's pool-info API before wiring in a new pair rather than assuming it matches an existing one.

## Current Pair Roster

SOL/USDC (Raydium CLMM), RAY/USDC (Raydium Standard AMM), BONK/USDC (Raydium CLMM), ZEC/USDC (Orca Whirlpool), TRX/USDC (Raydium CLMM), KMNO/USDC (Orca Whirlpool)

UNI/USDC was removed: no pool with meaningful liquidity exists for it on either Raydium or Orca (every alternative checked had single-digit-dollar TVL and no volume).

## MEV Discovery Subsystem (`discovery/`)

A separate, broad-coverage MEV opportunity discovery pipeline (Python + ClickHouse) — not part of the CEX-DEX paper trading pipeline described above. Full design: `docs/mev-discovery-plan.md`. Phase 1 (all 4 DEX venues + all 5 CEX exchanges, `known_entities`/`new_entity_events` detection, the Tier 1 scheduler, rate limiting on all 9 sources, Tier 2 promotion/demotion, and the Tier 2 RPC state provider) is complete as of Phase 1h. Phase 1h (`discovery/src/providers/solana_rpc_state.py`) replaced Tier 2's REST-based state polling with a batched `getMultipleAccounts` RPC provider for Orca Whirlpool and Raydium CLMM specifically — ported 1:1 from the ZORA DEX ingestor's own production-validated `clmm.rs`/`orca.rs`/`sqrt_price.rs` (byte offsets, discriminators, and the sqrt_price_x64→price formula), not re-derived; validated via a live three-way cross-check (Python RPC provider vs. the live Rust production code vs. the existing REST provider, `make discovery-test-rpc-crosscheck`) before being wired in. `active_liquidity` is genuinely decoded from on-chain `liquidity` for Tier 2-hot pools on those two venues now; Raydium AMM v4 (no RPC benefit — see `raydium_amm_v4.py`) and Meteora DLMM (bin-based state, deferred) still carry the `tvl_usd` placeholder. Scheduler execution is still sequential (not concurrent) — Phase 1e's reason for that (DEX providers had no rate limiting) is gone as of Phase 1f, but switching to concurrent execution is a deliberately deferred decision needing its own testing, not done yet (see `scheduler.py`'s TODO).

**Tier 2** (`discovery/src/tier2.py`, `discovery/src/tier2_poller.py`, section 3.2): age-based promotion (fixed 72h window from `new_entity_events`, never demoted early — state seeded from that permanent table on every scheduler startup, since it's not itself a ClickHouse table per the design doc's explicit instruction) and growth-based promotion/demotion (re-scored every Tier 1 cycle, OR rule on `volume_growth_24h > 3x` / `tvl_growth_24h > 2x` / new CEX listing for the pool's base token, 3-consecutive-cycle hysteresis before demotion) are two fully independent in-process registries (`Tier2State`), never persisted as a table. Tier 2's 1-minute re-poll of currently-hot pools runs inline inside the same single-threaded Tier 1 loop (checked at each of the 9 per-source checkpoints, not its own thread — see `tier2_poller.py`'s docstring for why), and caps + round-robins each venue's poll batch at `MAX_POOLS_PER_VENUE_PER_POLL` (100): an uncapped pass against a large real age-based backlog (seen live: 5558 Orca pools alone) tries one-HTTP-request-per-pool against Orca's 5 req/s bucket and stalls the whole scheduler for ~18 minutes — this cap exists specifically because that happened. The RPC path's own per-call `getMultipleAccounts` batch size is further capped at `QUICKNODE_BATCH_LIMIT=4` (`solana_rpc_state.py`) — the configured `SOLANA_RPC_URL` host (QuickNode's free "Discover" plan) 413s above 5 addresses per call (its own error text: `"getMultipleAccounts is limited to a 5 range, upgrade from discover plan..."`), well under the protocol's own 100-account cap (`GET_MULTIPLE_ACCOUNTS_BATCH_LIMIT`, kept only as documentation of that looser ceiling). 4 is one below the confirmed-working boundary, same margin-below-a-confirmed-number convention as this codebase's other rate limits. This fix (committed as `178ea7b`) was independently re-confirmed via a fresh live binary search against real hot-pool addresses for both `orca_whirlpool` and `raydium_clmm` (100 down through 6 all fail with HTTP 413, 5 always succeeds) — same boundary, corroborating the original finding.

**Meteora sub-cycle pagination** (`discovery/src/providers/meteora_dlmm.py`'s `MeteoraCursor`, wired in via `scheduler.py`'s `PARTIAL_COVERAGE_VENUES`): Meteora's full paginated walk (~126k pools, 127 pages at the API's max `page_size=1000`) took ~289s — ~46% of every 5-minute Tier 1 cycle on its own, the dominant cause of every cycle overrunning to a measured ~618s average. Root cause confirmed empirically, not assumed: page size is already maxed (the API 400s above `page_size=1000`), and per-page cost is ~99% response download time for a ~1.9MB payload (not JSON parsing, not our 27 req/s `TokenBucket`, which sat far below saturation) — 127 sequential HTTP round-trips is an unavoidable cost of a full sweep every cycle, not a client-side inefficiency to tune away. Fix (Option 2): `MeteoraCursor` is a persistent, in-process page-range cursor, constructed once at scheduler startup (same lifetime as `Tier2State`, restart just starts a fresh sweep from page 1 — accepted, since a full sweep is only ~3 cycles / ~15 min), advancing by ~1/3 of the universe's pages each Tier 1 cycle and wrapping back to page 1 after a full pass. **This is a deliberate, documented deviation from design doc section 3.2's "every Tier 1 cycle covers the full universe"** — that invariant now holds only every ~3 cycles (~15 min) for `meteora_dlmm` specifically; the other 8 sources are unaffected and still get full coverage every single cycle. Coverage is best-effort, not an exhaustive guarantee: page *boundaries* can drift across the ~15-minute sweep if the universe's size changes mid-sweep (the API exposes no stable cursor token, only page/page_size), so a pool could in principle land in two slices or in neither during one sweep — same class of tradeoff `tier2_poller.py`'s own poll cadence already accepts. This does NOT risk `detect_new_entities()` misreading an unswept pool as absent: that check is purely additive (a pool not in this cycle's candidates is simply not checked this cycle, never marked "gone"). It DID require fixing `GrowthBasedRegistry.evaluate()`'s prior-snapshot lookup for `meteora_dlmm`: the original single-"venue's-last-snapshot_ts" comparison silently found zero prior rows for almost every pool once consecutive cycles cover disjoint page slices, so partial-coverage venues now compare each pool against its own individually most-recent prior row (`fetch_prior_snapshot_per_pool`) instead — equivalent to the old behavior for the other 3 (still full-coverage) DEX venues, so nothing changed for them.

Validated live post-fix, together with the QuickNode `QUICKNODE_BATCH_LIMIT=4` fix above (3 consecutive real Tier 1 cycles, zero 413s throughout): Meteora's own per-cycle cost dropped from ~289s to a measured ~95-100s (pages 1-42, then 43-84, then 85-126, confirming both the rotation and the full-sweep wraparound work — note total_pages itself drifted 127→126 between runs, and the cursor adapted correctly), roughly the targeted 1/3. Total cycle duration dropped from the pre-fix ~618s average to a measured **~487.7s average (483.7s / 490.2s / 489.2s)** — a real ~21% reduction, but well short of the 300s target; don't assume this alone gets there. Two things eat the remaining gap, neither in this fix's scope: `orca_whirlpool` and `raydium_amm_v4`'s own Tier 1 REST fetch costs are untouched (~99-102s and ~117-126s respectively, together still the single largest chunk of every cycle); and fixing the QuickNode batch-limit issue above had a previously-invisible side effect on Tier 2's inline poll cost — before that fix, the RPC calls to `orca_whirlpool`/`raydium_clmm` failed near-instantly (~40ms, all 413s), so `tier2_poller.py`'s `poll_done` averaged ~32s/call (dominated by Meteora's own REST `get_pools` loop); post-fix, those RPC calls now do real work (~25 sequential 4-address `getMultipleAccounts` chunks per venue, rate-limited), so `poll_done` now averages ~52-57s/call at the same ~3 invocations/cycle — a good tradeoff (the RPC path is now actually producing real `active_liquidity` data instead of silently no-op'ing) but a real, newly-visible addition to the cycle-time budget that the original dry-run's "~330s other components" estimate didn't and couldn't account for, since it was measured while the RPC path was still 100% broken.

```bash
make discovery-build              # venv + ClickHouse (docker compose) + migrations
make discovery-test-<source>      # one-shot snapshot for a single provider (e.g. discovery-test-orca, discovery-test-binance) -- see Makefile for the full list (4 DEX + 5 CEX)
make discovery-test-entity-detection  # one-shot detection pass over whatever's currently in the snapshot tables
make discovery-start              # launches the Tier 1 scheduler in its own tmux session ('zora-discovery', separate from ZORA's 'zora' session)
make discovery-stop               # stops it
make discovery-status             # PID check
make discovery-attach             # re-attach to the tmux session
```

Key files: `discovery/src/providers/base.py` (DEX provider Protocols: `PoolMetadataProvider`/`PoolStateProvider`/`PoolVolumeProvider`), `discovery/src/providers/cex_base.py` (CEX `SymbolProvider` Protocol), `discovery/src/providers/{orca_rest,raydium_amm_v4,raydium_clmm,meteora_dlmm}.py` (DEX REST adapters; the two Raydium ones share `raydium_common.py`), `discovery/src/providers/solana_rpc_state.py` (Tier 2 RPC `PoolStateProvider` for Orca Whirlpool/Raydium CLMM — batched `getMultipleAccounts`, byte layouts ported 1:1 from `ingestor-dex-solana/src/{clmm,orca,sqrt_price}.rs`), `discovery/src/providers/{binance,bybit,okx,bitget,gateio}_symbols.py` (CEX adapters), `discovery/rate_limiter.py` (generic `TokenBucket`, used by all 9 REST sources — 5 CEX since Phase 1c, 4 DEX since Phase 1f — plus the two Tier 2 RPC providers since Phase 1h; each provider owns its own bucket instance, sized per section 5.1/5.4), `discovery/http_client.py` (shared User-Agent + retry/backoff — venue APIs behind Cloudflare, like Orca's, 403 requests without a browser-like User-Agent), `discovery/src/detection.py` (`detect_new_entities()` — the `known_entities`/`new_entity_events` check-then-insert registry pattern, section 3.6), `discovery/src/scheduler.py` (Tier 1 continuous 5-min-cycle scheduler, section 3.2 — sequential across all 9 sources, per-source failure isolation, runs detection against each cycle's own fresh rows), `discovery/src/tier2_poller.py` (Tier 2's 1-min re-poll — RPC for Orca/Raydium CLMM, REST for AMM v4/Meteora, metadata/volume carried forward from ClickHouse rather than refetched on the RPC path), `discovery/src/run_*_snapshot.py` / `run_entity_detection.py` / `run_rpc_state_crosscheck.py` (one-shot manual-run entrypoints, one per provider plus detection plus the Phase 1h RPC/REST/Rust three-way cross-check).

**Provider convention** (design doc section 3.3): a REST provider implements all three DEX provider Protocols (`PoolMetadataProvider`, `PoolStateProvider`, `PoolVolumeProvider`), since a venue's own REST endpoint typically returns metadata, price, and volume together in one call. An RPC provider (e.g. a future `getMultipleAccounts`-based one) implements `PoolStateProvider` only — raw on-chain account state gives price/active_liquidity but not the off-chain-aggregated `volume_24h_usd`. CEX providers implement a single `SymbolProvider` Protocol (`cex_base.py`) instead of the three-way DEX split — an exchange's `exchangeInfo`-shaped endpoint returns symbol/base/quote/status together, with no analogue to the DEX side's differing update cadences.

**ClickHouse HTTP field-size gotcha**: ClickHouse's HTTP interface enforces a per-form-field byte-size cap on parameterized-query bind values (confirmed empirically against a live 24.8 server: an `Array(String)` bind of ~1700 identifiers, ~77KB, succeeded in one query; ~4000 identifiers, ~178KB, failed with `HTML Form Exception: Field value too long` — consistent with Poco's ~128KiB default field-value limit). `clickhouse-connect`'s automatic GET→POST-form fallback for large bind params doesn't route around this, since the cap is enforced server-side on the field itself. `detect_new_entities()` in `detection.py` chunks its existence-check `IN`-list at `EXISTENCE_CHECK_CHUNK_SIZE` (1000 identifiers) to stay under it — keep this in mind before passing any large `Array(...)`-typed bind parameter to ClickHouse elsewhere in this subsystem.

## Environment

- `.env` (gitignored) holds `SOLANA_RPC_URL` — contains an embedded API key, must never be committed
- `data/*.csv` is gitignored (paper trade output)
- `scripts/zora.sh` sources `.env` before launching all processes

## Conventions

- Structured logging: `HH:MM:SS.mmm  LEVEL  SRC  key=value key=value...` — each component has its own logger (not a shared crate)
- No `unwrap()`/`expect()` in production paths (Rust side) — `?` at startup is fine, the poll loops use explicit error handling
- The `BookTickerHandler` in C++ has `kMaxRoutes = 8` — room for 2 more pairs before that needs bumping
