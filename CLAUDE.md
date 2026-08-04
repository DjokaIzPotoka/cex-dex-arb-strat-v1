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

A separate, broad-coverage MEV opportunity discovery pipeline (Python + ClickHouse) — not part of the CEX-DEX paper trading pipeline described above. Full design: `docs/mev-discovery-plan.md`. Phase 1 (all 4 DEX venues + all 5 CEX exchanges, `known_entities`/`new_entity_events` detection, the Tier 1 scheduler, and rate limiting on all 9 sources) is complete as of Phase 1f; Tier 2 promotion/demotion and the RPC state provider are Phase 1g/1h, not yet built. Scheduler execution is still sequential (not concurrent) — Phase 1e's reason for that (DEX providers had no rate limiting) is gone as of Phase 1f, but switching to concurrent execution is a deliberately deferred decision needing its own testing, not done yet (see `scheduler.py`'s TODO).

```bash
make discovery-build              # venv + ClickHouse (docker compose) + migrations
make discovery-test-<source>      # one-shot snapshot for a single provider (e.g. discovery-test-orca, discovery-test-binance) -- see Makefile for the full list (4 DEX + 5 CEX)
make discovery-test-entity-detection  # one-shot detection pass over whatever's currently in the snapshot tables
make discovery-start              # launches the Tier 1 scheduler in its own tmux session ('zora-discovery', separate from ZORA's 'zora' session)
make discovery-stop               # stops it
make discovery-status             # PID check
make discovery-attach             # re-attach to the tmux session
```

Key files: `discovery/src/providers/base.py` (DEX provider Protocols: `PoolMetadataProvider`/`PoolStateProvider`/`PoolVolumeProvider`), `discovery/src/providers/cex_base.py` (CEX `SymbolProvider` Protocol), `discovery/src/providers/{orca_rest,raydium_amm_v4,raydium_clmm,meteora_dlmm}.py` (DEX adapters; the two Raydium ones share `raydium_common.py`), `discovery/src/providers/{binance,bybit,okx,bitget,gateio}_symbols.py` (CEX adapters), `discovery/rate_limiter.py` (generic `TokenBucket`, used by all 9 providers — 5 CEX since Phase 1c, 4 DEX since Phase 1f; each provider owns its own bucket instance, sized per section 5.1/5.4, constructed fresh per scheduler cycle), `discovery/http_client.py` (shared User-Agent + retry/backoff — venue APIs behind Cloudflare, like Orca's, 403 requests without a browser-like User-Agent), `discovery/src/detection.py` (`detect_new_entities()` — the `known_entities`/`new_entity_events` check-then-insert registry pattern, section 3.6), `discovery/src/scheduler.py` (Tier 1 continuous 5-min-cycle scheduler, section 3.2 — sequential across all 9 sources, per-source failure isolation, runs detection against each cycle's own fresh rows), `discovery/src/run_*_snapshot.py` / `run_entity_detection.py` (one-shot manual-run entrypoints, one per provider plus detection).

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
