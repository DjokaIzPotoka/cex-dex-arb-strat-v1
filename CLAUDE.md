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

Token decimals matter: SOL=9, RAY=6, BONK=5, ZEC=8, UNI=8. Price calculation in `pool.rs` uses `base_decimals` to normalize reserves correctly.

For very low-priced pairs (like BONK at ~$0.000003), set `min_abs_price_delta` on the `PairContext` to filter quoting-noise false signals. Price display uses 8 decimal places throughout.

## Current Pair Roster

SOL/USDC, RAY/USDC, BONK/USDC, ZEC/USDC, UNI/USDC

## Environment

- `.env` (gitignored) holds `SOLANA_RPC_URL` — contains an embedded API key, must never be committed
- `data/*.csv` is gitignored (paper trade output)
- `scripts/zora.sh` sources `.env` before launching all processes

## Conventions

- Structured logging: `HH:MM:SS.mmm  LEVEL  SRC  key=value key=value...` — each component has its own logger (not a shared crate)
- No `unwrap()`/`expect()` in production paths (Rust side) — `?` at startup is fine, the poll loops use explicit error handling
- The `BookTickerHandler` in C++ has `kMaxRoutes = 8` — room for 3 more pairs before that needs bumping
