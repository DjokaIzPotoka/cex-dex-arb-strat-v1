//! DEX ingestor: polls Raydium and Orca pools on Solana and publishes
//! reserve/price data to shared memory for the spread detector to
//! consume. The Standard-AMM pool (RAY/USDC) reports reserves via
//! vault balances; CLMM/Whirlpool pools (SOL/USDC, BONK/USDC,
//! TRX/USDC, ZEC/USDC, KMNO/USDC) report price only.
//!
//! `#[tokio::main]` below expands this `async fn main` into a regular
//! `fn main` that spins up a tokio runtime and blocks on this function
//! -- that's what lets us use `.await` here at all; without it, `.await`
//! wouldn't compile outside an `async` context.
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use tokio::signal::unix::{signal, SignalKind};

mod clmm;
mod logger;
mod orca;
mod pool;
mod rpc;
mod shm;
mod sqrt_price;
mod types;

use logger::Level;
use shm::ShmWriter;
use solana_sdk::commitment_config::CommitmentConfig;

const POLL_INTERVAL: Duration = Duration::from_millis(400);
const RPC_WARN_THRESHOLD: Duration = Duration::from_millis(300);
// Bounds how long a stuck or heavily-rate-limited RPC call can stall the
// loop: without this, a hung request blocks not just that poll tick but
// also shutdown (Ctrl+C/SIGTERM are only checked between ticks, never
// while a fetch is in flight).
const RPC_TIMEOUT: Duration = Duration::from_secs(2);

// Raydium CLMM program id -- shared by every Raydium CLMM pool below
// (SOL/USDC, BONK/USDC, TRX/USDC), since it identifies the program,
// not any one pool.
const RAYDIUM_CLMM_PROGRAM_ID: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";

// SOL/USDC's Raydium CLMM pool (0.04% fee tier, ~$6M TVL at time of
// writing) -- replaces the old Standard AMM v4 vault-based pool
// entirely (not added alongside it) since this is the deeper, cheaper
// venue for the pair. Confirmed live via getAccountInfo:
// token_mint_0 = So11111111111111111111111111111111111111112 (WSOL,
// 9 decimals), token_mint_1 = EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
// (USDC, 6 decimals) -- same order as Raydium's API reports (mintA/
// mintB), but checked on-chain rather than assumed, same as BONK/TRX
// below.
const SOL_CLMM_POOL_ID: &str = "3ucNos4NbumPLZNWztqGHNFFgkHeRMBQAVemeeomsUxv";
const SOL_CLMM_DECIMALS_0: i32 = 9; // SOL, token_mint_0 (= base)
const SOL_CLMM_DECIMALS_1: i32 = 6; // USDC, token_mint_1

// BONK/USDC's CLMM pool -- most of BONK's Raydium liquidity now sits
// here (~$122k TVL at time of writing) rather than in a Standard AMM
// v4 pool, so this replaces (not adds to) BONK/USDC's old vault-based
// PoolConfig entry. See clmm.rs for how the byte offsets below were
// sourced and verified, and its doc comment for why BONK is mint_0
// and USDC is mint_1 specifically for this pool -- confirmed live,
// not assumed from token ordering by symbol.
const BONK_CLMM_POOL_ID: &str = "3UwfrdLTpAjxTRni1boc5HUWe6hzc4HgE5yLdvEp2Noc";
const BONK_CLMM_DECIMALS_0: i32 = 5; // BONK, token_mint_0 (= base)
const BONK_CLMM_DECIMALS_1: i32 = 6; // USDC, token_mint_1

// TRX/USDC's Raydium CLMM pool. Unlike BONK's pool, USDC is mint_0
// here and TRX is mint_1 -- token_mint_0/token_mint_1 order is decided
// on-chain by raw pubkey comparison, not by which token is
// economically the base, so this had to be checked live rather than
// assumed from the BONK pool's ordering (`base_is_mint_0` on
// `ClmmPoolConfig` below is what encodes this per pool). Confirmed
// against pool HpgV2jnzgrGfrZjeZGkHgTnEgRFAhtzCVuk3BRFTFJwk via
// getAccountInfo:
// token_mint_0 = EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v (USDC),
// token_mint_1 = GbbesPbaYh5uiAZSYNXTc7w9jty1rpg3P9L4JeN4LkKc (TRX),
// both 6 decimals; resulting price matched Dexscreener's live
// USDC/TRX quote within ordinary drift.
const TRX_CLMM_POOL_ID: &str = "HpgV2jnzgrGfrZjeZGkHgTnEgRFAhtzCVuk3BRFTFJwk";
const TRX_CLMM_DECIMALS_0: i32 = 6; // USDC, token_mint_0
const TRX_CLMM_DECIMALS_1: i32 = 6; // TRX, token_mint_1 (= base)

// Orca Whirlpool program id.
const ORCA_WHIRLPOOL_PROGRAM_ID: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";

// ZEC/USDC's Orca Whirlpool -- replaces the old Raydium CPMM pool
// entirely (2% fee, ~$26k TVL, essentially no volume) with a much
// deeper, cheaper venue (0.16% fee, ~$3.6M TVL, ~$3.96M/day volume at
// time of writing). Confirmed live via getAccountInfo: discriminator
// matches Whirlpool, owner is Orca's program, token_mint_a =
// A7bdiYdS5GjqGFtxf17ppRHtDKPkkRqbKtR27dxvQXaS (ZEC), token_mint_b =
// EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v (USDC) -- same order
// as KMNO's pool below, but checked independently rather than assumed.
const ZEC_ORCA_POOL_ID: &str = "GTHKH8s82ZR8GTSFZ1dUu6wfdxhy59wpMShxzG5zjiPm";
const ZEC_ORCA_DECIMALS_0: i32 = 8; // ZEC, token_mint_a (= base)
const ZEC_ORCA_DECIMALS_1: i32 = 6; // USDC, token_mint_b

// KMNO/USDC's Orca Whirlpool. KMNO is mint_a (mint_0-equivalent),
// USDC is mint_b -- confirmed live against pool
// 3ndjN1nJVUKGrJBc1hhVpER6kWTZKHdyDrPyCJyX3CXK via getAccountInfo, same
// way as the Raydium pools above; resulting price matched
// Dexscreener's live USDC/KMNO quote within ordinary drift.
const KMNO_ORCA_POOL_ID: &str = "3ndjN1nJVUKGrJBc1hhVpER6kWTZKHdyDrPyCJyX3CXK";
const KMNO_ORCA_DECIMALS_0: i32 = 6; // KMNO, token_mint_a (= base)
const KMNO_ORCA_DECIMALS_1: i32 = 6; // USDC, token_mint_b

/// A Raydium CLMM pool this ingestor polls for price only (no
/// reserves/sizing data -- see the module-level comment on
/// `PoolConfig` for the Standard-AMM equivalent). Owner-checked
/// against `program_id` on every fetch: a wrong or migrated pool
/// pubkey would otherwise misparse an unrelated account's bytes as
/// `PoolState` fields silently, which is exactly the failure mode
/// this whole feature was built to avoid.
struct ClmmPoolConfig {
    pair_label: &'static str,
    pool_pubkey: Pubkey,
    program_id: Pubkey,
    decimals_0: i32,
    decimals_1: i32,
    // Whether the base token (the one `price` is quoted per unit of)
    // is mint_0 or mint_1 -- on-chain mint order is decided by raw
    // pubkey comparison, not by which token is economically the base,
    // so this must be set per pool rather than assumed; see
    // `sqrt_price::price_in_quote_per_base`'s doc comment.
    base_is_mint_0: bool,
    shm_writer: ShmWriter,
    connected: bool,
    retry_count: u32,
}

/// An Orca Whirlpool this ingestor polls for price only. Same shape
/// and reasoning as `ClmmPoolConfig`, just for `orca::parse_whirlpool_account`
/// instead of `clmm::parse_clmm_account` -- kept as a separate struct
/// rather than unified with `ClmmPoolConfig` since the two parse
/// different account layouts from different programs, even though the
/// resulting price math (`sqrt_price::price_in_quote_per_base`) is shared.
struct OrcaPoolConfig {
    pair_label: &'static str,
    pool_pubkey: Pubkey,
    program_id: Pubkey,
    decimals_0: i32,
    decimals_1: i32,
    base_is_mint_0: bool,
    shm_writer: ShmWriter,
    connected: bool,
    retry_count: u32,
}

/// Fetches and publishes the CLMM pool's current price via a single
/// getAccountInfo call. Run concurrently with `process_all_pools_tick`
/// (see the `tokio::join!` in `main`), not sequentially after it --
/// CLMM needs a different RPC call shape (one pool-state account vs.
/// two token vaults per pool), so it can't share the existing batch,
/// but there's no reason to pay its network latency serially on top of
/// the batch's within the same 400ms poll budget.
async fn process_clmm_pool_tick(client: &RpcClient, pool: &mut ClmmPoolConfig) {
    let network_start = Instant::now();
    let fetch = tokio::time::timeout(
        RPC_TIMEOUT,
        client.get_account_with_commitment(&pool.pool_pubkey, CommitmentConfig::confirmed()),
    )
    .await;
    let network_elapsed = network_start.elapsed();

    let outcome: Result<(clmm::ClmmPoolState, u64)> = match fetch {
        Ok(Ok(resp)) => resp
            .value
            .context("CLMM pool account not found")
            .and_then(|account| {
                if account.owner != pool.program_id {
                    anyhow::bail!(
                        "CLMM pool account owner {} doesn't match expected program {} -- wrong pubkey or the pool migrated",
                        account.owner,
                        pool.program_id
                    );
                }
                clmm::parse_clmm_account(&account.data)
            })
            .map(|state| (state, resp.context.slot)),
        Ok(Err(e)) => Err(e).context("getAccountInfo RPC call failed"),
        Err(_elapsed) => Err(anyhow::anyhow!("timeout")),
    };

    match outcome {
        Ok((state, slot)) => {
            pool.retry_count = 0;
            if !pool.connected {
                pool.connected = true;
                logger::log(
                    Level::Info,
                    &[
                        ("pair", pool.pair_label),
                        ("rpc", "connected"),
                        ("status", "CONNECTED"),
                    ],
                );
            }

            let parse_start = Instant::now();
            let price = sqrt_price::price_in_quote_per_base(
                state.sqrt_price_x64,
                pool.decimals_0,
                pool.decimals_1,
                pool.base_is_mint_0,
            );
            let parse_elapsed = parse_start.elapsed();

            let shm_start = Instant::now();
            // reserve_sol/reserve_usdc are 0: CLMM has no constant-
            // product reserves to report, unlike the Standard-AMM
            // pools -- see spread-detector/src/main.rs's zero-reserve
            // guard before sizing.rs, which is what makes this safe to
            // publish as-is rather than needing a sentinel elsewhere.
            pool.shm_writer.write(0, 0, price, now_micros(), slot);
            let shm_elapsed = shm_start.elapsed();
            let code_elapsed = parse_elapsed + shm_elapsed;

            logger::log(
                Level::Info,
                &[
                    ("pair", pool.pair_label),
                    ("price", &format!("{:.8}", price)),
                    ("tick", &state.tick_current.to_string()),
                    ("liquidity", &state.liquidity.to_string()),
                    ("slot", &slot.to_string()),
                    ("net", &format!("{}ms", network_elapsed.as_millis())),
                    ("code", &format!("{}us", code_elapsed.as_micros())),
                ],
            );

            if network_elapsed > RPC_WARN_THRESHOLD {
                logger::log(
                    Level::Warn,
                    &[
                        ("pair", pool.pair_label),
                        ("rpc", "slow"),
                        ("latency", &format!("{}ms", network_elapsed.as_millis())),
                        ("threshold", &format!("{}ms", RPC_WARN_THRESHOLD.as_millis())),
                    ],
                );
            }
        }
        Err(e) => {
            pool.retry_count += 1;
            logger::log(
                Level::Error,
                &[
                    ("pair", pool.pair_label),
                    ("rpc", "failed"),
                    ("reason", &format!("{:#}", e)),
                    ("retry", &pool.retry_count.to_string()),
                ],
            );
            if network_elapsed > RPC_WARN_THRESHOLD {
                logger::log(
                    Level::Warn,
                    &[
                        ("pair", pool.pair_label),
                        ("rpc", "slow"),
                        ("latency", &format!("{}ms", network_elapsed.as_millis())),
                        ("threshold", &format!("{}ms", RPC_WARN_THRESHOLD.as_millis())),
                    ],
                );
            }
        }
    }
}

/// Fetches and publishes an Orca Whirlpool's current price via a
/// single getAccountInfo call. Structurally identical to
/// `process_clmm_pool_tick` -- same RPC call shape (one pool-state
/// account), same owner check, same zero-reserve SHM write -- just
/// against `orca::parse_whirlpool_account` instead of
/// `clmm::parse_clmm_account`. Not unified into one generic function
/// since the two account types (and their modules) are otherwise
/// unrelated; duplicating this one small function was judged cheaper
/// than a trait/generic abstraction for two call sites.
async fn process_orca_pool_tick(client: &RpcClient, pool: &mut OrcaPoolConfig) {
    let network_start = Instant::now();
    let fetch = tokio::time::timeout(
        RPC_TIMEOUT,
        client.get_account_with_commitment(&pool.pool_pubkey, CommitmentConfig::confirmed()),
    )
    .await;
    let network_elapsed = network_start.elapsed();

    let outcome: Result<(orca::WhirlpoolState, u64)> = match fetch {
        Ok(Ok(resp)) => resp
            .value
            .context("Whirlpool account not found")
            .and_then(|account| {
                if account.owner != pool.program_id {
                    anyhow::bail!(
                        "Whirlpool account owner {} doesn't match expected program {} -- wrong pubkey or the pool migrated",
                        account.owner,
                        pool.program_id
                    );
                }
                orca::parse_whirlpool_account(&account.data)
            })
            .map(|state| (state, resp.context.slot)),
        Ok(Err(e)) => Err(e).context("getAccountInfo RPC call failed"),
        Err(_elapsed) => Err(anyhow::anyhow!("timeout")),
    };

    match outcome {
        Ok((state, slot)) => {
            pool.retry_count = 0;
            if !pool.connected {
                pool.connected = true;
                logger::log(
                    Level::Info,
                    &[
                        ("pair", pool.pair_label),
                        ("rpc", "connected"),
                        ("status", "CONNECTED"),
                    ],
                );
            }

            let parse_start = Instant::now();
            let price = sqrt_price::price_in_quote_per_base(
                state.sqrt_price,
                pool.decimals_0,
                pool.decimals_1,
                pool.base_is_mint_0,
            );
            let parse_elapsed = parse_start.elapsed();

            let shm_start = Instant::now();
            pool.shm_writer.write(0, 0, price, now_micros(), slot);
            let shm_elapsed = shm_start.elapsed();
            let code_elapsed = parse_elapsed + shm_elapsed;

            logger::log(
                Level::Info,
                &[
                    ("pair", pool.pair_label),
                    ("price", &format!("{:.8}", price)),
                    ("tick", &state.tick_current_index.to_string()),
                    ("liquidity", &state.liquidity.to_string()),
                    ("slot", &slot.to_string()),
                    ("net", &format!("{}ms", network_elapsed.as_millis())),
                    ("code", &format!("{}us", code_elapsed.as_micros())),
                ],
            );

            if network_elapsed > RPC_WARN_THRESHOLD {
                logger::log(
                    Level::Warn,
                    &[
                        ("pair", pool.pair_label),
                        ("rpc", "slow"),
                        ("latency", &format!("{}ms", network_elapsed.as_millis())),
                        ("threshold", &format!("{}ms", RPC_WARN_THRESHOLD.as_millis())),
                    ],
                );
            }
        }
        Err(e) => {
            pool.retry_count += 1;
            logger::log(
                Level::Error,
                &[
                    ("pair", pool.pair_label),
                    ("rpc", "failed"),
                    ("reason", &format!("{:#}", e)),
                    ("retry", &pool.retry_count.to_string()),
                ],
            );
            if network_elapsed > RPC_WARN_THRESHOLD {
                logger::log(
                    Level::Warn,
                    &[
                        ("pair", pool.pair_label),
                        ("rpc", "slow"),
                        ("latency", &format!("{}ms", network_elapsed.as_millis())),
                        ("threshold", &format!("{}ms", RPC_WARN_THRESHOLD.as_millis())),
                    ],
                );
            }
        }
    }
}

/// One Standard-AMM pool this ingestor polls. Only RAY/USDC remains
/// here now -- SOL/USDC, ZEC/USDC, and UNI/USDC all used to be too
/// (see `sol_clmm`/`zec_orca` in `main`, and UNI/USDC's removal note
/// where its old entry used to sit). `base_vault`/`quote_vault` are
/// still named generically (not "ray_vault") in case another
/// Standard-AMM pair joins later -- the RPC/parsing code (rpc.rs) is
/// unchanged either way.
///
/// Note: `ShmWriter::write`'s parameter names (and the underlying
/// `ZoraDexPrice` struct's fields, shared byte-for-byte with the
/// spread detector) are still `reserve_sol`/`reserve_usdc` -- for
/// RAY/USDC, `reserve_sol` actually holds RAY's reserve. Renaming
/// those would ripple into the cross-process struct definition
/// (duplicated independently in the spread detector too) for a
/// cosmetic fix only, so they're left as-is; this comment is the
/// documentation of that wart.
struct PoolConfig {
    pair_label: &'static str,
    pool_id: &'static str,
    base_vault: Pubkey,
    quote_vault: Pubkey,
    // The base token's mint decimals (9 for SOL, 6 for RAY) -- passing
    // this through to calculate_price is what avoids the 1000x price
    // bug this exact refactor originally shipped with (see pool.rs).
    base_decimals: u32,
    shm_writer: ShmWriter,
    connected: bool,
    retry_count: u32,
}

/// Microseconds since the Unix epoch. `SystemTime::now()` returning a
/// time before `UNIX_EPOCH` would mean the OS clock itself is broken;
/// falling back to 0 keeps this out of the "never panic" poll loop
/// without pretending a broken clock is a recoverable-per-tick error.
fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// Logs and publishes to SHM one pool's successful fetch. Pulled out
/// of `process_all_pools_tick` purely to keep that function's
/// per-pool loop body readable -- behavior/output is identical to
/// what the old single-pool `process_pool_tick` did on success.
fn handle_pool_success(
    pool: &mut PoolConfig,
    state: pool::DexPoolState,
    network_elapsed: Duration,
    parse_elapsed: Duration,
) {
    let shm_start = Instant::now();
    // Sequence itself lives in SHM for the spread detector to read,
    // not in the log line -- the DEX log format doesn't carry a seq=
    // field the way CEX's does.
    let _seq = pool.shm_writer.write(
        state.reserve_sol,
        state.reserve_usdc,
        state.price,
        now_micros(),
        state.slot,
    );
    let shm_elapsed = shm_start.elapsed();
    // Mirrors the CEX ingestor's `age`: starts at the same conceptual
    // point (response bytes in hand, before parsing) and ends at the
    // same point (published to SHM). `parse_elapsed` was measured
    // back in rpc.rs, immediately after the network `.await`
    // returned -- adding it here rather than timing from `shm_start`
    // is what makes this number comparable to the CEX side's, instead
    // of only capturing the SHM write.
    let code_elapsed = parse_elapsed + shm_elapsed;
    pool.retry_count = 0;

    if !pool.connected {
        pool.connected = true;
        logger::log(
            Level::Info,
            &[
                ("pair", pool.pair_label),
                ("rpc", "connected"),
                ("status", "CONNECTED"),
            ],
        );
    }

    // One line per tick: pair/price/reserves/slot plus timing. `net`
    // (not `network`) and milliseconds (not micros) to match the
    // established format -- network round trips are the slow,
    // millisecond-scale part here, unlike `code`, which stays in
    // microseconds since it's fast enough that milliseconds would
    // round it to 0.
    logger::log(
        Level::Info,
        &[
            ("pair", pool.pair_label),
            // 8 decimals, not 3: BONK/USDC's price (~0.0000031) needs
            // it -- 3 decimals would print "0.000" for every BONK
            // line. Harmless extra trailing digits for the
            // larger-priced pairs.
            ("price", &format!("{:.8}", state.price)),
            ("sol", &format!("{:.1}", state.reserve_sol as f64 / 1_000_000_000.0)),
            ("usdc", &format!("{:.1}", state.reserve_usdc as f64 / 1_000_000.0)),
            ("slot", &state.slot.to_string()),
            ("net", &format!("{}ms", network_elapsed.as_millis())),
            ("code", &format!("{}us", code_elapsed.as_micros())),
        ],
    );

    if network_elapsed > RPC_WARN_THRESHOLD {
        logger::log(
            Level::Warn,
            &[
                ("pair", pool.pair_label),
                ("rpc", "slow"),
                ("latency", &format!("{}ms", network_elapsed.as_millis())),
                ("threshold", &format!("{}ms", RPC_WARN_THRESHOLD.as_millis())),
            ],
        );
    }
}

/// Logs one pool's fetch failure (parse-level, after a successful
/// network round trip, or a batch-wide network/timeout failure
/// reported once per pool) and increments its retry counter. Same
/// message shape the old single-pool `process_pool_tick` used.
fn handle_pool_error(pool: &mut PoolConfig, reason: &str, elapsed: Duration) {
    pool.retry_count += 1;
    logger::log(
        Level::Error,
        &[
            ("pair", pool.pair_label),
            ("rpc", "failed"),
            ("reason", reason),
            ("retry", &pool.retry_count.to_string()),
        ],
    );
    if elapsed > RPC_WARN_THRESHOLD {
        logger::log(
            Level::Warn,
            &[
                ("pair", pool.pair_label),
                ("rpc", "slow"),
                ("latency", &format!("{}ms", elapsed.as_millis())),
                ("threshold", &format!("{}ms", RPC_WARN_THRESHOLD.as_millis())),
            ],
        );
    }
}

/// Fetches every pool in one batched RPC call (see rpc.rs), then
/// publishes/logs each pool's result exactly as the old per-pool fetch
/// did. Returns how long the whole round took, so the caller can size
/// the sleep before the next one.
///
/// `pool_vaults` is precomputed once at startup (see `main`) rather
/// than rebuilt from `pools` on every tick, since the vault pubkeys
/// never change -- `pools` and `pool_vaults` are always the same
/// length and in the same order, which is what makes the `zip` below
/// (and the plain index-order match against rpc.rs's returned `Vec`)
/// correct.
async fn process_all_pools_tick(
    client: &RpcClient,
    pool_vaults: &[rpc::PoolVaults],
    pools: &mut [PoolConfig],
) -> Duration {
    let start = Instant::now();

    // `tokio::time::timeout` races the fetch against a 2s clock and
    // hands back `Err(Elapsed)` if the clock wins -- the fetch future
    // itself is dropped at that point, which cancels the in-flight
    // HTTP request rather than leaving it running in the background.
    // This is what actually bounds shutdown latency: without it, a
    // stuck RPC call (rate-limited, dead connection, etc.) blocks
    // Ctrl+C/SIGTERM until the call resolves on its own, which could
    // be arbitrarily long. Now that one call covers every pool, a
    // timeout here means every pool in the batch times out together --
    // unavoidable once they share one physical request, and handled
    // below the same way each pool's own timeout used to be.
    match tokio::time::timeout(RPC_TIMEOUT, rpc::fetch_all_pool_states(client, pool_vaults)).await {
        Ok(Ok((results, network_elapsed, parse_elapsed))) => {
            for (pool, result) in pools.iter_mut().zip(results) {
                match result {
                    // Successful network round trip, and this specific
                    // pool's accounts parsed cleanly.
                    Ok(state) => handle_pool_success(pool, state, network_elapsed, parse_elapsed),
                    // Successful network round trip, but this specific
                    // pool's accounts didn't parse (e.g. missing
                    // account) -- isolated to this pool only, exactly
                    // like the old per-pool call's error path was.
                    Err(e) => handle_pool_error(pool, &format!("{:#}", e), network_elapsed),
                }
            }
        }
        // The RPC call completed (didn't time out) but returned an
        // error -- e.g. the connection was refused outright. Never
        // panics: this just skips the tick rather than killing the
        // process. Since it's one physical request covering every
        // pool, every pool is reported as failed together; `{:#}`
        // carries anyhow's full context chain (e.g. "get_multiple_
        // accounts RPC call failed: <underlying error>"), not just the
        // outer message.
        Ok(Err(e)) => {
            let elapsed = start.elapsed();
            let reason = format!("{:#}", e);
            for pool in pools.iter_mut() {
                handle_pool_error(pool, &reason, elapsed);
            }
        }
        // The timeout won the race: the fetch was still pending after
        // RPC_TIMEOUT, so tokio::time::timeout dropped it (cancelling
        // the in-flight request) and returned Elapsed. Every pool in
        // the batch is reported as timed out together, same reasoning
        // as the Ok(Err(e)) arm above.
        Err(_elapsed) => {
            let elapsed = start.elapsed();
            for pool in pools.iter_mut() {
                handle_pool_error(pool, "timeout", elapsed);
            }
        }
    }

    start.elapsed()
}

#[tokio::main]
async fn main() -> Result<()> {
    logger::print_banner(
        "ZORA DEX INGESTOR",
        "Raydium RAY/USDC (CLMM: SOL/USDC + BONK/USDC + TRX/USDC), Orca ZEC/USDC + KMNO/USDC",
    );

    // The `?` operator here unwraps `Ok` or returns early with `Err`
    // wrapped in the surrounding function's `Result` -- since `main`
    // returns `Result<()>`, an `Err` bubbling all the way out gets
    // printed and turned into a non-zero exit code automatically. This
    // is fine even though the hard constraint is "no unwrap/expect in
    // production paths, only in tests/main setup": `?` isn't `.unwrap()`
    // (it doesn't panic), and startup failures like a malformed pubkey
    // or a shared memory setup failure genuinely should abort the
    // process rather than continue in a broken state.
    let client = rpc::build_client();

    let mut pools = vec![
        // SOL/USDC used to be here as a Standard AMM v4 pool. It's now
        // read via the CLMM pool instead (see `sol_clmm` below,
        // constructed separately since it needs a different RPC call
        // shape) -- that pool is deeper and charges 0.04% vs. this
        // legacy pool's 0.25%.
        PoolConfig {
            pair_label: "RAY/USDC",
            pool_id: "6UmmUiYoBjSrhakAobJw8BvkmJtDVxaeBtbt7rxWo1mg",
            base_vault: Pubkey::from_str("FdmKUE4UMiJYFK5ogCngHzShuVKrFXBamPWcewDr31th")
                .context("invalid RAY vault pubkey")?,
            quote_vault: Pubkey::from_str("Eqrhxd7bDUCH3MepKmdVkgwazXRzY6iHhEoBpY7yAohk")
                .context("invalid USDC vault pubkey (RAY/USDC pool)")?,
            base_decimals: 6,
            shm_writer: ShmWriter::new(types::ZORA_SHM_DEX_RAY_NAME)
                .context("failed to create RAY/USDC shared memory segment")?,
            connected: false,
            retry_count: 0,
        },
        // BONK/USDC used to be here as a Standard AMM v4 pool. It's now
        // read via the CLMM pool instead (see `bonk_clmm` below,
        // constructed separately since it needs a different RPC call
        // shape) -- most of BONK's Raydium liquidity sits in the CLMM
        // pool, not this legacy one.
        //
        // ZEC/USDC used to be here as a Raydium CPMM pool (2% fee,
        // ~$26k TVL). It's now read via the Orca Whirlpool instead (see
        // `zec_orca` below) -- that pool has ~$3.6M TVL and a 0.16%
        // fee, a much deeper and cheaper venue for the pair.
        //
        // UNI/USDC used to be here too. Removed outright (not moved):
        // every UNI/USDC pool checked across Raydium and Orca besides
        // this one has single-digit-dollar TVL and no volume -- there's
        // no viable venue for this pair at all.
    ];

    let raydium_clmm_program_id =
        Pubkey::from_str(RAYDIUM_CLMM_PROGRAM_ID).context("invalid Raydium CLMM program pubkey")?;

    let mut sol_clmm = ClmmPoolConfig {
        pair_label: "SOL/USDC",
        pool_pubkey: Pubkey::from_str(SOL_CLMM_POOL_ID)
            .context("invalid SOL/USDC CLMM pool pubkey")?,
        program_id: raydium_clmm_program_id,
        decimals_0: SOL_CLMM_DECIMALS_0,
        decimals_1: SOL_CLMM_DECIMALS_1,
        base_is_mint_0: true, // SOL is token_mint_0
        shm_writer: ShmWriter::new(types::ZORA_SHM_DEX_NAME)
            .context("failed to create SOL/USDC shared memory segment")?,
        connected: false,
        retry_count: 0,
    };

    let mut bonk_clmm = ClmmPoolConfig {
        pair_label: "BONK/USDC",
        pool_pubkey: Pubkey::from_str(BONK_CLMM_POOL_ID)
            .context("invalid BONK/USDC CLMM pool pubkey")?,
        program_id: raydium_clmm_program_id,
        decimals_0: BONK_CLMM_DECIMALS_0,
        decimals_1: BONK_CLMM_DECIMALS_1,
        base_is_mint_0: true, // BONK is token_mint_0
        shm_writer: ShmWriter::new(types::ZORA_SHM_DEX_BONK_NAME)
            .context("failed to create BONK/USDC shared memory segment")?,
        connected: false,
        retry_count: 0,
    };

    let mut trx_clmm = ClmmPoolConfig {
        pair_label: "TRX/USDC",
        pool_pubkey: Pubkey::from_str(TRX_CLMM_POOL_ID)
            .context("invalid TRX/USDC CLMM pool pubkey")?,
        program_id: raydium_clmm_program_id,
        decimals_0: TRX_CLMM_DECIMALS_0,
        decimals_1: TRX_CLMM_DECIMALS_1,
        base_is_mint_0: false, // TRX is token_mint_1, not token_mint_0
        shm_writer: ShmWriter::new(types::ZORA_SHM_DEX_TRX_NAME)
            .context("failed to create TRX/USDC shared memory segment")?,
        connected: false,
        retry_count: 0,
    };

    let orca_whirlpool_program_id =
        Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM_ID).context("invalid Orca Whirlpool program pubkey")?;

    let mut zec_orca = OrcaPoolConfig {
        pair_label: "ZEC/USDC",
        pool_pubkey: Pubkey::from_str(ZEC_ORCA_POOL_ID)
            .context("invalid ZEC/USDC Whirlpool pubkey")?,
        program_id: orca_whirlpool_program_id,
        decimals_0: ZEC_ORCA_DECIMALS_0,
        decimals_1: ZEC_ORCA_DECIMALS_1,
        base_is_mint_0: true, // ZEC is token_mint_a (mint_0-equivalent)
        shm_writer: ShmWriter::new(types::ZORA_SHM_DEX_ZEC_NAME)
            .context("failed to create ZEC/USDC shared memory segment")?,
        connected: false,
        retry_count: 0,
    };

    let mut kmno_orca = OrcaPoolConfig {
        pair_label: "KMNO/USDC",
        pool_pubkey: Pubkey::from_str(KMNO_ORCA_POOL_ID)
            .context("invalid KMNO/USDC Whirlpool pubkey")?,
        program_id: orca_whirlpool_program_id,
        decimals_0: KMNO_ORCA_DECIMALS_0,
        decimals_1: KMNO_ORCA_DECIMALS_1,
        base_is_mint_0: true, // KMNO is token_mint_a (mint_0-equivalent)
        shm_writer: ShmWriter::new(types::ZORA_SHM_DEX_KMNO_NAME)
            .context("failed to create KMNO/USDC shared memory segment")?,
        connected: false,
        retry_count: 0,
    };

    for pool in &pools {
        // Not "status=CONNECTED" yet: constructing an RpcClient doesn't
        // itself talk to the network, so we don't actually know the RPC
        // endpoint is reachable until the first fetch succeeds. The
        // banner keeps showing CONNECTING... until then.
        logger::log(
            Level::Info,
            &[
                ("pair", pool.pair_label),
                ("pool", pool.pool_id),
                ("base_vault", &pool.base_vault.to_string()),
                ("quote_vault", &pool.quote_vault.to_string()),
            ],
        );
        logger::log(
            Level::Info,
            &[("pair", pool.pair_label), ("shm", "created")],
        );
    }

    for (pair_label, pool_id) in [
        (sol_clmm.pair_label, SOL_CLMM_POOL_ID),
        (bonk_clmm.pair_label, BONK_CLMM_POOL_ID),
        (trx_clmm.pair_label, TRX_CLMM_POOL_ID),
    ] {
        logger::log(
            Level::Info,
            &[("pair", pair_label), ("pool", pool_id), ("program", "CLMM")],
        );
        logger::log(Level::Info, &[("pair", pair_label), ("shm", "created")]);
    }

    for (pair_label, pool_id) in [
        (zec_orca.pair_label, ZEC_ORCA_POOL_ID),
        (kmno_orca.pair_label, KMNO_ORCA_POOL_ID),
    ] {
        logger::log(
            Level::Info,
            &[("pair", pair_label), ("pool", pool_id), ("program", "Whirlpool")],
        );
        logger::log(Level::Info, &[("pair", pair_label), ("shm", "created")]);
    }

    // Precomputed once (the vault pubkeys never change) rather than
    // rebuilt from `pools` on every tick. Must stay the same length
    // and order as `pools` -- `process_all_pools_tick` zips the two
    // together and matches rpc.rs's returned results back to pools by
    // that same positional order.
    let pool_vaults: Vec<rpc::PoolVaults> = pools
        .iter()
        .map(|p| rpc::PoolVaults {
            base_vault: p.base_vault,
            quote_vault: p.quote_vault,
            base_decimals: p.base_decimals,
        })
        .collect();

    // SIGTERM needs explicit opt-in on Unix (there's no default
    // handler tokio installs for it).
    //
    // Both signal streams are created *once* here and reused via
    // `.recv()` for the rest of the process, rather than calling
    // `tokio::signal::ctrl_c()` fresh inside the select! below every
    // loop iteration (this used to do exactly that). With 2 pools now
    // fetched sequentially, their combined RPC latency frequently eats
    // the whole 400ms budget, so `remaining` (the sleep below) is often
    // near-zero -- meaning the sleep branch of the select! wins nearly
    // instantly almost every time, and a freshly-created `ctrl_c()`
    // listener barely exists before being dropped. A real Ctrl+C
    // arriving in one of those gaps is simply missed. Confirmed by
    // testing: two consecutive SIGINTs did nothing to a live process
    // in this exact state. A persistent listener has no such gap.
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;

    loop {
        // All Standard-AMM pools are fetched in one batched RPC call
        // (see rpc.rs's fetch_all_pool_states) -- half the request rate
        // against the public RPC endpoint, which throttles per-request.
        // The CLMM/Whirlpool pools each need a different call shape (one
        // pool-state account, not two token vaults) and can't join that
        // batch, so each is fetched via its own getAccountInfo call --
        // run concurrently with the batch and each other (not serially)
        // so none of their network latency stacks on top of the
        // others' within the same 400ms poll budget. `start.elapsed()`
        // below measures the true wall-clock cost of all four together,
        // not any one alone.
        let start = Instant::now();
        let _ = tokio::join!(
            process_all_pools_tick(&client, &pool_vaults, &mut pools),
            process_clmm_pool_tick(&client, &mut sol_clmm),
            process_clmm_pool_tick(&client, &mut bonk_clmm),
            process_clmm_pool_tick(&client, &mut trx_clmm),
            process_orca_pool_tick(&client, &mut zec_orca),
            process_orca_pool_tick(&client, &mut kmno_orca),
        );
        let total_elapsed = start.elapsed();

        // saturating_sub instead of plain `-`: if fetching all pools
        // took longer than the poll interval, a normal subtraction
        // would underflow (Duration can't go negative) and panic.
        // Saturating just clamps to zero, meaning "don't sleep, poll
        // again immediately."
        let remaining = POLL_INTERVAL.saturating_sub(total_elapsed);

        // tokio::select! races these three futures and proceeds with
        // whichever completes first, dropping the others. That's what
        // makes shutdown signals interrupt the sleep immediately
        // instead of waiting for the next poll tick to notice them.
        tokio::select! {
            _ = tokio::time::sleep(remaining) => {}
            _ = sigint.recv() => break,
            _ = sigterm.recv() => break,
        }
    }

    for pool in pools {
        pool.shm_writer.shutdown();
    }
    sol_clmm.shm_writer.shutdown();
    bonk_clmm.shm_writer.shutdown();
    trx_clmm.shm_writer.shutdown();
    zec_orca.shm_writer.shutdown();
    kmno_orca.shm_writer.shutdown();
    logger::log(Level::Info, &[("shutdown", "complete")]);
    Ok(())
}
