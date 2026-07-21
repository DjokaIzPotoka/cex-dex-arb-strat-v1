//! DEX ingestor: polls Raydium pools (SOL/USDC, RAY/USDC) on Solana and
//! publishes reserve/price data to shared memory for the spread
//! detector to consume.
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

mod logger;
mod pool;
mod rpc;
mod shm;
mod types;

use logger::Level;
use shm::ShmWriter;

const POLL_INTERVAL: Duration = Duration::from_millis(400);
const RPC_WARN_THRESHOLD: Duration = Duration::from_millis(300);
// Bounds how long a stuck or heavily-rate-limited RPC call can stall the
// loop: without this, a hung request blocks not just that poll tick but
// also shutdown (Ctrl+C/SIGTERM are only checked between ticks, never
// while a fetch is in flight).
const RPC_TIMEOUT: Duration = Duration::from_secs(2);

/// One pool this ingestor polls. `base_vault`/`quote_vault` are named
/// generically (not "sol_vault") since the same struct now serves both
/// SOL/USDC and RAY/USDC -- only the pubkeys differ, the RPC/parsing
/// code is identical either way (see rpc.rs, unchanged).
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
    logger::print_banner("ZORA DEX INGESTOR", "Raydium SOL/USDC + RAY/USDC + BONK/USDC + ZEC/USDC + UNI/USDC");

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
        PoolConfig {
            pair_label: "SOL/USDC",
            pool_id: "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2",
            base_vault: Pubkey::from_str("DQyrAcCrDXQ7NeoqGgDCZwBvWDcYmFCjSb9JtteuvPpz")
                .context("invalid SOL vault pubkey")?,
            quote_vault: Pubkey::from_str("HLmqeL62xR1QoZ1HKKbXRrdN1p3phKpxRMb2VVopvBBz")
                .context("invalid USDC vault pubkey (SOL/USDC pool)")?,
            base_decimals: 9,
            shm_writer: ShmWriter::new(types::ZORA_SHM_DEX_NAME)
                .context("failed to create SOL/USDC shared memory segment")?,
            connected: false,
            retry_count: 0,
        },
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
        // BONK, 5 decimals -- calculate_price is already generic over
        // base_decimals, same as RAY.
        PoolConfig {
            pair_label: "BONK/USDC",
            pool_id: "G7mw1d83ismcQJKkzt62Ug4noXCjVhu3eV7U5EMgge6Z",
            base_vault: Pubkey::from_str("B4kvik9yEXTJ1UScDUqTxfvhnHUkfChv5pF6zNSyEraQ")
                .context("invalid BONK vault pubkey")?,
            quote_vault: Pubkey::from_str("5BsukuACaQcjVu9XSZ8PP3BN9pBxxtZjQELxosC4Ucyx")
                .context("invalid USDC vault pubkey (BONK/USDC pool)")?,
            base_decimals: 5,
            shm_writer: ShmWriter::new(types::ZORA_SHM_DEX_BONK_NAME)
                .context("failed to create BONK/USDC shared memory segment")?,
            connected: false,
            retry_count: 0,
        },
        // Wormhole-bridged ZEC, 8 decimals. This pool is on Raydium's
        // newer CPMM program rather than the legacy AMM v4 program the
        // other pairs use -- doesn't matter here, since we only ever
        // read raw vault balances via get_multiple_accounts, never call
        // any program-specific swap instruction.
        PoolConfig {
            pair_label: "ZEC/USDC",
            pool_id: "FrMLgdeZrFp6mG4DZsvcVDpbyFMaBcsWRQ9WNk7pbM5e",
            base_vault: Pubkey::from_str("AyzsBxhxK54oftZMBHjLdj9a6tBptPDL6sUyRtrSWuew")
                .context("invalid ZEC vault pubkey")?,
            quote_vault: Pubkey::from_str("5JXdVaPQ6GxH3pF8d6xZiMNN3motNDa5f654J9eyvMbc")
                .context("invalid USDC vault pubkey (ZEC/USDC pool)")?,
            base_decimals: 8,
            shm_writer: ShmWriter::new(types::ZORA_SHM_DEX_ZEC_NAME)
                .context("failed to create ZEC/USDC shared memory segment")?,
            connected: false,
            retry_count: 0,
        },
        // Wormhole-bridged UNI, 8 decimals, legacy AMM v4 pool (same
        // program as SOL/RAY/BONK).
        PoolConfig {
            pair_label: "UNI/USDC",
            pool_id: "8J5fa8WBGaDSv8AUpgtqdh9HM5AZuSf2ijvSkKoaCXCi",
            base_vault: Pubkey::from_str("B5S6r6DBFgB8nxa8P7FnTwps7NAiTsFbiM6Xo7KrGtxP")
                .context("invalid UNI vault pubkey")?,
            quote_vault: Pubkey::from_str("DBd8RZyBi3rdrpbXxXdcmWuTTrfkA5vfPh9HDLo1cHS")
                .context("invalid USDC vault pubkey (UNI/USDC pool)")?,
            base_decimals: 8,
            shm_writer: ShmWriter::new(types::ZORA_SHM_DEX_UNI_NAME)
                .context("failed to create UNI/USDC shared memory segment")?,
            connected: false,
            retry_count: 0,
        },
    ];

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
        // All pools are fetched in one batched RPC call now (see
        // rpc.rs's fetch_all_pool_states) rather than one call per
        // pool -- half the request rate against the public RPC
        // endpoint, which throttles per-request. The trade-off from
        // before (a slow RPC provider occasionally pushing total tick
        // time past POLL_INTERVAL) still applies the same way:
        // saturating_sub below just skips the sleep when that happens.
        let total_elapsed =
            process_all_pools_tick(&client, &pool_vaults, &mut pools).await;

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
    logger::log(Level::Info, &[("shutdown", "complete")]);
    Ok(())
}
