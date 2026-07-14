//! DEX ingestor: polls the Raydium SOL/USDC pool on Solana and
//! publishes reserve/price data to shared memory for the spread
//! detector (not built yet) to consume.
//!
//! `#[tokio::main]` below expands this `async fn main` into a regular
//! `fn main` that spins up a tokio runtime and blocks on this function
//! -- that's what lets us use `.await` here at all; without it, `.await`
//! wouldn't compile outside an `async` context.
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use solana_sdk::pubkey::Pubkey;
use tokio::signal::unix::{signal, SignalKind};

mod logger;
mod pool;
mod rpc;
mod shm;
mod types;

use logger::Level;
use shm::ShmWriter;

// Raydium SOL/USDC pool. The pool ID itself isn't needed by this MVP
// (which reads the two vault balances directly), but is kept here for
// when the spread detector needs the full pool account -- e.g. to read
// the live fee rate instead of assuming Raydium's usual 25 bps.
const RAYDIUM_POOL_ID: &str = "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2";
const SOL_VAULT: &str = "DQyrAcCrDXQ7NeoqGgDCZwBvWDcYmFCjSb9JtteuvPpz";
const USDC_VAULT: &str = "HLmqeL62xR1QoZ1HKKbXRrdN1p3phKpxRMb2VVopvBBz";

const POLL_INTERVAL: Duration = Duration::from_millis(400);
const RPC_WARN_THRESHOLD: Duration = Duration::from_millis(300);
// Bounds how long a stuck or heavily-rate-limited RPC call can stall the
// loop: without this, a hung request blocks not just that poll tick but
// also shutdown (Ctrl+C/SIGTERM are only checked between ticks, never
// while a fetch is in flight).
const RPC_TIMEOUT: Duration = Duration::from_secs(2);

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

#[tokio::main]
async fn main() -> Result<()> {
    logger::print_banner("ZORA DEX INGESTOR", "Raydium SOL/USDC  mainnet");

    // The `?` operator here unwraps `Ok` or returns early with `Err`
    // wrapped in the surrounding function's `Result` -- since `main`
    // returns `Result<()>`, an `Err` bubbling all the way out gets
    // printed and turned into a non-zero exit code automatically. This
    // is fine even though the hard constraint is "no unwrap/expect in
    // production paths, only in tests/main setup": `?` isn't `.unwrap()`
    // (it doesn't panic), and startup failures like a malformed pubkey
    // or a shared memory setup failure genuinely should abort the
    // process rather than continue in a broken state.
    let sol_vault = Pubkey::from_str(SOL_VAULT).context("invalid SOL vault pubkey")?;
    let usdc_vault = Pubkey::from_str(USDC_VAULT).context("invalid USDC vault pubkey")?;

    let client = rpc::build_client();
    // Not "status=CONNECTED" yet: constructing an RpcClient doesn't
    // itself talk to the network, so we don't actually know the RPC
    // endpoint is reachable until the first fetch below succeeds. The
    // banner keeps showing CONNECTING... until then.
    logger::log(
        Level::Info,
        &[
            ("pool", RAYDIUM_POOL_ID),
            ("sol_vault", SOL_VAULT),
            ("usdc_vault", USDC_VAULT),
        ],
    );

    let mut shm_writer = ShmWriter::new().context("failed to create shared memory segment")?;
    logger::log(
        Level::Info,
        &[("shm", "created"), ("path", types::ZORA_SHM_DEX_NAME)],
    );

    // SIGTERM needs explicit opt-in on Unix (there's no default
    // handler tokio installs for it); Ctrl+C / SIGINT is handled
    // portably by `tokio::signal::ctrl_c()` in the select! below.
    let mut sigterm = signal(SignalKind::terminate())?;

    // Tracks whether we've had at least one successful fetch (flips the
    // banner's status from CONNECTING to CONNECTED, once) and how many
    // consecutive failures we've seen (mirrors the CEX ingestor's
    // websocket reconnect `attempt` counter, logged as `retry=`).
    let mut connected = false;
    let mut retry_count: u32 = 0;

    loop {
        let start = Instant::now();

        // `total_elapsed` is filled in below in every arm; declared here
        // so it's in scope for the sleep-remainder calculation after
        // the match.
        let total_elapsed;

        // `tokio::time::timeout` races the fetch against a 2s clock and
        // hands back `Err(Elapsed)` if the clock wins -- the fetch
        // future itself is dropped at that point, which cancels the
        // in-flight HTTP request rather than leaving it running in the
        // background. This is what actually bounds shutdown latency
        // now: without it, a stuck RPC call (rate-limited, dead
        // connection, etc.) blocks Ctrl+C/SIGTERM until the call
        // resolves on its own, which could be arbitrarily long.
        match tokio::time::timeout(
            RPC_TIMEOUT,
            rpc::fetch_pool_state(&client, &sol_vault, &usdc_vault),
        )
        .await
        {
            Ok(Ok((state, network_elapsed, parse_elapsed))) => {
                let shm_start = Instant::now();
                // Sequence itself lives in SHM for the spread detector
                // to read, not in the log line -- the target DEX log
                // format doesn't carry a seq= field the way CEX's does.
                let _seq = shm_writer.write(
                    state.reserve_sol,
                    state.reserve_usdc,
                    state.price,
                    now_micros(),
                    state.slot,
                );
                let shm_elapsed = shm_start.elapsed();
                // Mirrors the CEX ingestor's `age`: starts at the same
                // conceptual point (response bytes in hand, before
                // parsing) and ends at the same point (published to
                // SHM). `parse_elapsed` was measured back in rpc.rs,
                // immediately after the network `.await` returned --
                // adding it here rather than timing from `shm_start`
                // is what makes this number comparable to the CEX
                // side's, instead of only capturing the SHM write.
                let code_elapsed = parse_elapsed + shm_elapsed;
                total_elapsed = start.elapsed();
                retry_count = 0;

                if !connected {
                    connected = true;
                    logger::log(
                        Level::Info,
                        &[("rpc", "connected"), ("status", "CONNECTED")],
                    );
                }

                // One line per tick: price/reserves/slot plus timing.
                // `net` (not `network`) and milliseconds (not micros)
                // to match the target format -- network round trips
                // are the slow, millisecond-scale part here, unlike
                // `code`, which stays in microseconds since it's fast
                // enough that milliseconds would round it to 0.
                logger::log(
                    Level::Info,
                    &[
                        ("price", &format!("{:.3}", state.price)),
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
                            ("rpc", "slow"),
                            ("latency", &format!("{}ms", network_elapsed.as_millis())),
                            ("threshold", &format!("{}ms", RPC_WARN_THRESHOLD.as_millis())),
                        ],
                    );
                }
            }
            // The RPC call completed (didn't time out) but returned an
            // error -- e.g. an account genuinely not found, or the
            // connection was refused outright. Never panics: this just
            // skips the tick rather than killing the process. `{:#}`
            // carries anyhow's full context chain (e.g. "get_multiple_
            // accounts RPC call failed: <underlying error>"), not just
            // the outer message.
            Ok(Err(e)) => {
                total_elapsed = start.elapsed();
                retry_count += 1;
                logger::log(
                    Level::Error,
                    &[
                        ("rpc", "failed"),
                        ("reason", &format!("{:#}", e)),
                        ("retry", &retry_count.to_string()),
                    ],
                );
                if total_elapsed > RPC_WARN_THRESHOLD {
                    logger::log(
                        Level::Warn,
                        &[
                            ("rpc", "slow"),
                            ("latency", &format!("{}ms", total_elapsed.as_millis())),
                            ("threshold", &format!("{}ms", RPC_WARN_THRESHOLD.as_millis())),
                        ],
                    );
                }
            }
            // The timeout won the race: the fetch was still pending
            // after RPC_TIMEOUT, so tokio::time::timeout dropped it
            // (cancelling the in-flight request) and returned Elapsed.
            Err(_elapsed) => {
                total_elapsed = start.elapsed();
                retry_count += 1;
                logger::log(
                    Level::Error,
                    &[
                        ("rpc", "failed"),
                        ("reason", "timeout"),
                        ("retry", &retry_count.to_string()),
                    ],
                );
                logger::log(
                    Level::Warn,
                    &[
                        ("rpc", "slow"),
                        ("latency", &format!("{}ms", total_elapsed.as_millis())),
                        ("threshold", &format!("{}ms", RPC_WARN_THRESHOLD.as_millis())),
                    ],
                );
            }
        }

        // saturating_sub instead of plain `-`: if the RPC call alone
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
            _ = tokio::signal::ctrl_c() => break,
            _ = sigterm.recv() => break,
        }
    }

    shm_writer.shutdown();
    logger::log(Level::Info, &[("shutdown", "complete")]);
    Ok(())
}
