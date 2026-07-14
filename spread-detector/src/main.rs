//! Spread detector: the "brain" of the ZORA arbitrage system. Reads
//! both ingestors' shared memory, computes the CEX-DEX spread, sizes
//! and logs profitable opportunities. Paper mode only -- this process
//! never submits a real order.
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tokio::signal::unix::{signal, SignalKind};

mod detector;
mod logger;
mod paper;
mod shm;
mod sizing;
mod types;

use detector::TOTAL_COSTS_PCT;
use logger::Level;
use types::{Direction, Opportunity};

/// Jito tip is normally quoted in SOL (a fixed lamport amount); this
/// converts it to a USDC-equivalent cost using the current CEX bid as
/// the reference price, so it can be subtracted alongside the
/// percentage-based costs in the same USDC-denominated profit figure.
const FIXED_JITO_TIP_SOL: f64 = 0.003;

fn now_micros() -> u64 {
    // Production paths avoid unwrap/expect; a clock before the epoch
    // would mean the OS clock itself is broken, in which case 0 is a
    // safe, obviously-wrong fallback rather than a panic.
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    logger::print_banner("ZORA SPREAD DETECTOR", "CEX-DEX Arbitrage Brain");

    let max_trade_size_usdc = env_f64("MAX_TRADE_SIZE_USDC", 1000.0);
    let min_net_spread_pct = env_f64("MIN_NET_SPREAD_PCT", 0.0);
    let output_csv = env_string("OUTPUT_CSV", "data/paper_trades.csv");
    let cex_stale_ms = env_u64("CEX_STALE_MS", 500);
    let dex_stale_ms = env_u64("DEX_STALE_MS", 1000);

    // Registered *before* attempting to open shared memory below,
    // deliberately: an earlier version of this code opened SHM first
    // (which can block for a long time retrying if an ingestor hasn't
    // started yet) and only registered signal handling afterward,
    // which meant Ctrl+C/SIGTERM did nothing at all while "waiting for
    // ingestors..." was on screen -- there was no handler installed
    // yet to catch them.
    //
    // Both are created *once* here and reused via `.recv()` for the
    // rest of the process, rather than calling `tokio::signal::ctrl_c()`
    // fresh inside each loop iteration's `select!` (an earlier version
    // did that too): each call to `ctrl_c()` creates a brand-new signal
    // listener and drops it moments later once some other branch of
    // that `select!` wins, which is true almost every time in a hot
    // loop. A real Ctrl+C arriving during one of the many gaps where
    // no listener currently exists is simply missed -- confirmed by
    // testing this exact scenario (a live Ctrl+C did nothing while the
    // process was spin-waiting). A persistent listener has no such gap.
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sigint = signal(SignalKind::interrupt())?;

    // Retries every 200ms, racing each wait against both shutdown
    // signals so this is interruptible even before either ingestor
    // has started. Never treats a missing segment as fatal -- that's
    // an expected, temporary startup condition here, not an error.
    let mut logged_wait = false;
    let reader = loop {
        match shm::ShmReader::try_open() {
            Ok(reader) => break reader,
            Err(_) => {
                if !logged_wait {
                    logger::log(
                        Level::Info,
                        &[("status", "waiting"), ("msg", "waiting_for_ingestors")],
                    );
                    logged_wait = true;
                }
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {}
                    _ = sigint.recv() => {
                        logger::log(Level::Info, &[("shutdown", "complete")]);
                        return Ok(());
                    }
                    _ = sigterm.recv() => {
                        logger::log(Level::Info, &[("shutdown", "complete")]);
                        return Ok(());
                    }
                }
            }
        }
    };
    logger::log(
        Level::Info,
        &[("status", "CONNECTED"), ("msg", "both_shm_segments_mapped")],
    );

    // `?` here is fine even in "no unwrap/expect in production paths":
    // it isn't `.unwrap()` (doesn't panic), and a CSV file we can't
    // create/open is a genuine startup failure worth aborting on,
    // unlike a per-tick error inside the poll loop below.
    let mut paper = paper::PaperLogger::new(&output_csv)?;

    let mut last_cex_seq: u32 = 0;
    let mut iteration: u64 = 0;
    let mut last_seq_change_us = now_micros();
    let mut warned_cex_dead = false;

    loop {
        // Checked every 1000 iterations regardless of whether this
        // iteration found new CEX data or not -- if it were only
        // checked in the "no new data, spin" branch, a CEX feed
        // producing a new sequence on literally every check (never
        // hitting that branch) would starve this process of any
        // chance to observe a shutdown signal at all.
        iteration += 1;
        if iteration % 1000 == 0 {
            tokio::select! {
                _ = tokio::task::yield_now() => {}
                _ = sigint.recv() => break,
                _ = sigterm.recv() => break,
            }

            // Piggybacked on this same checkpoint rather than checked
            // every spin, to avoid adding a clock read to the tight
            // spin loop below: if the CEX ingestor dies outright, its
            // sequence number simply stops changing forever, and the
            // *per-tick* staleness check further down never fires --
            // it only runs after a new sequence arrives, which never
            // happens. Without this, that failure mode is silent
            // 100%-CPU spinning with no log output at all (found by
            // observing exactly that while testing this process).
            let stalled_ms = now_micros().saturating_sub(last_seq_change_us) / 1000;
            if stalled_ms > cex_stale_ms && !warned_cex_dead {
                logger::log(
                    Level::Warn,
                    &[("cex_stale", "true"), ("age", &format!("{stalled_ms}ms"))],
                );
                warned_cex_dead = true;
            }
        }

        let cex = reader.read_cex();
        if cex.sequence == last_cex_seq {
            std::hint::spin_loop();
            continue;
        }
        last_cex_seq = cex.sequence;
        last_seq_change_us = now_micros();
        warned_cex_dead = false;

        let dex = reader.read_dex();
        let now_us = now_micros();

        let cex_age_ms = now_us.saturating_sub(cex.timestamp_us) / 1000;
        if detector::is_stale(now_us, cex.timestamp_us, cex_stale_ms) {
            logger::log(
                Level::Warn,
                &[("cex_stale", "true"), ("age", &format!("{cex_age_ms}ms"))],
            );
            continue;
        }

        let dex_age_ms = now_us.saturating_sub(dex.timestamp_us) / 1000;
        if detector::is_stale(now_us, dex.timestamp_us, dex_stale_ms) {
            logger::log(
                Level::Warn,
                &[("dex_stale", "true"), ("age", &format!("{dex_age_ms}ms"))],
            );
            continue;
        }

        let gross = detector::gross_spread(cex.bid, dex.price);
        let net = detector::net_spread(gross, TOTAL_COSTS_PCT);

        let status = if net > min_net_spread_pct {
            "OPPORTUNITY"
        } else {
            "WATCHING"
        };
        logger::log(
            Level::Info,
            &[
                ("cex", &format!("{:.3}", cex.bid)),
                ("dex", &format!("{:.3}", dex.price)),
                ("gross", &format!("{:+.3}%", gross * 100.0)),
                ("net", &format!("{:+.3}%", net * 100.0)),
                ("status", status),
            ],
        );

        if net <= min_net_spread_pct {
            if net < 0.0 {
                logger::log(
                    Level::Warn,
                    &[
                        ("spread_negative", "true"),
                        ("gap", &format!("{:+.3}%", net * 100.0)),
                        ("threshold", &format!("{:.3}%", TOTAL_COSTS_PCT * 100.0)),
                    ],
                );
            }
            continue;
        }

        // OPPORTUNITY: size it.
        let fixed_jito_tip_usdc = FIXED_JITO_TIP_SOL * cex.bid;
        let (optimal_size, estimated_profit) = sizing::find_optimal_size(
            dex.reserve_usdc,
            dex.reserve_sol,
            cex.bid,
            max_trade_size_usdc,
            TOTAL_COSTS_PCT,
            fixed_jito_tip_usdc,
        );

        // The raw spread looked positive, but price impact at any
        // size up to max_trade_size_usdc ate the whole thing --
        // find_optimal_size returns (0.0, 0.0) in that case.
        if optimal_size <= 0.0 || estimated_profit <= 0.0 {
            continue;
        }

        let amount_in_raw = (optimal_size * 1_000_000.0) as u64;
        let sol_out_raw = sizing::get_amount_out(
            amount_in_raw,
            dex.reserve_usdc,
            dex.reserve_sol,
            sizing::RAYDIUM_FEE_BPS,
        );
        let expected_sol_out = sol_out_raw as f64 / 1_000_000_000.0;

        let opportunity = Opportunity {
            cex_bid: cex.bid,
            dex_price: dex.price,
            gross_spread: gross,
            net_spread: net,
            optimal_size_usdc: optimal_size,
            expected_sol_out,
            estimated_profit,
            direction: Direction::BuyDexSellCex,
            detected_at_us: now_us,
            cex_seq: cex.sequence,
            dex_seq: dex.sequence,
            dex_slot: dex.slot,
        };

        logger::log(
            Level::Info,
            &[
                ("size", &format!("{:.2}", opportunity.optimal_size_usdc)),
                ("profit", &format!("{:.2}", opportunity.estimated_profit)),
                ("direction", opportunity.direction.as_str()),
            ],
        );

        logger::print_opportunity_box(cex.bid, dex.price, gross * 100.0, net * 100.0);

        // Never panics: a CSV write failure is logged and the loop
        // continues -- losing one paper-trade record isn't a reason
        // to kill the whole detector.
        if let Err(e) = paper.log(&opportunity) {
            logger::log(
                Level::Error,
                &[("csv", "write_failed"), ("reason", &format!("{:#}", e))],
            );
        }
    }

    logger::log(Level::Info, &[("shutdown", "complete")]);
    Ok(())
}
