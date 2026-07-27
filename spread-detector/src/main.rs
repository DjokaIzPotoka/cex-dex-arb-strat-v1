//! Spread detector: the "brain" of the ZORA arbitrage system. Reads
//! both ingestors' shared memory for each configured pair, computes
//! the CEX-DEX spread, sizes and logs profitable opportunities. Paper
//! mode only -- this process never submits a real order.
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tokio::signal::unix::{signal, SignalKind};

mod detector;
mod logger;
mod paper;
mod shm;
mod sizing;
mod types;

use detector::{SpreadStatus, TOTAL_COSTS_PCT};
use logger::Level;
use types::{Direction, Opportunity};

/// Jito tip is normally quoted in SOL (a fixed lamport amount); this
/// converts it to a USDC-equivalent cost using the current CEX bid as
/// the reference price, so it can be subtracted alongside the
/// percentage-based costs in the same USDC-denominated profit figure.
const FIXED_JITO_TIP_SOL: f64 = 0.003;

/// How often a reopen is actually attempted once a pair is observed
/// stale, regardless of how many times staleness gets re-detected in
/// that window (the periodic watchdog and the per-tick checks below
/// can all fire far more often than this). Without this throttle, a
/// pair stuck stale would hammer shm_open/mmap on every single check.
const REOPEN_RETRY_INTERVAL_US: u64 = 200_000; // 200ms

/// How often an *already-reported* staleness episode is re-logged,
/// independent of REOPEN_RETRY_INTERVAL_US above -- deliberately much
/// longer. Reopen attempts stay fast (200ms) so a real ingestor restart
/// is picked up quickly, but logging a fresh warning every single one
/// of those attempts is what turned a real, if unremarkable, gap in a
/// thin pair's feed (RAY/USDC routinely goes several seconds between
/// Binance bookTicker updates -- a thin order book just doesn't move
/// that often) into a console flooded with a warn+remap pair every
/// 200ms for the entire gap. This keeps the "still stale" reminder
/// alive (so a genuine, ongoing problem doesn't go silent) without
/// spamming on every retry.
const STALE_WARN_REPEAT_INTERVAL_US: u64 = 5_000_000; // 5s

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

/// Formats a data-freshness age for logging, treating `data_ts_us == 0`
/// specially: both ingestors' writers zero their segment on mmap
/// before ever writing to it (see their own shm_writer.cpp / shm.rs),
/// so a just-(re)opened segment that hasn't received its first real
/// write yet reads back as timestamp_us=0. Computing a plain
/// now_us - 0 there gives a "since the Unix epoch" age in the
/// quadrillions of milliseconds -- accurate arithmetic, but a
/// nonsensical, alarming thing to print for "no data yet", which is
/// all it actually means. Found by testing try_reopen against a
/// freshly-created segment.
fn format_age(now_us: u64, data_ts_us: u64) -> String {
    if data_ts_us == 0 {
        "not_yet_populated".to_string()
    } else {
        format!("{}ms", now_us.saturating_sub(data_ts_us) / 1000)
    }
}

/// Per-pair state carried across poll loop iterations: which shared
/// memory segments to read, and how far along that pair's own
/// "wait for a new CEX sequence" tracking is.
struct PairContext {
    pair_label: &'static str,
    reader: shm::ShmReader,
    // Kept around (not just used once at startup) so a stale pair can
    // re-resolve the same two names later via try_reopen -- an
    // ingestor that restarts unlinks its old segment and creates a new
    // one under this same name, which this reader's existing mapping
    // has no way to notice on its own.
    cex_shm_name: &'static str,
    dex_shm_name: &'static str,
    last_cex_seq: u32,
    last_seq_change_us: u64,
    warned_cex_dead: bool,
    // 0 means "never attempted" -- always due on the first staleness
    // observation, since now_micros() is always > REOPEN_RETRY_INTERVAL_US
    // in practice (comfortably past the epoch).
    last_reopen_attempt_us: u64,
    // Governs how often a *log line* is emitted for an ongoing CEX
    // staleness episode, separately from last_reopen_attempt_us (which
    // governs how often a reopen is retried) -- see should_log_stale.
    last_cex_stale_warn_us: u64,
    // DEX staleness gets its own warned/last-warn pair rather than
    // reusing the CEX ones: CEX and DEX go stale independently (e.g.
    // Solana RPC hiccups without Binance doing so, or vice versa), and
    // the per-tick loop resets warned_cex_dead the moment CEX ticks --
    // if DEX staleness shared that flag, a fast-ticking CEX feed would
    // keep resetting it and defeat the repeat-warning throttle for a
    // DEX-only outage.
    warned_dex_dead: bool,
    last_dex_stale_warn_us: u64,
    // Floor below which |cex.bid - dex.price| is treated as quoting
    // noise rather than a real spread, regardless of what percentage
    // that difference computes to. 0.0 means no guard (the default --
    // fine for pairs priced high enough that a single exchange tick
    // is a negligible fraction of the price). Needed for very
    // low-priced pairs like BONK (~$0.000003): Binance's own tick
    // size there is 0.00000001, so a single tick of bid movement
    // alone computes to a ~0.3% "gross spread" with nothing behind it
    // -- confirmed by testing, where BONK repeatedly logged
    // INTERESTING/OPPORTUNITY status off of exactly a 1-2 tick
    // cex/dex difference.
    min_abs_price_delta: f64,
}

/// Whether an ongoing staleness episode should produce a log line right
/// now: yes the first time it's observed (and marks it warned), then at
/// most once every STALE_WARN_REPEAT_INTERVAL_US after that for as long
/// as it persists. Callers reuse this same answer to gate both the
/// staleness warning itself and try_reopen_if_due's own
/// "remapped"/"reopen_failed" logging, so one ongoing episode produces
/// one coherent burst of log lines every few seconds rather than a
/// warn+remap pair every single REOPEN_RETRY_INTERVAL_US. Takes the
/// warned flag and its paired timestamp directly (rather than a whole
/// `&mut PairContext`) so it works for both the CEX and DEX staleness
/// tracks, which each need their own independent instance of this state.
fn should_log_stale(warned: &mut bool, last_warn_us: &mut u64, now_us: u64) -> bool {
    if !*warned {
        *warned = true;
        *last_warn_us = now_us;
        true
    } else if now_us.saturating_sub(*last_warn_us) >= STALE_WARN_REPEAT_INTERVAL_US {
        *last_warn_us = now_us;
        true
    } else {
        false
    }
}

/// Attempts to remap a stale pair's shared memory, throttled to at
/// most once per `REOPEN_RETRY_INTERVAL_US` regardless of how often
/// this is called -- callers just call it every time staleness is
/// observed, however frequently that is, and this decides whether an
/// attempt is actually due yet.
///
/// This is what makes "remapped" recovery from an ingestor restart
/// work: the ingestor's old segment was `shm_unlink`ed and a new one
/// created under the same name, and `try_reopen` re-resolves that name
/// to pick up the new segment (see shm.rs for why the existing mapping
/// can't notice this on its own).
///
/// `should_log` is the caller's already-computed `should_log_stale`
/// answer for this same tick: the actual reopen attempt always happens
/// on its own REOPEN_RETRY_INTERVAL_US cadence regardless (cheap, and
/// needed for prompt recovery), but whether it's *logged* follows the
/// slower, separate STALE_WARN_REPEAT_INTERVAL_US cadence -- otherwise
/// a long-but-harmless gap (segment never actually went away, just
/// hasn't been written to) would log a "remapped" line every single
/// retry attempt.
fn try_reopen_if_due(pair: &mut PairContext, now_us: u64, should_log: bool) {
    if now_us.saturating_sub(pair.last_reopen_attempt_us) < REOPEN_RETRY_INTERVAL_US {
        return;
    }
    pair.last_reopen_attempt_us = now_us;

    match pair.reader.try_reopen(pair.cex_shm_name, pair.dex_shm_name) {
        Ok(()) => {
            if should_log {
                logger::log(
                    Level::Info,
                    &[
                        ("pair", pair.pair_label),
                        ("shm", "remapped"),
                        ("msg", "SHM_remapped,_continuing"),
                    ],
                );
            }
            // Deliberately does NOT reset last_seq_change_us or
            // warned_cex_dead here. shm_open+mmap succeeding only means
            // the *name* still resolves to *some* segment -- it says
            // nothing about whether that segment's data is actually
            // fresh. An earlier version reset the staleness clock right
            // here, which was wrong: if the underlying ingestor is alive
            // but stuck (not the "old segment unlinked, new one
            // created" case this method is for), the segment it remaps
            // to on every retry is the exact same still-stale one, and
            // resetting the clock each time made the watchdog report a
            // misleadingly constant "juuust over threshold" age forever
            // instead of the true, growing age -- confirmed by testing:
            // a genuinely stuck CEX process produced an endless
            // "age=501ms" instead of an honestly increasing one. Real
            // recovery is only ever detected where it actually happens:
            // the per-tick read below, when a fresh, non-stale sequence
            // is observed -- that's also the only place warned_cex_dead
            // gets reset, so an ongoing episode keeps being reported
            // (via should_log_stale's repeat interval) rather than
            // silently resetting itself every 200ms.
            //
            // last_cex_seq is likewise left alone: read_cex() already
            // treats whatever sequence the (possibly new) segment
            // reports as "new" the moment it differs from the old
            // value, which happens naturally after a real writer
            // restart (its counter starts over from 0/1).
        }
        Err(e) => {
            // Never panics: the old (stale but still valid) mapping
            // is untouched on failure -- see try_reopen's own doc
            // comment for why that's safe. The next call (next time
            // staleness is observed, at most REOPEN_RETRY_INTERVAL_US
            // later) tries again; this recurs naturally from the
            // call sites below rather than needing its own loop.
            if should_log {
                logger::log(
                    Level::Warn,
                    &[
                        ("pair", pair.pair_label),
                        ("shm", "reopen_failed"),
                        ("reason", &format!("{:#}", e)),
                        ("msg", "retrying..."),
                    ],
                );
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    logger::print_banner("ZORA SPREAD DETECTOR", "CEX-DEX Arbitrage Brain");

    let max_trade_size_usdc = env_f64("MAX_TRADE_SIZE_USDC", 1000.0);
    // Both thresholds are configured as percentages (e.g. "0.05" means
    // 0.05%), matching how they're written in logs/CSV -- converted
    // once here to the fraction units gross_spread/net_spread actually
    // use internally (0.05% -> 0.0005).
    let min_net_spread = env_f64("MIN_NET_SPREAD_PCT", 0.0) / 100.0;
    let min_log_gross_spread = env_f64("MIN_LOG_GROSS_SPREAD", 0.05) / 100.0;
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

    // Retries every 200ms, racing the wait against both shutdown
    // signals so this is interruptible even before either ingestor has
    // started. Both pairs' segments are attempted together each retry
    // (rather than independently) -- simpler, and in practice both
    // ingestors start together anyway. Never treats a missing segment
    // as fatal -- that's an expected, temporary startup condition here,
    // not an error.
    let mut logged_wait = false;
    let (sol_reader, ray_reader, bonk_reader, zec_reader, uni_reader) = loop {
        let sol_result = shm::ShmReader::try_open(types::ZORA_SHM_CEX_NAME, types::ZORA_SHM_DEX_NAME);
        let ray_result =
            shm::ShmReader::try_open(types::ZORA_SHM_CEX_RAY_NAME, types::ZORA_SHM_DEX_RAY_NAME);
        let bonk_result =
            shm::ShmReader::try_open(types::ZORA_SHM_CEX_BONK_NAME, types::ZORA_SHM_DEX_BONK_NAME);
        let zec_result =
            shm::ShmReader::try_open(types::ZORA_SHM_CEX_ZEC_NAME, types::ZORA_SHM_DEX_ZEC_NAME);
        let uni_result =
            shm::ShmReader::try_open(types::ZORA_SHM_CEX_UNI_NAME, types::ZORA_SHM_DEX_UNI_NAME);

        match (sol_result, ray_result, bonk_result, zec_result, uni_result) {
            (Ok(sol), Ok(ray), Ok(bonk), Ok(zec), Ok(uni)) => {
                break (sol, ray, bonk, zec, uni)
            }
            _ => {
                if !logged_wait {
                    logger::log(
                        Level::Info,
                        &[("status", "waiting"), ("msg", "waiting_for_ingestors")],
                    );
                    logged_wait = true;
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {}
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
        &[("status", "CONNECTED"), ("msg", "all_shm_segments_mapped")],
    );

    // `?` here is fine even in "no unwrap/expect in production paths":
    // it isn't `.unwrap()` (doesn't panic), and a CSV file we can't
    // create/open is a genuine startup failure worth aborting on,
    // unlike a per-tick error inside the poll loop below.
    let mut paper = paper::PaperLogger::new(&output_csv)?;

    let mut pairs = vec![
        PairContext {
            pair_label: "SOL/USDC",
            reader: sol_reader,
            cex_shm_name: types::ZORA_SHM_CEX_NAME,
            dex_shm_name: types::ZORA_SHM_DEX_NAME,
            last_cex_seq: 0,
            last_seq_change_us: now_micros(),
            warned_cex_dead: false,
            last_reopen_attempt_us: 0,
            last_cex_stale_warn_us: 0,
            warned_dex_dead: false,
            last_dex_stale_warn_us: 0,
            min_abs_price_delta: 0.0,
        },
        PairContext {
            pair_label: "RAY/USDC",
            reader: ray_reader,
            cex_shm_name: types::ZORA_SHM_CEX_RAY_NAME,
            dex_shm_name: types::ZORA_SHM_DEX_RAY_NAME,
            last_cex_seq: 0,
            last_seq_change_us: now_micros(),
            warned_cex_dead: false,
            last_reopen_attempt_us: 0,
            last_cex_stale_warn_us: 0,
            warned_dex_dead: false,
            last_dex_stale_warn_us: 0,
            min_abs_price_delta: 0.0,
        },
        PairContext {
            pair_label: "BONK/USDC",
            reader: bonk_reader,
            cex_shm_name: types::ZORA_SHM_CEX_BONK_NAME,
            dex_shm_name: types::ZORA_SHM_DEX_BONK_NAME,
            last_cex_seq: 0,
            last_seq_change_us: now_micros(),
            warned_cex_dead: false,
            last_reopen_attempt_us: 0,
            last_cex_stale_warn_us: 0,
            warned_dex_dead: false,
            last_dex_stale_warn_us: 0,
            // Binance's BONKUSDC tick size is 0.00000001 -- 10 ticks
            // (0.0000001) is comfortably above the 1-2 tick noise
            // observed producing spurious INTERESTING/OPPORTUNITY
            // readings, while still letting a genuine larger move
            // through.
            min_abs_price_delta: 0.0000001,
        },
        PairContext {
            pair_label: "ZEC/USDC",
            reader: zec_reader,
            cex_shm_name: types::ZORA_SHM_CEX_ZEC_NAME,
            dex_shm_name: types::ZORA_SHM_DEX_ZEC_NAME,
            last_cex_seq: 0,
            last_seq_change_us: now_micros(),
            warned_cex_dead: false,
            last_reopen_attempt_us: 0,
            last_cex_stale_warn_us: 0,
            warned_dex_dead: false,
            last_dex_stale_warn_us: 0,
            min_abs_price_delta: 0.0,
        },
        PairContext {
            pair_label: "UNI/USDC",
            reader: uni_reader,
            cex_shm_name: types::ZORA_SHM_CEX_UNI_NAME,
            dex_shm_name: types::ZORA_SHM_DEX_UNI_NAME,
            last_cex_seq: 0,
            last_seq_change_us: now_micros(),
            warned_cex_dead: false,
            last_reopen_attempt_us: 0,
            last_cex_stale_warn_us: 0,
            warned_dex_dead: false,
            last_dex_stale_warn_us: 0,
            min_abs_price_delta: 0.0,
        },
    ];

    let mut iteration: u64 = 0;

    loop {
        // Checked every 1000 iterations regardless of whether this
        // iteration found new CEX data for any pair -- if it were only
        // checked when nothing changed, a CEX feed producing a new
        // sequence on literally every check (never hitting that
        // branch) would starve this process of any chance to observe
        // a shutdown signal at all.
        iteration += 1;
        if iteration % 1000 == 0 {
            tokio::select! {
                _ = tokio::task::yield_now() => {}
                _ = sigint.recv() => break,
                _ = sigterm.recv() => break,
            }

            // Piggybacked on this same checkpoint rather than checked
            // every spin, to avoid adding a clock read to the tight
            // spin loop below: if a CEX ingestor dies outright, its
            // sequence number simply stops changing forever, and the
            // *per-tick* staleness check further down never fires for
            // that pair -- it only runs after a new sequence arrives,
            // which never happens. Without this, that failure mode is
            // silent 100%-CPU spinning with no log output at all.
            let now_us = now_micros();
            for pair in &mut pairs {
                let stalled_ms = now_us.saturating_sub(pair.last_seq_change_us) / 1000;
                if stalled_ms > cex_stale_ms {
                    let should_log = should_log_stale(
                        &mut pair.warned_cex_dead,
                        &mut pair.last_cex_stale_warn_us,
                        now_us,
                    );
                    if should_log {
                        logger::log(
                            Level::Warn,
                            &[
                                ("pair", pair.pair_label),
                                ("cex_stale", "true"),
                                ("age", &format!("{stalled_ms}ms")),
                            ],
                        );
                    }
                    // Covers both an ingestor that crashed outright
                    // (in which case every reopen attempt keeps
                    // failing, logged and retried at
                    // REOPEN_RETRY_INTERVAL_US) and one that crashed
                    // and has since been restarted (the first attempt
                    // after it comes back up succeeds and remaps).
                    try_reopen_if_due(pair, now_us, should_log);
                }
            }
        }

        let mut any_new = false;

        for pair in &mut pairs {
            let cex = pair.reader.read_cex();
            if cex.sequence == pair.last_cex_seq {
                continue;
            }
            any_new = true;
            pair.last_cex_seq = cex.sequence;
            pair.last_seq_change_us = now_micros();
            pair.warned_cex_dead = false;

            let dex = pair.reader.read_dex();
            let now_us = now_micros();

            if detector::is_stale(now_us, cex.timestamp_us, cex_stale_ms) {
                let should_log = should_log_stale(
                    &mut pair.warned_cex_dead,
                    &mut pair.last_cex_stale_warn_us,
                    now_us,
                );
                if should_log {
                    logger::log(
                        Level::Warn,
                        &[
                            ("pair", pair.pair_label),
                            ("cex_stale", "true"),
                            ("age", &format_age(now_us, cex.timestamp_us)),
                        ],
                    );
                }
                try_reopen_if_due(pair, now_us, should_log);
                continue;
            }

            if detector::is_stale(now_us, dex.timestamp_us, dex_stale_ms) {
                let should_log = should_log_stale(
                    &mut pair.warned_dex_dead,
                    &mut pair.last_dex_stale_warn_us,
                    now_us,
                );
                if should_log {
                    logger::log(
                        Level::Warn,
                        &[
                            ("pair", pair.pair_label),
                            ("dex_stale", "true"),
                            ("age", &format_age(now_us, dex.timestamp_us)),
                        ],
                    );
                }
                try_reopen_if_due(pair, now_us, should_log);
                continue;
            }
            pair.warned_dex_dead = false;

            let gross = detector::gross_spread(cex.bid, dex.price);
            let net = detector::net_spread(gross, TOTAL_COSTS_PCT);
            let mut status =
                detector::classify_spread(gross, net, min_log_gross_spread, min_net_spread);

            // Downgrades a percentage-based Interesting/Opportunity
            // reading back to Watching if the underlying absolute
            // cex/dex difference doesn't clear this pair's noise
            // floor (0.0 for most pairs -- see min_abs_price_delta's
            // own doc comment for why very low-priced pairs like BONK
            // need this).
            if (cex.bid - dex.price).abs() < pair.min_abs_price_delta {
                status = SpreadStatus::Watching;
            }

            // Watching: too noisy to log at all, and below the CSV
            // recording threshold too (both use the same gross_spread
            // cutoff) -- skip straight to the next pair.
            if status == SpreadStatus::Watching {
                continue;
            }

            logger::log(
                Level::Info,
                &[
                    ("pair", pair.pair_label),
                    // 8 decimals, not 3: BONK/USDC's price (~0.0000031)
                    // needs it -- 3 decimals would print "0.000" for
                    // every BONK line. Harmless extra trailing digits
                    // for the larger-priced pairs.
                    ("cex", &format!("{:.8}", cex.bid)),
                    ("dex", &format!("{:.8}", dex.price)),
                    ("gross", &format!("{:+.3}%", gross * 100.0)),
                    ("net", &format!("{:+.3}%", net * 100.0)),
                    ("status", status.as_str()),
                ],
            );

            if status == SpreadStatus::Interesting {
                if net < 0.0 {
                    logger::log(
                        Level::Warn,
                        &[
                            ("pair", pair.pair_label),
                            ("spread_negative", "true"),
                            ("gap", &format!("{:+.3}%", net * 100.0)),
                            ("threshold", &format!("{:.3}%", TOTAL_COSTS_PCT * 100.0)),
                        ],
                    );
                }

                // Recorded per the spec ("record ALL rows where
                // gross_spread > threshold, both Interesting and
                // Opportunity", to analyze the full distribution
                // later) -- size/profit are 0 since sizing isn't run
                // for a spread that isn't profitable even before
                // price impact.
                let opportunity = Opportunity {
                    pair: pair.pair_label,
                    cex_bid: cex.bid,
                    dex_price: dex.price,
                    gross_spread: gross,
                    net_spread: net,
                    optimal_size_usdc: 0.0,
                    expected_sol_out: 0.0,
                    estimated_profit: 0.0,
                    direction: Direction::BuyDexSellCex,
                    detected_at_us: now_us,
                    cex_seq: cex.sequence,
                    dex_seq: dex.sequence,
                    dex_slot: dex.slot,
                };
                if let Err(e) = paper.log(&opportunity) {
                    logger::log(
                        Level::Error,
                        &[
                            ("pair", pair.pair_label),
                            ("csv", "write_failed"),
                            ("reason", &format!("{:#}", e)),
                        ],
                    );
                }
                continue;
            }

            // status == Opportunity: size it.
            //
            // CLMM-sourced pairs (BONK/USDC as of this writing) report
            // reserve_sol=0/reserve_usdc=0 -- there's no constant-
            // product curve to size against yet (see
            // ingestor-dex-solana/src/clmm.rs), and feeding a zero
            // reserve into find_optimal_size's binary search reaches an
            // amount_in=0/reserve_in=0 division by zero in
            // sizing::get_amount_out the first time low stays at its
            // initial 0.0 (which it always does once reserve_out=0
            // makes every size's profit strictly decreasing -- see that
            // function). Treat it the same as "no profitable size
            // found" below: the spread itself still gets logged and
            // recorded to CSV.
            let (optimal_size, estimated_profit) = if dex.reserve_usdc == 0 || dex.reserve_sol == 0 {
                (0.0, 0.0)
            } else {
                let fixed_jito_tip_usdc = FIXED_JITO_TIP_SOL * cex.bid;
                sizing::find_optimal_size(
                    dex.reserve_usdc,
                    dex.reserve_sol,
                    cex.bid,
                    max_trade_size_usdc,
                    TOTAL_COSTS_PCT,
                    fixed_jito_tip_usdc,
                )
            };

            // The raw spread looked positive, but price impact at any
            // size up to max_trade_size_usdc ate the whole thing --
            // find_optimal_size returns (0.0, 0.0) in that case. Still
            // recorded to CSV above the gross_spread threshold, just
            // with no sizing/box.
            if optimal_size <= 0.0 || estimated_profit <= 0.0 {
                let opportunity = Opportunity {
                    pair: pair.pair_label,
                    cex_bid: cex.bid,
                    dex_price: dex.price,
                    gross_spread: gross,
                    net_spread: net,
                    optimal_size_usdc: 0.0,
                    expected_sol_out: 0.0,
                    estimated_profit: 0.0,
                    direction: Direction::BuyDexSellCex,
                    detected_at_us: now_us,
                    cex_seq: cex.sequence,
                    dex_seq: dex.sequence,
                    dex_slot: dex.slot,
                };
                if let Err(e) = paper.log(&opportunity) {
                    logger::log(
                        Level::Error,
                        &[
                            ("pair", pair.pair_label),
                            ("csv", "write_failed"),
                            ("reason", &format!("{:#}", e)),
                        ],
                    );
                }
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
                pair: pair.pair_label,
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
                    ("pair", pair.pair_label),
                    ("size", &format!("{:.2}", opportunity.optimal_size_usdc)),
                    ("profit", &format!("{:.2}", opportunity.estimated_profit)),
                    ("direction", opportunity.direction.as_str()),
                ],
            );

            logger::print_opportunity_box(cex.bid, dex.price, gross * 100.0, net * 100.0);

            // Never panics: a CSV write failure is logged and the loop
            // continues -- losing one paper-trade record isn't a
            // reason to kill the whole detector.
            if let Err(e) = paper.log(&opportunity) {
                logger::log(
                    Level::Error,
                    &[
                        ("pair", pair.pair_label),
                        ("csv", "write_failed"),
                        ("reason", &format!("{:#}", e)),
                    ],
                );
            }
        }

        if !any_new {
            std::hint::spin_loop();
        }
    }

    logger::log(Level::Info, &[("shutdown", "complete")]);
    Ok(())
}
