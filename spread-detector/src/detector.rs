//! Pure spread calculation and staleness checks. Kept free of any I/O
//! or SHM access so these are simple to unit test in isolation.

/// Jito tip estimate (0.10%) + Binance taker fee (0.075%) -- the parts
/// of round-trip cost that don't depend on which DEX pool a pair
/// trades against. The DEX swap fee itself varies per pool/program
/// (a Standard-AMM pool's 0.25% is nothing like a thin CPMM pool's
/// 2%), so it's supplied per pair via `total_costs_pct` below rather
/// than folded into one global constant.
pub const JITO_TIP_PCT: f64 = 0.0010;
pub const BINANCE_TAKER_FEE_PCT: f64 = 0.00075;

/// Legacy Raydium Standard-AMM v4 fee (0.25%), still accurate for
/// RAY/USDC -- the only pair left on that program (UNI/USDC used to
/// share it too, before being removed for having no viable pool
/// anywhere).
pub const RAYDIUM_FEE_PCT: f64 = 0.0025;

/// Round-trip cost for a pair, given that pair's own DEX pool fee.
/// Fetched per pool from Raydium's/Orca's pool-info API (feeRate /
/// tradeFeeRate), not assumed -- these vary by an order of magnitude
/// or more across pools/programs:
///   SOL/USDC (Raydium CLMM, 3ucNos4Nbu...):        0.04%
///   RAY/USDC (Raydium Standard AMM v4):             0.25%
///   BONK/USDC (Raydium CLMM, 3UwfrdLTpA...):        0.15%
///   ZEC/USDC (Orca Whirlpool, GTHKH8s82Z...):       0.16%
///   TRX/USDC (Raydium CLMM, HpgV2jnzgr...):         0.05%
///   KMNO/USDC (Orca Whirlpool, 3ndjN1nJVU...):      0.01%
/// (ZEC/USDC used to trade against a Raydium CPMM pool with a 2% fee
/// and ~$26k TVL -- switched to the Orca Whirlpool above, which has
/// ~$3.6M TVL and a much lower fee.)
pub fn total_costs_pct(dex_fee_pct: f64) -> f64 {
    dex_fee_pct + JITO_TIP_PCT + BINANCE_TAKER_FEE_PCT
}

pub const SOL_CLMM_FEE_PCT: f64 = 0.0004;
pub const BONK_CLMM_FEE_PCT: f64 = 0.0015;
pub const ZEC_WHIRLPOOL_FEE_PCT: f64 = 0.0016;
pub const TRX_CLMM_FEE_PCT: f64 = 0.0005;
pub const KMNO_WHIRLPOOL_FEE_PCT: f64 = 0.0001;

/// (cex_bid - dex_price) / dex_price. Uses the CEX *bid* rather than
/// mid: bid is what selling into the order book actually fills at, and
/// mid would overstate the achievable spread.
pub fn gross_spread(cex_bid: f64, dex_price: f64) -> f64 {
    (cex_bid - dex_price) / dex_price
}

/// Gross spread minus round-trip costs. Positive means a genuinely
/// profitable-before-sizing opportunity.
pub fn net_spread(gross_spread: f64, total_costs_pct: f64) -> f64 {
    gross_spread - total_costs_pct
}

/// True if `data_ts_us` is more than `max_age_ms` old relative to
/// `now_us`. `saturating_sub` guards against `data_ts_us` being
/// (implausibly) ahead of `now_us` -- e.g. clock skew between this
/// process and whichever CPU core wrote the timestamp -- by treating
/// that as zero age rather than wrapping to a huge u64.
pub fn is_stale(now_us: u64, data_ts_us: u64, max_age_ms: u64) -> bool {
    let age_us = now_us.saturating_sub(data_ts_us);
    age_us > max_age_ms.saturating_mul(1000)
}

/// How interesting a given spread reading is. `Watching` is not logged
/// at all (too noisy at typical tick rates); `Interesting` and
/// `Opportunity` both get logged and recorded to CSV, but only
/// `Opportunity` gets sized and shown in the opportunity box.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpreadStatus {
    /// gross_spread <= the configured MIN_LOG_GROSS_SPREAD threshold.
    Watching,
    /// gross_spread above threshold, but net_spread (after costs)
    /// isn't -- worth recording for later distribution analysis, not
    /// worth sizing a trade for.
    Interesting,
    /// net_spread > the configured MIN_NET_SPREAD_PCT threshold --
    /// profitable after costs, before accounting for price impact
    /// (sizing in main.rs may still reduce this to nothing).
    Opportunity,
}

impl SpreadStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SpreadStatus::Watching => "WATCHING",
            SpreadStatus::Interesting => "INTERESTING",
            SpreadStatus::Opportunity => "OPPORTUNITY",
        }
    }
}

/// Classifies a spread reading. Opportunity is checked first even
/// though it implies gross_spread is also above the (much smaller)
/// Interesting threshold: net_spread = gross_spread - costs, so
/// net_spread > 0 already means gross_spread > costs, and costs
/// (0.425%) is always well above a sane min_log_gross_spread (e.g.
/// 0.05%) -- checking order here doesn't change behavior, just avoids
/// a redundant comparison.
pub fn classify_spread(
    gross_spread: f64,
    net_spread: f64,
    min_log_gross_spread: f64,
    min_net_spread: f64,
) -> SpreadStatus {
    if net_spread > min_net_spread {
        SpreadStatus::Opportunity
    } else if gross_spread > min_log_gross_spread {
        SpreadStatus::Interesting
    } else {
        SpreadStatus::Watching
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gross_spread_positive_when_cex_bid_above_dex_price() {
        // (77.0 - 76.5) / 76.5 = 0.006535947712418301
        let g = gross_spread(77.0, 76.5);
        assert!((g - 0.006535947712418301).abs() < 1e-12);
    }

    #[test]
    fn gross_spread_negative_when_cex_bid_below_dex_price() {
        let g = gross_spread(76.0, 76.5);
        assert!(g < 0.0);
    }

    #[test]
    fn net_spread_subtracts_total_costs() {
        let g = 0.005;
        let n = net_spread(g, total_costs_pct(RAYDIUM_FEE_PCT));
        assert!((n - (0.005 - 0.00425)).abs() < 1e-12);
    }

    #[test]
    fn total_costs_pct_matches_spec_for_standard_amm() {
        assert!((total_costs_pct(RAYDIUM_FEE_PCT) - 0.00425).abs() < 1e-12);
    }

    #[test]
    fn total_costs_pct_varies_per_pool_fee() {
        // The cheapest CLMM/Whirlpool pool here costs meaningfully
        // less per round trip than a Standard-AMM pair -- this is what
        // makes one global constant wrong across the whole roster.
        assert!(total_costs_pct(KMNO_WHIRLPOOL_FEE_PCT) < total_costs_pct(RAYDIUM_FEE_PCT));
    }

    #[test]
    fn staleness_detects_old_data() {
        assert!(is_stale(1_000_000, 0, 500)); // 1s old vs 500ms threshold
        assert!(!is_stale(1_000_000, 900_000, 500)); // 100ms old vs 500ms threshold
    }

    #[test]
    fn staleness_handles_clock_skew_without_underflow() {
        // data_ts_us slightly ahead of now_us shouldn't panic or wrap.
        assert!(!is_stale(1000, 1050, 500));
    }

    const MIN_LOG_GROSS: f64 = 0.0005; // 0.05%
    const MIN_NET: f64 = 0.0; // 0.0%

    #[test]
    fn classify_watching_when_gross_at_or_below_threshold() {
        assert_eq!(
            classify_spread(0.0004, -0.0038, MIN_LOG_GROSS, MIN_NET),
            SpreadStatus::Watching
        );
        // Exactly at the threshold is still Watching (spec: "> 0.05%").
        assert_eq!(
            classify_spread(MIN_LOG_GROSS, MIN_LOG_GROSS - 0.00425, MIN_LOG_GROSS, MIN_NET),
            SpreadStatus::Watching
        );
    }

    #[test]
    fn classify_interesting_when_gross_above_but_net_not_positive() {
        // gross=0.20% (above 0.05%), net = 0.20% - 0.425% = -0.225%
        assert_eq!(
            classify_spread(0.0020, -0.00225, MIN_LOG_GROSS, MIN_NET),
            SpreadStatus::Interesting
        );
    }

    #[test]
    fn classify_opportunity_when_net_positive() {
        // gross=0.60%, net = 0.60% - 0.425% = +0.175%
        assert_eq!(
            classify_spread(0.0060, 0.00175, MIN_LOG_GROSS, MIN_NET),
            SpreadStatus::Opportunity
        );
    }

    #[test]
    fn classify_respects_configured_thresholds() {
        // A custom, wider MIN_LOG_GROSS_SPREAD should reclassify a
        // reading that would otherwise be Interesting as Watching.
        assert_eq!(
            classify_spread(0.0020, -0.00225, 0.0030, MIN_NET),
            SpreadStatus::Watching
        );
    }
}
