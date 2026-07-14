//! Pure spread calculation and staleness checks. Kept free of any I/O
//! or SHM access so these are simple to unit test in isolation.

/// Raydium fee (0.25%) + Jito tip estimate (0.10%) + Binance taker fee
/// (0.075%) = 0.425% total round-trip cost, as a fraction (matching
/// `gross_spread`'s own units, not a percentage).
pub const RAYDIUM_FEE_PCT: f64 = 0.0025;
pub const JITO_TIP_PCT: f64 = 0.0010;
pub const BINANCE_TAKER_FEE_PCT: f64 = 0.00075;
pub const TOTAL_COSTS_PCT: f64 = RAYDIUM_FEE_PCT + JITO_TIP_PCT + BINANCE_TAKER_FEE_PCT;

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
        let n = net_spread(g, TOTAL_COSTS_PCT);
        assert!((n - (0.005 - 0.00425)).abs() < 1e-12);
    }

    #[test]
    fn total_costs_pct_matches_spec() {
        assert!((TOTAL_COSTS_PCT - 0.00425).abs() < 1e-12);
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
}
