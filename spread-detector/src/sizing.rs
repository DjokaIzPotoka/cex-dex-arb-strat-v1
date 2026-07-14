//! Optimal position sizing via binary search on the DEX's constant-
//! product curve: larger trades move the DEX price against us, so
//! there's a size beyond which additional size stops being profitable.

/// Raydium's constant-product (x*y=k) swap quote, in the pool's raw
/// integer units. Duplicated from `ingestor-dex-solana/src/pool.rs`
/// rather than imported: that crate has no library target (bin-only)
/// and is deliberately kept outside this workspace (see the root
/// Cargo.toml), so this small, already-unit-tested function is
/// independently duplicated here rather than linked.
pub fn get_amount_out(amount_in: u64, reserve_in: u64, reserve_out: u64, fee_bps: u64) -> u64 {
    let amount_with_fee = amount_in as u128 * (10_000 - fee_bps) as u128;
    let numerator = amount_with_fee * reserve_out as u128;
    let denominator = reserve_in as u128 * 10_000 + amount_with_fee;
    (numerator / denominator) as u64
}

pub const RAYDIUM_FEE_BPS: u64 = 25; // 0.25%

/// Binary search for the USDC input size that (approximately)
/// maximizes profit: buy SOL on the DEX with `size` USDC (paying
/// Raydium's price-impact curve, which worsens as size grows), then
/// sell the resulting SOL on CEX at `cex_bid`.
///
/// This searches on the *sign of marginal profit* (does one more
/// $0.01 of size still help?), not on the sign of total profit.
/// Total profit is concave in size here: it's negative at size 0
/// (fixed costs), rises as size grows, peaks, then falls back through
/// zero at some larger "breakeven" size where price impact has fully
/// eaten the spread. Bisecting on "is total profit still positive?"
/// converges to that breakeven size -- the *worst* profitable size,
/// since profit is ~0 there by definition -- not the best one.
/// Bisecting on marginal profit instead converges to the actual peak.
///
/// `reserve_usdc_raw`/`reserve_sol_raw` are the DEX pool's raw integer
/// reserves (micro-USDC / lamports, matching `ZoraDexPrice`'s own
/// units) -- USDC is the input side of this swap (we're buying SOL).
///
/// Returns `(optimal_size_usdc, estimated_profit_usdc)`. Both are 0.0
/// if no size in `[0, max_size_usdc]` is profitable.
pub fn find_optimal_size(
    reserve_usdc_raw: u64,
    reserve_sol_raw: u64,
    cex_bid: f64,
    max_size_usdc: f64,
    total_costs_pct: f64,
    fixed_jito_tip_usdc: f64,
) -> (f64, f64) {
    let profit_at = |size_usdc: f64| -> f64 {
        let amount_in_raw = (size_usdc * 1_000_000.0) as u64;
        let sol_out_raw = get_amount_out(
            amount_in_raw,
            reserve_usdc_raw,
            reserve_sol_raw,
            RAYDIUM_FEE_BPS,
        );
        let sol_out = sol_out_raw as f64 / 1_000_000_000.0;
        let usdc_from_cex = sol_out * cex_bid;
        let costs = size_usdc * total_costs_pct + fixed_jito_tip_usdc;
        usdc_from_cex - size_usdc - costs
    };

    // Fixed (not interval-relative) step for the marginal-profit
    // finite difference: we want "is one more cent of size still
    // worth it", the same question at every scale, not a step that
    // shrinks as the search interval narrows.
    const DERIVATIVE_STEP_USDC: f64 = 0.01;

    let mut low = 0.0_f64;
    let mut high = max_size_usdc;

    for _ in 0..50 {
        let mid = (low + high) / 2.0;
        let marginal = profit_at(mid + DERIVATIVE_STEP_USDC) - profit_at(mid);
        if marginal > 0.0 {
            low = mid; // profit still rising -- peak is to the right
        } else {
            high = mid; // profit already falling -- peak is to the left
        }
    }

    let profit = profit_at(low);
    if profit <= 0.0 {
        (0.0, 0.0)
    } else {
        (low, profit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pool worth ~4,000,000 USDC against ~55,000 SOL implies a DEX
    // price of ~72.73 USDC/SOL -- clearly below a 76.0 CEX bid, so a
    // real, sizeable opportunity should exist within these reserves.
    const RESERVE_USDC_RAW: u64 = 4_000_000 * 1_000_000;
    const RESERVE_SOL_RAW: u64 = 55_000 * 1_000_000_000;
    const CEX_BID: f64 = 76.0;
    const TOTAL_COSTS_PCT: f64 = 0.00425;

    fn fixed_jito_tip_usdc() -> f64 {
        0.003 * CEX_BID
    }

    #[test]
    fn optimal_size_finds_positive_profit_when_spread_exists() {
        let (size, profit) = find_optimal_size(
            RESERVE_USDC_RAW,
            RESERVE_SOL_RAW,
            CEX_BID,
            1000.0,
            TOTAL_COSTS_PCT,
            fixed_jito_tip_usdc(),
        );
        assert!(size > 0.0, "expected a positive size, got {size}");
        assert!(profit > 0.0, "expected positive profit, got {profit}");
    }

    #[test]
    fn profit_drops_meaningfully_above_optimal_size() {
        // A much smaller pool than the other tests use: with the
        // earlier ~4M USDC pool, a $1000 cap is tiny enough relative
        // to pool depth that profit is still rising at the cap --
        // there's no interior peak to find within [0, 1000], so that
        // scenario can't exercise "profit drops past the optimum".
        // This pool (50k USDC / 700 SOL) puts the real peak around
        // size=1300-1400 (verified independently), comfortably inside
        // a 5000 cap.
        let reserve_usdc_raw = 50_000 * 1_000_000u64;
        let reserve_sol_raw = 700 * 1_000_000_000u64;
        let max_size_usdc = 5000.0;

        let (size, profit) = find_optimal_size(
            reserve_usdc_raw,
            reserve_sol_raw,
            CEX_BID,
            max_size_usdc,
            TOTAL_COSTS_PCT,
            fixed_jito_tip_usdc(),
        );
        assert!(profit > 0.0, "expected positive profit, got {profit}");
        assert!(
            size < max_size_usdc,
            "expected an interior peak, not the max_size_usdc boundary: size={size}"
        );

        // A meaningfully larger size should show reduced (or negative)
        // profit -- price impact eating further into the spread.
        let larger_size = (size + 300.0).min(max_size_usdc);
        let amount_in_raw = (larger_size * 1_000_000.0) as u64;
        let sol_out_raw =
            get_amount_out(amount_in_raw, reserve_usdc_raw, reserve_sol_raw, RAYDIUM_FEE_BPS);
        let sol_out = sol_out_raw as f64 / 1_000_000_000.0;
        let larger_profit = sol_out * CEX_BID
            - larger_size
            - (larger_size * TOTAL_COSTS_PCT + fixed_jito_tip_usdc());

        assert!(
            larger_profit < profit,
            "expected profit to drop above the optimal size: {larger_profit} >= {profit}"
        );
    }

    #[test]
    fn no_opportunity_when_dex_price_above_cex_bid() {
        // Same reserves as above but a CEX bid below the DEX's ~72.73
        // implied price -- no direction where buying DEX/selling CEX
        // is profitable.
        let (size, profit) = find_optimal_size(
            RESERVE_USDC_RAW,
            RESERVE_SOL_RAW,
            70.0,
            1000.0,
            TOTAL_COSTS_PCT,
            0.003 * 70.0,
        );
        assert_eq!(size, 0.0);
        assert_eq!(profit, 0.0);
    }

    #[test]
    fn get_amount_out_matches_known_value() {
        // Verified independently: 1000-unit input, 25bps fee.
        let out = get_amount_out(1_000, 1_000_000, 2_000_000, RAYDIUM_FEE_BPS);
        assert_eq!(out, 1993);
    }
}
