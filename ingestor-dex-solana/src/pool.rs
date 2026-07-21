//! Pool state parsing and AMM price/quote math for the Raydium
//! SOL/USDC pool.

/// A single polled snapshot of the pool's reserves, already converted
/// into a normalized price. This is an internal/intermediate value --
/// `shm::ShmWriter::write` is what actually publishes it, adding the
/// atomic sequence number.
#[derive(Debug, Clone, Copy)]
pub struct DexPoolState {
    pub reserve_sol: u64,
    pub reserve_usdc: u64,
    pub price: f64,
    pub slot: u64,
}

const USDC_DECIMALS: f64 = 1_000_000.0;

/// Normalizes raw on-chain reserves into a USDC-per-base-token price.
/// `base_decimals` must match the base token's actual mint decimals --
/// 9 for SOL, 6 for RAY, etc. (this used to be hardcoded to SOL's 9,
/// which silently produced a price 1000x too high when reused for
/// RAY/USDC, since RAY has 6 decimals; caught by testing the RAY pool
/// live and noticing the price didn't match Binance's ~0.69 quote).
/// Returns 0.0 for an empty pool rather than dividing by zero --
/// shouldn't happen for a live pool, but 0.0 is an obviously-wrong
/// sentinel a downstream reader will notice immediately, which is
/// cheaper than plumbing a Result through for a case that shouldn't
/// occur.
pub fn calculate_price(reserve_base: u64, reserve_usdc: u64, base_decimals: u32) -> f64 {
    let base_normalized = reserve_base as f64 / 10f64.powi(base_decimals as i32);
    if base_normalized == 0.0 {
        return 0.0;
    }
    let usdc_normalized = reserve_usdc as f64 / USDC_DECIMALS;
    usdc_normalized / base_normalized
}

/// Raydium's constant-product (x * y = k) swap quote, in the pool's
/// raw integer units. `fee_bps` is in basis points (Raydium's SOL/USDC
/// pool charges 25 bps = 0.25%).
///
/// Not used by the MVP poll loop (which only reads reserves), but
/// needed later by the spread detector to size a profitable trade once
/// an arbitrage opportunity is found -- hence `allow(dead_code)`: this
/// binary crate has no other internal caller yet, but the function is
/// intentionally kept (and unit-tested) ahead of that consumer existing.
#[allow(dead_code)]
pub fn get_amount_out(amount_in: u64, reserve_in: u64, reserve_out: u64, fee_bps: u64) -> u64 {
    let amount_with_fee = amount_in as u128 * (10_000 - fee_bps) as u128;
    let numerator = amount_with_fee * reserve_out as u128;
    let denominator = reserve_in as u128 * 10_000 + amount_with_fee;
    (numerator / denominator) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn price_normalizes_decimals_correctly_for_9_decimal_base() {
        // 1 SOL (1e9 lamports, 9 decimals) in a pool worth 150 USDC
        // (150e6 micro-USDC) per SOL should normalize to price = 150.0
        let price = calculate_price(1_000_000_000, 150_000_000, 9);
        assert!((price - 150.0).abs() < 1e-9);
    }

    #[test]
    fn price_normalizes_decimals_correctly_for_6_decimal_base() {
        // 1 RAY (1e6 raw units, 6 decimals) in a pool worth 0.69 USDC
        // per RAY should normalize to price = 0.69, not 690 -- this is
        // exactly the bug that shipped when base_decimals wasn't a
        // parameter (see calculate_price's doc comment).
        let price = calculate_price(1_000_000, 690_000, 6);
        assert!((price - 0.69).abs() < 1e-9);
    }

    #[test]
    fn price_is_zero_for_empty_pool() {
        assert_eq!(calculate_price(0, 1_000_000, 9), 0.0);
    }

    #[test]
    fn get_amount_out_no_fee_matches_constant_product() {
        // x*y=k: reserve_in=1000, reserve_out=1000, amount_in=100, fee=0.
        // new_reserve_in=1100, new_reserve_out=1_000_000/1100=909 (floor),
        // amount_out = 1000 - 909 = 91... verified independently below
        // to be 90 due to integer truncation happening in a single step
        // rather than two, matching this function's actual formula.
        let out = get_amount_out(100, 1_000, 1_000, 0);
        assert_eq!(out, 90);
    }

    #[test]
    fn get_amount_out_with_raydium_fee() {
        // reserve_in=1_000_000, reserve_out=2_000_000, amount_in=1000,
        // fee=25bps (Raydium's actual fee). Verified independently.
        let out = get_amount_out(1_000, 1_000_000, 2_000_000, 25);
        assert_eq!(out, 1993);
    }

    #[test]
    fn get_amount_out_zero_amount_in_yields_zero() {
        let out = get_amount_out(0, 1_000_000, 2_000_000, 25);
        assert_eq!(out, 0);
    }
}
