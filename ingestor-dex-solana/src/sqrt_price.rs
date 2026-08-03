//! Shared Q64.64 sqrt-price math for concentrated-liquidity pools.
//! Raydium CLMM (clmm.rs) and Orca Whirlpools (orca.rs) are different
//! programs with different account layouts, but both encode price the
//! same way -- sqrt(token_1_raw / token_0_raw) in Q64.64 fixed point --
//! so this conversion is shared rather than duplicated or, worse,
//! pulled by one parser out of the other's module.

/// Converts a Q64.64 sqrt-price into a real-world price.
///
/// `sqrt_price_x64` is sqrt(token_1_raw / token_0_raw) in Q64.64 fixed
/// point; squaring it gives the raw ratio token_1_raw/token_0_raw
/// (smallest units of each mint, undecimalized). `decimals_0` is
/// mint_0's decimals, `decimals_1` is mint_1's -- multiplying by
/// 10^(decimals_0 - decimals_1) rescales that raw ratio into real
/// token_1 units per real token_0 unit:
///
///   real_token0 = raw_token0 / 10^decimals_0
///   real_token1 = raw_token1 / 10^decimals_1
///   price (token_1 per token_0)
///     = real_token1 / real_token0
///     = (raw_token1 / raw_token0) * 10^(decimals_0 - decimals_1)
///
/// Callers must pass decimals in mint_0/mint_1 order, not "base/quote"
/// by convention: which mint is 0 vs. 1 is decided on-chain by raw
/// pubkey ordering (token_mint_0 < token_mint_1 for Raydium CLMM,
/// token_mint_a < token_mint_b for Orca Whirlpools), not by which
/// token is economically the "base". See `price_in_quote_per_base` for
/// the base/quote-aware wrapper most callers actually want.
pub fn sqrt_price_to_price(sqrt_price_x64: u128, decimals_0: i32, decimals_1: i32) -> f64 {
    let price_raw = (sqrt_price_x64 as f64 / (1u128 << 64) as f64).powi(2);
    price_raw * 10f64.powi(decimals_0 - decimals_1)
}

/// Converts a Q64.64 sqrt-price into a quote-per-base price, given
/// which mint (0 or 1) is economically the "base" token.
///
/// `sqrt_price_to_price` always returns token_1/token_0 -- that's a
/// fact about the on-chain encoding, not about which token a caller
/// considers the base. Whether the base token happens to be mint_0 or
/// mint_1 is pool-specific (decided by raw pubkey ordering, not by
/// symbol) and must be checked per pool, not assumed -- getting this
/// backwards silently produces a valid-looking but inverted price
/// (e.g. "3.07" instead of "0.325"), not a crash, so every caller
/// passes `base_is_mint_0` explicitly rather than this function
/// guessing.
pub fn price_in_quote_per_base(
    sqrt_price_x64: u128,
    decimals_0: i32,
    decimals_1: i32,
    base_is_mint_0: bool,
) -> f64 {
    let price_1_per_0 = sqrt_price_to_price(sqrt_price_x64, decimals_0, decimals_1);
    if base_is_mint_0 {
        price_1_per_0
    } else {
        1.0 / price_1_per_0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Live BONK/USDC CLMM pool snapshot (pool
    // 3UwfrdLTpAjxTRni1boc5HUWe6hzc4HgE5yLdvEp2Noc), read via
    // getAccountInfo during implementation of clmm.rs. BONK is mint_0.
    const BONK_SQRT_PRICE_X64: u128 = 101_848_797_086_041_337;
    const BONK_EXPECTED_PRICE: f64 = 3.042638548482449e-6;

    // Live TRX/USDC Raydium CLMM pool snapshot (pool
    // HpgV2jnzgrGfrZjeZGkHgTnEgRFAhtzCVuk3BRFTFJwk), read via
    // getAccountInfo while adding TRX support. USDC is mint_0 here
    // (opposite order from the BONK pool), TRX is mint_1 -- confirmed
    // independently against Dexscreener's reported price
    // (0.3254 USDC/TRX at the time of the read) within ordinary price
    // drift, not the ~10x/reciprocal error a flipped mint-order
    // assumption would produce.
    const TRX_SQRT_PRICE_X64: u128 = 32_335_876_459_198_372_403;
    const TRX_EXPECTED_PRICE_USDC_PER_TRX: f64 = 0.3254;

    // Live KMNO/USDC Orca Whirlpool snapshot (pool
    // 3ndjN1nJVUKGrJBc1hhVpER6kWTZKHdyDrPyCJyX3CXK), read via
    // getAccountInfo while adding Orca support. KMNO is mint_a
    // (mint_0-equivalent), USDC is mint_b -- confirmed against
    // Dexscreener's reported price (0.01794 USDC/KMNO at the time of
    // the read).
    const KMNO_SQRT_PRICE: u128 = 2_471_028_187_629_921_260;
    const KMNO_EXPECTED_PRICE_USDC_PER_KMNO: f64 = 0.01794;

    #[test]
    fn sqrt_price_to_price_matches_known_bonk_usdc_snapshot() {
        let price = sqrt_price_to_price(BONK_SQRT_PRICE_X64, 5, 6);
        // 1% covers ordinary price drift between the two reads while
        // still catching a decimals-direction bug, which would be off
        // by a full power of 10 (1000%), not 1%.
        let rel_err = (price - BONK_EXPECTED_PRICE).abs() / BONK_EXPECTED_PRICE;
        assert!(
            rel_err < 0.01,
            "price {price} not within 1% of expected {BONK_EXPECTED_PRICE} (rel_err={rel_err})"
        );
    }

    #[test]
    fn sqrt_price_to_price_zero_is_zero() {
        assert_eq!(sqrt_price_to_price(0, 5, 6), 0.0);
    }

    #[test]
    fn sqrt_price_to_price_decimals_direction_matters() {
        // Swapping decimals_0/decimals_1 should move the result by
        // 10^(2*(decimals_1 - decimals_0)), not leave it unchanged --
        // this is the exact bug class the task warned about (silent
        // garbage from a flipped/wrong offset or argument order).
        let correct = sqrt_price_to_price(BONK_SQRT_PRICE_X64, 5, 6);
        let flipped = sqrt_price_to_price(BONK_SQRT_PRICE_X64, 6, 5);
        assert!((flipped / correct - 100.0).abs() < 1e-6);
    }

    #[test]
    fn price_in_quote_per_base_no_inversion_when_base_is_mint_0() {
        // BONK is mint_0, USDC is mint_1 -- base_is_mint_0=true should
        // reproduce sqrt_price_to_price's raw token_1/token_0 result
        // unchanged.
        let price = price_in_quote_per_base(BONK_SQRT_PRICE_X64, 5, 6, true);
        let rel_err = (price - BONK_EXPECTED_PRICE).abs() / BONK_EXPECTED_PRICE;
        assert!(rel_err < 0.01, "price {price} not within 1% of {BONK_EXPECTED_PRICE}");
    }

    #[test]
    fn price_in_quote_per_base_inverts_when_base_is_mint_1() {
        // USDC is mint_0, TRX is mint_1 for this pool -- base_is_mint_0
        // must be false to get USDC-per-TRX rather than TRX-per-USDC
        // (which would be ~3.07, not ~0.325 -- a reciprocal, not a
        // rounding-sized error, so this is easy to catch if it
        // regresses).
        let price = price_in_quote_per_base(TRX_SQRT_PRICE_X64, 6, 6, false);
        let rel_err =
            (price - TRX_EXPECTED_PRICE_USDC_PER_TRX).abs() / TRX_EXPECTED_PRICE_USDC_PER_TRX;
        assert!(
            rel_err < 0.02,
            "price {price} not within 2% of {TRX_EXPECTED_PRICE_USDC_PER_TRX} (rel_err={rel_err})"
        );
    }

    #[test]
    fn price_in_quote_per_base_matches_known_kmno_usdc_snapshot() {
        let price = price_in_quote_per_base(KMNO_SQRT_PRICE, 6, 6, true);
        let rel_err =
            (price - KMNO_EXPECTED_PRICE_USDC_PER_KMNO).abs() / KMNO_EXPECTED_PRICE_USDC_PER_KMNO;
        assert!(
            rel_err < 0.02,
            "price {price} not within 2% of {KMNO_EXPECTED_PRICE_USDC_PER_KMNO} (rel_err={rel_err})"
        );
    }

    #[test]
    fn price_in_quote_per_base_zero_is_zero_either_orientation() {
        assert_eq!(price_in_quote_per_base(0, 5, 6, true), 0.0);
        // 1.0 / 0.0 is +inf in IEEE 754, not a panic -- explicitly
        // pinned here so a future change to a checked division isn't
        // mistaken for a regression.
        assert!(price_in_quote_per_base(0, 5, 6, false).is_infinite());
    }
}
