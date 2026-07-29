//! Minimal Raydium CLMM (concentrated liquidity) pool-state reader:
//! current price only. Deliberately does not implement swap/price-
//! impact math (get_amount_out's tick-crossing equivalent) -- that's
//! for the executor, later.
//!
//! Byte offsets below are sourced from
//! raydium-io/raydium-clmm@master, programs/amm/src/states/pool.rs
//! (`#[account(zero_copy(unsafe))] #[repr(C, packed)] struct PoolState`),
//! not guessed: `repr(C, packed)` guarantees declaration-order fields
//! with no inter-field padding, and Anchor prepends a fixed 8-byte
//! account discriminator before those fields on-chain. Offsets (as
//! bytes into the *full* account data, discriminator included):
//!
//!   bump              @   8 (1 byte)
//!   amm_config        @   9 (32)
//!   owner             @  41 (32)
//!   token_mint_0      @  73 (32)
//!   token_mint_1      @ 105 (32)
//!   token_vault_0     @ 137 (32)
//!   token_vault_1     @ 169 (32)
//!   observation_key   @ 201 (32)
//!   mint_decimals_0   @ 233 (1)
//!   mint_decimals_1   @ 234 (1)
//!   tick_spacing      @ 235 (2)
//!   liquidity         @ 237 (16)  <- read
//!   sqrt_price_x64    @ 253 (16)  <- read
//!   tick_current      @ 269 (4)   <- read
//!
//! Independently cross-checked live (not just derived on paper) against
//! pool 3UwfrdLTpAjxTRni1boc5HUWe6hzc4HgE5yLdvEp2Noc (BONK/USDC) during
//! implementation:
//! - The 8-byte discriminator read back as f7ede3f5d7c3de46, matching
//!   sha256("account:PoolState")[..8] computed independently -- this
//!   pins the struct identity, not just its field layout.
//! - mint_decimals_0/mint_decimals_1 at the computed offsets read back
//!   as 5/6, and token_mint_0/token_mint_1 (also decoded) matched
//!   BONK's and USDC's real mint addresses respectively -- confirming
//!   BONK is mint_0 (`decimals_a`) and USDC is mint_1 (`decimals_b`)
//!   for this pool, which is what `sqrt_price_to_price`'s doc comment
//!   below assumes.
//! - The resulting price (see `sqrt_price_to_price`) came out to
//!   ~3.048e-6 USDC/BONK, matching the ~3.043e-6 snapshot from
//!   Raydium's own API within ~0.2% (expected drift between two
//!   different reads of a live price, not a units/offset error, which
//!   would show up as an off-by-10x, not a 0.2% difference).
use anyhow::{bail, Result};

/// Expected Anchor discriminator for `PoolState`, i.e.
/// `sha256("account:PoolState")[..8]`. Checked defensively: a wrong
/// pool pubkey or an upstream layout change would otherwise misparse
/// silently (garbage sqrt_price_x64 in, garbage price out, no crash to
/// signal it) -- exactly the failure mode this module's own doc
/// comment warns about.
const POOL_STATE_DISCRIMINATOR: [u8; 8] = [0xf7, 0xed, 0xe3, 0xf5, 0xd7, 0xc3, 0xde, 0x46];

const OFFSET_LIQUIDITY: usize = 237;
const OFFSET_SQRT_PRICE_X64: usize = 253;
const OFFSET_TICK_CURRENT: usize = 269;
const MIN_ACCOUNT_LEN: usize = OFFSET_TICK_CURRENT + 4;

#[derive(Debug, Clone, Copy)]
pub struct ClmmPoolState {
    pub sqrt_price_x64: u128,
    pub tick_current: i32,
    pub liquidity: u128,
}

/// Parses `liquidity`/`sqrt_price_x64`/`tick_current` out of a raw
/// CLMM `PoolState` account's data. Rejects anything that isn't
/// actually a `PoolState` account (wrong discriminator) or is too
/// short to contain the fields we read, rather than risk silently
/// returning garbage.
pub fn parse_clmm_account(data: &[u8]) -> Result<ClmmPoolState> {
    if data.len() < MIN_ACCOUNT_LEN {
        bail!(
            "CLMM account too short: {} bytes, need at least {}",
            data.len(),
            MIN_ACCOUNT_LEN
        );
    }
    if data[0..8] != POOL_STATE_DISCRIMINATOR {
        bail!(
            "account discriminator {:02x?} doesn't match PoolState {:02x?} -- wrong pubkey or upstream layout change",
            &data[0..8],
            POOL_STATE_DISCRIMINATOR
        );
    }

    let liquidity =
        u128::from_le_bytes(data[OFFSET_LIQUIDITY..OFFSET_LIQUIDITY + 16].try_into().unwrap());
    let sqrt_price_x64 = u128::from_le_bytes(
        data[OFFSET_SQRT_PRICE_X64..OFFSET_SQRT_PRICE_X64 + 16]
            .try_into()
            .unwrap(),
    );
    let tick_current = i32::from_le_bytes(
        data[OFFSET_TICK_CURRENT..OFFSET_TICK_CURRENT + 4]
            .try_into()
            .unwrap(),
    );

    Ok(ClmmPoolState {
        sqrt_price_x64,
        tick_current,
        liquidity,
    })
}

/// Converts a CLMM pool's Q64.64 sqrt-price into a real-world price.
///
/// `sqrt_price_x64` is sqrt(token_1_raw / token_0_raw) in Q64.64 fixed
/// point (Raydium's own doc comment on the field); squaring it gives
/// the raw ratio token_1_raw/token_0_raw (smallest units of each mint,
/// undecimalized). `decimals_a` is token_0's mint decimals, `decimals_b`
/// is token_1's -- multiplying by 10^(decimals_a - decimals_b) rescales
/// that raw ratio into real token_1 units per real token_0 unit:
///
///   real_token0 = raw_token0 / 10^decimals_a
///   real_token1 = raw_token1 / 10^decimals_b
///   price (token_1 per token_0)
///     = real_token1 / real_token0
///     = (raw_token1 / raw_token0) * 10^(decimals_a - decimals_b)
///
/// For BONK/USDC specifically, BONK is token_0 (decimals_a=5) and USDC
/// is token_1 (decimals_b=6) -- see this module's doc comment for how
/// that was confirmed, not assumed. Callers must pass decimals in
/// mint_0/mint_1 order, not "base/quote" by convention, since which
/// mint is 0 vs. 1 is decided on-chain by raw pubkey ordering
/// (token_mint_0 < token_mint_1), not by which token is economically
/// the "base".
pub fn sqrt_price_to_price(sqrt_price_x64: u128, decimals_a: i32, decimals_b: i32) -> f64 {
    let price_raw = (sqrt_price_x64 as f64 / (1u128 << 64) as f64).powi(2);
    price_raw * 10f64.powi(decimals_a - decimals_b)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Live BONK/USDC CLMM pool snapshot (pool
    // 3UwfrdLTpAjxTRni1boc5HUWe6hzc4HgE5yLdvEp2Noc), read via
    // getAccountInfo during implementation of this module.
    const SNAPSHOT_SQRT_PRICE_X64: u128 = 101_848_797_086_041_337;
    const SNAPSHOT_TICK_CURRENT: i32 = -103_989;
    const SNAPSHOT_LIQUIDITY: u128 = 47_758_826_584_743;
    // Raydium API's reported price at the time given in the task spec
    // (a few seconds apart from the live read above -- a live price
    // moves between the two, so the test tolerance below is loose
    // enough to absorb that drift without masking a real offset bug).
    const TASK_SPEC_EXPECTED_PRICE: f64 = 3.042638548482449e-6;

    #[test]
    fn sqrt_price_to_price_matches_known_bonk_usdc_snapshot() {
        let price = sqrt_price_to_price(SNAPSHOT_SQRT_PRICE_X64, 5, 6);
        // 1% covers ordinary price drift between the two reads while
        // still catching a decimals-direction bug, which would be off
        // by a full power of 10 (1000%), not 1%.
        let rel_err = (price - TASK_SPEC_EXPECTED_PRICE).abs() / TASK_SPEC_EXPECTED_PRICE;
        assert!(
            rel_err < 0.01,
            "price {price} not within 1% of expected {TASK_SPEC_EXPECTED_PRICE} (rel_err={rel_err})"
        );
    }

    #[test]
    fn sqrt_price_to_price_zero_is_zero() {
        assert_eq!(sqrt_price_to_price(0, 5, 6), 0.0);
    }

    #[test]
    fn sqrt_price_to_price_decimals_direction_matters() {
        // Swapping decimals_a/decimals_b should move the result by
        // 10^(2*(decimals_b - decimals_a)), not leave it unchanged --
        // this is the exact bug class the task warned about (silent
        // garbage from a flipped/wrong offset or argument order).
        let correct = sqrt_price_to_price(SNAPSHOT_SQRT_PRICE_X64, 5, 6);
        let flipped = sqrt_price_to_price(SNAPSHOT_SQRT_PRICE_X64, 6, 5);
        assert!((flipped / correct - 100.0).abs() < 1e-6);
    }

    fn synthetic_account_bytes() -> Vec<u8> {
        let mut buf = vec![0u8; MIN_ACCOUNT_LEN];
        buf[0..8].copy_from_slice(&POOL_STATE_DISCRIMINATOR);
        buf[OFFSET_LIQUIDITY..OFFSET_LIQUIDITY + 16]
            .copy_from_slice(&SNAPSHOT_LIQUIDITY.to_le_bytes());
        buf[OFFSET_SQRT_PRICE_X64..OFFSET_SQRT_PRICE_X64 + 16]
            .copy_from_slice(&SNAPSHOT_SQRT_PRICE_X64.to_le_bytes());
        buf[OFFSET_TICK_CURRENT..OFFSET_TICK_CURRENT + 4]
            .copy_from_slice(&SNAPSHOT_TICK_CURRENT.to_le_bytes());
        buf
    }

    #[test]
    fn parse_clmm_account_reads_known_fields() {
        let buf = synthetic_account_bytes();
        let state = parse_clmm_account(&buf).unwrap();
        assert_eq!(state.liquidity, SNAPSHOT_LIQUIDITY);
        assert_eq!(state.sqrt_price_x64, SNAPSHOT_SQRT_PRICE_X64);
        assert_eq!(state.tick_current, SNAPSHOT_TICK_CURRENT);
    }

    #[test]
    fn parse_clmm_account_rejects_short_data() {
        let short = vec![0u8; 100];
        assert!(parse_clmm_account(&short).is_err());
    }

    #[test]
    fn parse_clmm_account_rejects_wrong_discriminator() {
        let mut buf = synthetic_account_bytes();
        buf[0] ^= 0xff; // corrupt just the discriminator
        assert!(parse_clmm_account(&buf).is_err());
    }
}
