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
//!   BONK is mint_0 and USDC is mint_1 for this pool, which is what
//!   `sqrt_price::sqrt_price_to_price`'s doc comment assumes.
//! - The resulting price (see `crate::sqrt_price::sqrt_price_to_price`)
//!   came out to ~3.048e-6 USDC/BONK, matching the ~3.043e-6 snapshot from
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

// Price conversion (sqrt_price_x64 -> real price) is shared with
// Orca Whirlpools and lives in sqrt_price.rs, not here -- see that
// module's doc comment.

#[cfg(test)]
mod tests {
    use super::*;

    // Live BONK/USDC CLMM pool snapshot (pool
    // 3UwfrdLTpAjxTRni1boc5HUWe6hzc4HgE5yLdvEp2Noc), read via
    // getAccountInfo during implementation of this module.
    const SNAPSHOT_SQRT_PRICE_X64: u128 = 101_848_797_086_041_337;
    const SNAPSHOT_TICK_CURRENT: i32 = -103_989;
    const SNAPSHOT_LIQUIDITY: u128 = 47_758_826_584_743;

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
