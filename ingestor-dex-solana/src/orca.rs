//! Minimal Orca Whirlpool (concentrated liquidity) pool-state reader:
//! current price only, mirroring clmm.rs's scope for Raydium CLMM --
//! no swap/price-impact math here either.
//!
//! Byte offsets below are sourced from orca-so/whirlpools@main,
//! programs/whirlpool/src/state/whirlpool.rs (`#[account] struct
//! Whirlpool`), not guessed. Anchor's `#[account]` macro
//! (de)serializes via Borsh, which -- like `repr(C, packed)` --
//! writes fixed-size fields (u8/u16/u32/u64/u128/[u8; N]/Pubkey) back
//! to back in declaration order with no padding, and prepends a fixed
//! 8-byte account discriminator. Offsets (as bytes into the *full*
//! account data, discriminator included):
//!
//!   whirlpools_config @   8 (32)
//!   whirlpool_bump    @  40 (1)
//!   tick_spacing      @  41 (2)
//!   fee_tier_index_seed @ 43 (2)
//!   fee_rate          @  45 (2)
//!   protocol_fee_rate @  47 (2)
//!   liquidity         @  49 (16)  <- read
//!   sqrt_price        @  65 (16)  <- read
//!   tick_current_index@  81 (4)   <- read
//!   protocol_fee_owed_a @ 85 (8)
//!   protocol_fee_owed_b @ 93 (8)
//!   token_mint_a      @ 101 (32)
//!   token_vault_a     @ 133 (32)
//!   fee_growth_global_a @ 165 (16)
//!   token_mint_b      @ 181 (32)
//!   token_vault_b     @ 213 (32)
//!   ...
//!
//! (`Whirlpool::LEN` in the same file is `8 + 261 + 384 = 653`, which
//! matches summing the fields above through the reward infos array --
//! an independent cross-check that these offsets/widths, not just the
//! discriminator, are right.)
//!
//! Independently cross-checked live against pool
//! 3ndjN1nJVUKGrJBc1hhVpER6kWTZKHdyDrPyCJyX3CXK (KMNO/USDC) during
//! implementation:
//! - The 8-byte discriminator read back as 3f95d10ce1806309, matching
//!   sha256("account:Whirlpool")[..8] computed independently.
//! - The account's owner read back as
//!   whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc (Orca's Whirlpool
//!   program).
//! - token_mint_a/token_mint_b (also decoded, though not read at
//!   runtime -- see `parse_whirlpool_account`'s doc comment) matched
//!   KMNO's and USDC's real mint addresses respectively, confirming
//!   KMNO is mint_a for this pool.
//! - The resulting price (see `crate::sqrt_price::price_in_quote_per_base`)
//!   came out to ~0.017944 USDC/KMNO, matching Dexscreener's ~0.01794
//!   snapshot within ~0.02% (expected drift between two different
//!   reads of a live price, not a units/offset error).
use anyhow::{bail, Result};

/// Expected Anchor discriminator for `Whirlpool`, i.e.
/// `sha256("account:Whirlpool")[..8]`. Checked defensively for the
/// same reason clmm.rs checks `PoolState`'s: a wrong pool pubkey or an
/// upstream layout change would otherwise misparse silently.
const WHIRLPOOL_DISCRIMINATOR: [u8; 8] = [0x3f, 0x95, 0xd1, 0x0c, 0xe1, 0x80, 0x63, 0x09];

const OFFSET_LIQUIDITY: usize = 49;
const OFFSET_SQRT_PRICE: usize = 65;
const OFFSET_TICK_CURRENT_INDEX: usize = 81;
const MIN_ACCOUNT_LEN: usize = OFFSET_TICK_CURRENT_INDEX + 4;

#[derive(Debug, Clone, Copy)]
pub struct WhirlpoolState {
    pub sqrt_price: u128,
    pub tick_current_index: i32,
    pub liquidity: u128,
}

/// Parses `liquidity`/`sqrt_price`/`tick_current_index` out of a raw
/// Orca `Whirlpool` account's data. Like `clmm::parse_clmm_account`,
/// doesn't read `token_mint_a`/`token_mint_b` at runtime -- which mint
/// is which was confirmed once against a live account during
/// implementation (see this module's doc comment) and is supplied per
/// pool by the caller instead, since the account owner check already
/// guards against a wrong/migrated pool pubkey misparsing unrelated
/// bytes as `Whirlpool` fields.
pub fn parse_whirlpool_account(data: &[u8]) -> Result<WhirlpoolState> {
    if data.len() < MIN_ACCOUNT_LEN {
        bail!(
            "Whirlpool account too short: {} bytes, need at least {}",
            data.len(),
            MIN_ACCOUNT_LEN
        );
    }
    if data[0..8] != WHIRLPOOL_DISCRIMINATOR {
        bail!(
            "account discriminator {:02x?} doesn't match Whirlpool {:02x?} -- wrong pubkey or upstream layout change",
            &data[0..8],
            WHIRLPOOL_DISCRIMINATOR
        );
    }

    let liquidity =
        u128::from_le_bytes(data[OFFSET_LIQUIDITY..OFFSET_LIQUIDITY + 16].try_into().unwrap());
    let sqrt_price = u128::from_le_bytes(
        data[OFFSET_SQRT_PRICE..OFFSET_SQRT_PRICE + 16]
            .try_into()
            .unwrap(),
    );
    let tick_current_index = i32::from_le_bytes(
        data[OFFSET_TICK_CURRENT_INDEX..OFFSET_TICK_CURRENT_INDEX + 4]
            .try_into()
            .unwrap(),
    );

    Ok(WhirlpoolState {
        sqrt_price,
        tick_current_index,
        liquidity,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Live KMNO/USDC Whirlpool snapshot (pool
    // 3ndjN1nJVUKGrJBc1hhVpER6kWTZKHdyDrPyCJyX3CXK), read via
    // getAccountInfo during implementation of this module.
    const SNAPSHOT_SQRT_PRICE: u128 = 2_471_028_187_629_921_260;
    const SNAPSHOT_TICK_CURRENT_INDEX: i32 = -40_208;
    const SNAPSHOT_LIQUIDITY: u128 = 41_416_473_938;

    fn synthetic_account_bytes() -> Vec<u8> {
        let mut buf = vec![0u8; MIN_ACCOUNT_LEN];
        buf[0..8].copy_from_slice(&WHIRLPOOL_DISCRIMINATOR);
        buf[OFFSET_LIQUIDITY..OFFSET_LIQUIDITY + 16]
            .copy_from_slice(&SNAPSHOT_LIQUIDITY.to_le_bytes());
        buf[OFFSET_SQRT_PRICE..OFFSET_SQRT_PRICE + 16]
            .copy_from_slice(&SNAPSHOT_SQRT_PRICE.to_le_bytes());
        buf[OFFSET_TICK_CURRENT_INDEX..OFFSET_TICK_CURRENT_INDEX + 4]
            .copy_from_slice(&SNAPSHOT_TICK_CURRENT_INDEX.to_le_bytes());
        buf
    }

    #[test]
    fn parse_whirlpool_account_reads_known_fields() {
        let buf = synthetic_account_bytes();
        let state = parse_whirlpool_account(&buf).unwrap();
        assert_eq!(state.liquidity, SNAPSHOT_LIQUIDITY);
        assert_eq!(state.sqrt_price, SNAPSHOT_SQRT_PRICE);
        assert_eq!(state.tick_current_index, SNAPSHOT_TICK_CURRENT_INDEX);
    }

    #[test]
    fn parse_whirlpool_account_rejects_short_data() {
        let short = vec![0u8; 60];
        assert!(parse_whirlpool_account(&short).is_err());
    }

    #[test]
    fn parse_whirlpool_account_rejects_wrong_discriminator() {
        let mut buf = synthetic_account_bytes();
        buf[0] ^= 0xff; // corrupt just the discriminator
        assert!(parse_whirlpool_account(&buf).is_err());
    }
}
