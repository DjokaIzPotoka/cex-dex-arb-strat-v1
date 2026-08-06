//! One-off validation binary for this session's Tier 2 RPC
//! PoolStateProvider three-way cross-check (docs/mev-discovery-plan.md
//! section 3.3). Fetches the same two validation pools' raw account
//! data live via getAccountInfo and runs them through the actual
//! production parsers (clmm.rs, orca.rs) and price math
//! (sqrt_price.rs) via `#[path]`-included sibling modules -- these are
//! the real functions main.rs calls in production, not a
//! reimplementation, so this gives the Python RPC provider's
//! cross-check a genuine independent live reading from this language's
//! side, not just the Rust unit tests' frozen snapshot constants.
//!
//! Not wired into main.rs and not part of the production build path --
//! a standalone bin target for this session's validation only.
//!
//! Usage: cargo run --release --bin live_price_check
//! (reads SOLANA_RPC_URL from the environment, same convention as
//! rpc.rs::build_client; falls back to the public endpoint otherwise.
//! Never prints the URL itself, only its host -- see CLAUDE.md's
//! Environment/security note.)

#[path = "../clmm.rs"]
mod clmm;
#[path = "../orca.rs"]
mod orca;
#[path = "../sqrt_price.rs"]
mod sqrt_price;

use std::str::FromStr;

use anyhow::{Context, Result};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey};

// Same two pools sqrt_price.rs's/orca.rs's/clmm.rs's own doc comments
// were validated against originally.
const KMNO_USDC_WHIRLPOOL: &str = "3ndjN1nJVUKGrJBc1hhVpER6kWTZKHdyDrPyCJyX3CXK";
const TRX_USDC_CLMM: &str = "HpgV2jnzgrGfrZjeZGkHgTnEgRFAhtzCVuk3BRFTFJwk";

fn main() -> Result<()> {
    let url = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
    let host = url.split('/').nth(2).unwrap_or("?");
    println!("rpc_host={host}");

    let client = RpcClient::new_with_commitment(url, CommitmentConfig::confirmed());

    let kmno_pubkey = Pubkey::from_str(KMNO_USDC_WHIRLPOOL).context("bad KMNO pool pubkey")?;
    let kmno_account = client
        .get_account(&kmno_pubkey)
        .context("KMNO/USDC getAccountInfo failed")?;
    let whirlpool = orca::parse_whirlpool_account(&kmno_account.data)?;
    // KMNO is mint_a, USDC is mint_b, both 6 decimals -- per orca.rs's
    // own doc comment (confirmed there against decoded mint addresses).
    let kmno_price = sqrt_price::price_in_quote_per_base(whirlpool.sqrt_price, 6, 6, true);
    println!(
        "KMNO/USDC (Orca Whirlpool {KMNO_USDC_WHIRLPOOL}): sqrt_price={} liquidity={} tick_current_index={} price_native={:.10}",
        whirlpool.sqrt_price, whirlpool.liquidity, whirlpool.tick_current_index, kmno_price
    );

    let trx_pubkey = Pubkey::from_str(TRX_USDC_CLMM).context("bad TRX pool pubkey")?;
    let trx_account = client
        .get_account(&trx_pubkey)
        .context("TRX/USDC getAccountInfo failed")?;
    let pool_state = clmm::parse_clmm_account(&trx_account.data)?;
    // USDC is mint_0, TRX is mint_1, both 6 decimals -- per clmm.rs's
    // own doc comment (confirmed there against Dexscreener).
    let trx_price = sqrt_price::price_in_quote_per_base(pool_state.sqrt_price_x64, 6, 6, false);
    println!(
        "TRX/USDC (Raydium CLMM {TRX_USDC_CLMM}): sqrt_price_x64={} liquidity={} tick_current={} price_native={:.10}",
        pool_state.sqrt_price_x64, pool_state.liquidity, pool_state.tick_current, trx_price
    );

    Ok(())
}
