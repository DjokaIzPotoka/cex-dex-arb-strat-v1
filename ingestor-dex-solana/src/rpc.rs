//! Solana RPC access: fetching the two SPL token vault balances that
//! make up the Raydium SOL/USDC pool's reserves.
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{commitment_config::CommitmentConfig, program_pack::Pack, pubkey::Pubkey};
use spl_token::state::Account as TokenAccount;

use crate::pool::{calculate_price, DexPoolState};

/// Builds the async RPC client. `SOLANA_RPC_URL` lets you point this at
/// Helius or another provider without a code change; falls back to the
/// public mainnet endpoint (fine for development, but rate-limited --
/// switch this for production use).
pub fn build_client() -> RpcClient {
    let url = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
    RpcClient::new_with_commitment(url, CommitmentConfig::confirmed())
}

/// Fetches both vault balances in a single RPC round trip so they're
/// read at the same slot -- two separate calls could observe the pool
/// mid-swap and report a reserve pair that was never simultaneously
/// true on-chain.
///
/// Returns the parsed state alongside two durations:
/// - `network_elapsed`: time spent purely waiting on the round trip
///   (DNS/TLS is amortized via keep-alive, so in practice this is
///   mostly "Solana RPC thinking" + transit). Not under our control --
///   fix by switching provider, not by touching this code.
/// - `parse_elapsed`: time spent unpacking the two SPL token accounts
///   and computing the price, starting the instant the network
///   response bytes are in hand. This is deliberately split out and
///   timed as its own phase (rather than folded into `network_elapsed`
///   or left untimed) so it can be added to the SHM-write time in
///   `main.rs` to get a "code" latency number that starts at the same
///   conceptual point as the CEX ingestor's `age`: bytes received,
///   before any parsing, through to published.
pub async fn fetch_pool_state(
    client: &RpcClient,
    sol_vault: &Pubkey,
    usdc_vault: &Pubkey,
) -> Result<(DexPoolState, Duration, Duration)> {
    let network_start = Instant::now();
    let response = client
        .get_multiple_accounts_with_commitment(
            &[*sol_vault, *usdc_vault],
            CommitmentConfig::confirmed(),
        )
        .await
        .context("get_multiple_accounts RPC call failed")?;
    let network_elapsed = network_start.elapsed();

    let parse_start = Instant::now();

    let slot = response.context.slot;
    let mut accounts = response.value.into_iter();

    let sol_account = accounts
        .next()
        .flatten()
        .context("SOL vault account not found")?;
    let usdc_account = accounts
        .next()
        .flatten()
        .context("USDC vault account not found")?;

    let reserve_sol = TokenAccount::unpack(&sol_account.data)
        .context("failed to unpack SOL vault token account")?
        .amount;
    let reserve_usdc = TokenAccount::unpack(&usdc_account.data)
        .context("failed to unpack USDC vault token account")?
        .amount;

    let price = calculate_price(reserve_sol, reserve_usdc);

    let parse_elapsed = parse_start.elapsed();

    Ok((
        DexPoolState {
            reserve_sol,
            reserve_usdc,
            price,
            slot,
        },
        network_elapsed,
        parse_elapsed,
    ))
}
