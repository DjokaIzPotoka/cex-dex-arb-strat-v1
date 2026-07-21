//! Solana RPC access: fetching the SPL token vault balances that make
//! up every Raydium pool's reserves (SOL/USDC, RAY/USDC, ...) in a
//! single batched RPC call.
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::account::Account;
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

/// One pool's vault pair plus the base token's mint decimals -- kept
/// together since `calculate_price` needs `base_decimals` and there's
/// no other natural place to pair it with the right vaults once every
/// pool's accounts are flattened into a single request below.
pub struct PoolVaults {
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub base_decimals: u32,
}

/// Unpacks one pool's pair of accounts (already fetched) into a
/// priced `DexPoolState`. Split out from `fetch_all_pool_states` so
/// `?` only ever aborts *this one pool's* parsing, not the whole
/// batch: a bad/missing account for one pool must not discard another
/// pool's already-successfully-parsed state.
fn parse_pool_state(
    base_account: Option<Account>,
    quote_account: Option<Account>,
    base_decimals: u32,
    slot: u64,
) -> Result<DexPoolState> {
    let base_account = base_account.context("base vault account not found")?;
    let quote_account = quote_account.context("quote (USDC) vault account not found")?;

    let reserve_sol = TokenAccount::unpack(&base_account.data)
        .context("failed to unpack base vault token account")?
        .amount;
    let reserve_usdc = TokenAccount::unpack(&quote_account.data)
        .context("failed to unpack quote vault token account")?
        .amount;

    let price = calculate_price(reserve_sol, reserve_usdc, base_decimals);

    Ok(DexPoolState {
        reserve_sol,
        reserve_usdc,
        price,
        slot,
    })
}

/// Fetches every pool's vault balances in a *single* RPC round trip --
/// merged from what used to be one `get_multiple_accounts` call per
/// pool (two calls per polling cycle for SOL/USDC + RAY/USDC), to
/// halve the request rate against the public RPC endpoint. It
/// throttles per-request, not per-connection, so two sequential calls
/// every cycle were sporadically hitting a ~15s timeout on whichever
/// call landed second; one combined call is charged as a single
/// request instead of two.
///
/// All pools are read at the same slot, same as the old per-pool
/// version was for its one pool's two vaults -- now extended to every
/// pool in the batch simultaneously, which is a small additional
/// consistency benefit of merging the calls.
///
/// A network-level failure (timeout, connection refused) necessarily
/// fails the whole batch -- it's one physical HTTP request, so there's
/// no such thing as "SOL/USDC's half timed out but RAY/USDC's didn't".
/// That's the `Result` this function itself returns. Once the network
/// round trip *has* succeeded, though, each pool's own account data is
/// parsed independently: one pool having a bad/missing account
/// returns `Err` only for that pool's entry in the returned `Vec`,
/// leaving every other pool's successfully-parsed state intact --
/// preserving the old per-pool error isolation for anything past the
/// network step.
///
/// Returns one `Result<DexPoolState>` per entry in `pools`, in the
/// same order, alongside two durations:
/// - `network_elapsed`: time spent purely waiting on the round trip.
///   Not under our control -- fix by switching provider, not by
///   touching this code.
/// - `parse_elapsed`: time spent unpacking every pool's two SPL token
///   accounts and computing each price, starting the instant the
///   network response bytes are in hand. Split out (rather than
///   folded into `network_elapsed`) so the caller can add it to the
///   SHM-write time to get a "code" latency number comparable to the
///   CEX ingestor's `age` -- unchanged in spirit from the single-pool
///   version, just covering every pool's parse work in one measured
///   span instead of one pool's.
pub async fn fetch_all_pool_states(
    client: &RpcClient,
    pools: &[PoolVaults],
) -> Result<(Vec<Result<DexPoolState>>, Duration, Duration)> {
    let mut vault_keys = Vec::with_capacity(pools.len() * 2);
    for pool in pools {
        vault_keys.push(pool.base_vault);
        vault_keys.push(pool.quote_vault);
    }

    let network_start = Instant::now();
    let response = client
        .get_multiple_accounts_with_commitment(&vault_keys, CommitmentConfig::confirmed())
        .await
        .context("get_multiple_accounts RPC call failed")?;
    let network_elapsed = network_start.elapsed();

    let parse_start = Instant::now();

    let slot = response.context.slot;
    let mut accounts = response.value.into_iter();

    let states = pools
        .iter()
        .map(|pool| {
            let base_account = accounts.next().flatten();
            let quote_account = accounts.next().flatten();
            parse_pool_state(base_account, quote_account, pool.base_decimals, slot)
        })
        .collect();

    let parse_elapsed = parse_start.elapsed();

    Ok((states, network_elapsed, parse_elapsed))
}
