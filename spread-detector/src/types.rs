//! Shared-memory wire structs (mirroring the CEX and DEX ingestors'
//! real, verified layouts) plus the `Opportunity`/`Direction` types
//! this process produces.

/// Mirrors `shared/include/zora_ipc.h`'s `ZoraCexPrice` C struct
/// byte-for-byte. Verified via a real compile (`sizeof`/`offsetof`):
/// 56 bytes total, not the 52 you'd get from just summing field sizes
/// -- there are 4 trailing padding bytes after `symbol[16]` so the
/// struct's total size is a multiple of the 8-byte alignment its
/// doubles require.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ZoraCexPrice {
    pub bid: f64,
    pub ask: f64,
    pub mid: f64,
    pub timestamp_us: u64,
    pub sequence: u32,
    #[allow(dead_code)] // never read; must exist for correct layout/size
    pub symbol: [u8; 16],
}

/// Mirrors `ingestor-dex-solana/src/types.rs`'s `ZoraDexPrice` exactly
/// -- duplicated here rather than imported: that crate has no library
/// target (bin-only) and is deliberately kept outside this workspace
/// (see the root Cargo.toml), so small shared pieces like this struct
/// are independently authored on each side to match the same byte
/// layout, the same way the CEX struct above matches the C++ side
/// without any FFI binding between them. Keep both copies in sync by
/// hand if either side's layout ever changes.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ZoraDexPrice {
    pub reserve_sol: u64,
    pub reserve_usdc: u64,
    pub price: f64,
    pub timestamp_us: u64,
    pub sequence: u32,
    pub slot: u64,
}

// SOL/USDC segment names are unchanged; RAY/USDC's, BONK/USDC's,
// ZEC/USDC's, TRX/USDC's, and KMNO/USDC's are new (same naming choice
// made on both ingestors, so this reads symmetrically). UNI/USDC used
// to have a pair here too but was removed -- no viable pool exists for
// it on either Raydium or Orca.
pub const ZORA_SHM_CEX_NAME: &str = "/zora_cex_price";
pub const ZORA_SHM_DEX_NAME: &str = "/zora_dex_price";
pub const ZORA_SHM_CEX_RAY_NAME: &str = "/zora_cex_ray";
pub const ZORA_SHM_DEX_RAY_NAME: &str = "/zora_dex_ray";
pub const ZORA_SHM_CEX_BONK_NAME: &str = "/zora_cex_bonk";
pub const ZORA_SHM_DEX_BONK_NAME: &str = "/zora_dex_bonk";
pub const ZORA_SHM_CEX_ZEC_NAME: &str = "/zora_cex_zec";
pub const ZORA_SHM_DEX_ZEC_NAME: &str = "/zora_dex_zec";
pub const ZORA_SHM_CEX_TRX_NAME: &str = "/zora_cex_trx";
pub const ZORA_SHM_DEX_TRX_NAME: &str = "/zora_dex_trx";
pub const ZORA_SHM_CEX_KMNO_NAME: &str = "/zora_cex_kmno";
pub const ZORA_SHM_DEX_KMNO_NAME: &str = "/zora_dex_kmno";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// DEX is cheaper: buy SOL there, sell it on CEX.
    BuyDexSellCex,
    /// CEX is cheaper: buy SOL there, sell it on DEX. Not implemented
    /// yet -- the detector only ever produces `BuyDexSellCex` for now.
    #[allow(dead_code)]
    BuyCexSellDex,
}

impl Direction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Direction::BuyDexSellCex => "BUY_DEX_SELL_CEX",
            Direction::BuyCexSellDex => "BUY_CEX_SELL_DEX",
        }
    }
}

/// A detected, sized arbitrage opportunity, ready to log and record.
/// Paper-mode only: nothing in this process ever submits a real order.
#[derive(Debug, Clone, Copy)]
pub struct Opportunity {
    pub pair: &'static str, // "SOL/USDC", "RAY/USDC"
    pub cex_bid: f64,
    pub dex_price: f64,
    pub gross_spread: f64, // fraction, e.g. 0.00198 for 0.198%
    pub net_spread: f64,   // fraction, after total_costs_pct

    pub optimal_size_usdc: f64,
    #[allow(dead_code)] // part of the spec'd struct; not one of the CSV columns
    pub expected_sol_out: f64,
    pub estimated_profit: f64, // USD

    pub direction: Direction,

    pub detected_at_us: u64,
    pub cex_seq: u32,
    pub dex_seq: u32,
    pub dex_slot: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // A layout regression here would silently break reads from either
    // ingestor's shared memory -- wrong reserves/prices with no crash
    // to signal it, which is the worst failure mode for this process.
    #[test]
    fn cex_struct_matches_verified_cpp_layout() {
        assert_eq!(std::mem::size_of::<ZoraCexPrice>(), 56);
        assert_eq!(std::mem::align_of::<ZoraCexPrice>(), 8);
    }

    #[test]
    fn dex_struct_matches_existing_ingestor_layout() {
        assert_eq!(std::mem::size_of::<ZoraDexPrice>(), 48);
        assert_eq!(std::mem::align_of::<ZoraDexPrice>(), 8);
    }
}
