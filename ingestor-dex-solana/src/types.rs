//! Shared-memory wire format for the DEX side of the ZORA arbitrage
//! system. Must stay byte-for-byte compatible with what the spread
//! detector (a separate process, not yet built) expects to read.

/// Layout mirrors the C++ `ZoraCexPrice` struct in
/// `shared/include/zora_ipc.h` on the CEX-ingestor side: fixed-size,
/// `#[repr(C)]`, no pointers or Rust-specific types, so any other
/// process mapping this same memory region interprets the bytes
/// identically regardless of language.
///
/// Field order is deliberate and must not be reordered casually: it's
/// part of the cross-process contract. Note there are 4 padding bytes
/// after `sequence` (a u32 sandwiched before a u64 forces the compiler
/// to align `slot` to 8 bytes) -- see the `layout_matches_expected_size`
/// test below.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ZoraDexPrice {
    pub reserve_sol: u64,  // lamports (9 decimals)
    pub reserve_usdc: u64, // micro-USDC (6 decimals)
    pub price: f64,        // usdc / sol, normalized
    pub timestamp_us: u64, // microseconds since epoch
    pub sequence: u32,     // incremented on every update -- see shm.rs
                           // for why this is written atomically even
                           // though the field itself is a plain u32
    pub slot: u64,         // Solana slot this snapshot came from
}

/// POSIX shared memory segment names the spread detector reads from.
/// SOL/USDC keeps its original name unchanged; RAY/USDC, BONK/USDC,
/// ZEC/USDC, and UNI/USDC are new.
pub const ZORA_SHM_DEX_NAME: &str = "/zora_dex_price";
pub const ZORA_SHM_DEX_RAY_NAME: &str = "/zora_dex_ray";
pub const ZORA_SHM_DEX_BONK_NAME: &str = "/zora_dex_bonk";
pub const ZORA_SHM_DEX_ZEC_NAME: &str = "/zora_dex_zec";
pub const ZORA_SHM_DEX_UNI_NAME: &str = "/zora_dex_uni";

#[cfg(test)]
mod tests {
    use super::*;

    // A layout regression here would silently break the cross-process
    // contract (every reader assumes this exact byte layout), so this
    // pins the actual verified size/alignment rather than leaving it
    // implicit.
    #[test]
    fn layout_matches_expected_size() {
        assert_eq!(std::mem::size_of::<ZoraDexPrice>(), 48);
        assert_eq!(std::mem::align_of::<ZoraDexPrice>(), 8);
    }
}
