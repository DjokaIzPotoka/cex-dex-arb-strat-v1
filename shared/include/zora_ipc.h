#pragma once
#include <stdint.h>

// <stdatomic.h>'s function-like macros (atomic_store/atomic_load/...)
// collide with libc++'s <atomic> the moment both end up in the same
// translation unit (e.g. via simdjson) — so C++ uses std::atomic<uint32_t>
// here instead. It is layout- and lock-free-compatible with C11's
// _Atomic uint32_t (and with Rust's AtomicU32): all are a plain 4-byte
// aligned uint32_t under the hood on every mainstream platform.
#ifdef __cplusplus
#include <atomic>
#define ZORA_ATOMIC_U32 std::atomic<uint32_t>
#else
#include <stdatomic.h>
#define ZORA_ATOMIC_U32 _Atomic uint32_t
#endif

#define ZORA_SHM_CEX_NAME "/zora_cex_price"      // SOL/USDC (unchanged)
#define ZORA_SHM_CEX_RAY_NAME "/zora_cex_ray"    // RAY/USDC
#define ZORA_SHM_CEX_BONK_NAME "/zora_cex_bonk"  // BONK/USDC
#define ZORA_SHM_CEX_ZEC_NAME "/zora_cex_zec"    // ZEC/USDC
#define ZORA_SHM_CEX_UNI_NAME "/zora_cex_uni"    // UNI/USDC
#define ZORA_SHM_SIZE sizeof(ZoraCexPrice)

typedef struct {
    double bid;
    double ask;
    double mid;
    uint64_t timestamp_us;      // microseconds since epoch
    ZORA_ATOMIC_U32 sequence;   // incremented on every update
    char symbol[16];            // "SOLUSDC\0"
} ZoraCexPrice;
