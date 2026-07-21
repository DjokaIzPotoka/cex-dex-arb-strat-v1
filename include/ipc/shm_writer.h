#pragma once
#include "zora_ipc.h"
#include <cstdint>

namespace zora::ipc {

// Owns the lifecycle of a POSIX shared memory segment used to publish a
// CEX price (SOL/USDC, RAY/USDC, ...) to the Rust arbitrage bot. One
// instance per trading pair -- `shm_name` (e.g. ZORA_SHM_CEX_RAY_NAME)
// is what makes each instance independent.
class ShmWriter {
public:
    ShmWriter() = default;
    ~ShmWriter();

    ShmWriter(const ShmWriter&) = delete;
    ShmWriter& operator=(const ShmWriter&) = delete;

    // Creates, sizes, and maps the shared memory segment named
    // `shm_name` (a string literal is expected -- the pointer is kept,
    // not copied, since shutdown() needs it again later). `symbol` is
    // the human-readable ticker written into the struct's own
    // `symbol` field (e.g. "RAYUSDC"), independent of the OS-level
    // segment name. Must be called once at startup, before the
    // WebSocket connects. Returns false on failure (caller should
    // treat this as fatal).
    bool init(const char* shm_name, const char* symbol) noexcept;

    // Hot path: writes bid/ask/mid/timestamp and publishes the update with
    // a release store on `sequence`. Returns the sequence number written.
    // No allocation, no exceptions.
    uint32_t write(double bid, double ask, uint64_t timestamp_us) noexcept;

    // Called when the WebSocket reconnects, to signal a data gap to the
    // reader before the next real update arrives.
    void signal_gap() noexcept;

    // Unmaps and unlinks the segment. Safe to call multiple times.
    void shutdown() noexcept;

private:
    ZoraCexPrice* shm_ = nullptr;
    int fd_ = -1;
    const char* shm_name_ = nullptr;
    bool owns_segment_ = false;
    uint32_t seq_counter_ = 0;
};

} // namespace zora::ipc
