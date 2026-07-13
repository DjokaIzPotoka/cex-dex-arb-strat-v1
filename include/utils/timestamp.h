#pragma once
#include <chrono>
#include <cstdint>

namespace zora::utils {

inline uint64_t now_micros() noexcept {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count());
}

} // namespace zora::utils
