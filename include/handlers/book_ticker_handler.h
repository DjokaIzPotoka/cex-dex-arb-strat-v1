#pragma once
#include "ipc/shm_writer.h"
#include <simdjson.h>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace zora::handlers {

// Parses solusdc@bookTicker messages and publishes them to shared memory.
class BookTickerHandler {
public:
    explicit BookTickerHandler(zora::ipc::ShmWriter& shm);

    // Hot path: parse a raw bookTicker JSON message and publish it to
    // shared memory. No heap allocation, no exceptions.
    void handle(std::string_view raw, uint64_t receive_ts_us) noexcept;

private:
    static constexpr std::size_t kMaxMessage = 1024;
    static constexpr std::size_t kBufCapacity = kMaxMessage + simdjson::SIMDJSON_PADDING;

    zora::ipc::ShmWriter& shm_;
    simdjson::ondemand::parser parser_;
    char json_buf_[kBufCapacity];
};

} // namespace zora::handlers
