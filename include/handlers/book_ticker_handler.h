#pragma once
#include "ipc/shm_writer.h"
#include <simdjson.h>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace zora::handlers {

// A single pair this handler routes to: the Binance ticker symbol
// (e.g. "SOLUSDC", as it appears in the combined stream's "data"."s"
// field), the human-readable label used in logs (e.g. "SOL/USDC"),
// and the shared memory segment that pair's prices get written to.
struct PairRoute {
    const char* binance_symbol; // "SOLUSDC"
    const char* pair_label;     // "SOL/USDC"
    zora::ipc::ShmWriter* shm;  // pointer, not reference: PairRoute
                                // lives in a fixed-size array member,
                                // which needs default-construct +
                                // copy-assign -- references can't do
                                // either.
};

// Parses bookTicker messages from Binance's *combined* stream (each
// message wrapped as {"stream":"...","data":{...}}) and routes each to
// the right shared memory segment based on the embedded "data"."s"
// field. One handler serves every subscribed pair -- there's only one
// WebSocket connection.
class BookTickerHandler {
public:
    explicit BookTickerHandler(std::initializer_list<PairRoute> routes);

    // Hot path: parse a raw combined-stream JSON message and publish
    // it to the correct pair's shared memory. No heap allocation, no
    // exceptions.
    void handle(std::string_view raw, uint64_t receive_ts_us) noexcept;

private:
    static constexpr std::size_t kMaxRoutes = 8; // 5 pairs currently, headroom for 3 more
    static constexpr std::size_t kMaxMessage = 1024;
    static constexpr std::size_t kBufCapacity = kMaxMessage + simdjson::SIMDJSON_PADDING;

    PairRoute routes_[kMaxRoutes];
    std::size_t route_count_ = 0;
    simdjson::ondemand::parser parser_;
    char json_buf_[kBufCapacity];
};

} // namespace zora::handlers
