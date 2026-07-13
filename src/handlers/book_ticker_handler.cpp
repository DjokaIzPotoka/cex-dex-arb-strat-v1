#include "handlers/book_ticker_handler.h"
#include "utils/logger.h"
#include "utils/timestamp.h"

#include <cstdlib>
#include <cstring>

namespace zora::handlers {

using zora::utils::LogLevel;

namespace {

// Not std::from_chars: this libc++ only implements the integral overloads,
// not floating-point. strtod on a small stack-copied, null-terminated
// buffer is portable, allocation-free, and doesn't throw.
bool parse_double(std::string_view sv, double& out) noexcept {
    if (sv.empty() || sv.size() >= 32) return false;
    char tmp[32];
    std::memcpy(tmp, sv.data(), sv.size());
    tmp[sv.size()] = '\0';

    char* end = nullptr;
    out = std::strtod(tmp, &end);
    return end != tmp;
}

} // namespace

BookTickerHandler::BookTickerHandler(zora::ipc::ShmWriter& shm)
    : shm_(shm)
{
    // Pre-warm the parser's internal buffers so the first real message
    // doesn't pay an allocation on the hot path.
    auto err = parser_.allocate(kBufCapacity);
    if (err) {
        zora::utils::log_fmt(LogLevel::Error,
            "Failed to preallocate JSON parser buffer: %s", simdjson::error_message(err));
    }
}

void BookTickerHandler::handle(std::string_view raw, uint64_t receive_ts_us) noexcept {
    if (raw.size() > kMaxMessage) {
        zora::utils::log_fmt(LogLevel::Warn,
            "Dropped oversized message (%zu bytes)", raw.size());
        return;
    }

    // Copy into a pre-allocated, padded buffer so simdjson can operate
    // without allocating — the flat_buffer view from the WebSocket layer
    // isn't guaranteed to have simdjson's required SIMD overread padding.
    std::memcpy(json_buf_, raw.data(), raw.size());

    simdjson::ondemand::document doc;
    auto error = parser_.iterate(json_buf_, raw.size(), kBufCapacity).get(doc);
    if (error) {
        zora::utils::log_fmt(LogLevel::Warn, "Parse error: %s",
                              simdjson::error_message(error));
        return;
    }

    std::string_view bid_sv, ask_sv;
    error = doc["b"].get_string().get(bid_sv);
    if (error) return;
    error = doc["a"].get_string().get(ask_sv);
    if (error) return;

    double bid = 0.0, ask = 0.0;
    if (!parse_double(bid_sv, bid) || !parse_double(ask_sv, ask)) {
        zora::utils::log_line(LogLevel::Warn, "Failed to parse bid/ask");
        return;
    }

    uint64_t write_ts_us = zora::utils::now_micros();
    double mid = (bid + ask) * 0.5;

    uint32_t seq = shm_.write(bid, ask, write_ts_us);

    zora::utils::log_fmt(LogLevel::Book,
        "bid=%.5f ask=%.5f mid=%.5f seq=%u", bid, ask, mid, seq);

    uint64_t age_us = write_ts_us - receive_ts_us;
    zora::utils::log_fmt(LogLevel::Shm,
        "Written seq=%u age=%lluus", seq, static_cast<unsigned long long>(age_us));
}

} // namespace zora::handlers
