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

BookTickerHandler::BookTickerHandler(std::initializer_list<PairRoute> routes) {
    for (const auto& r : routes) {
        if (route_count_ >= kMaxRoutes) break;
        routes_[route_count_++] = r;
    }

    // Pre-warm the parser's internal buffers so the first real message
    // doesn't pay an allocation on the hot path.
    auto err = parser_.allocate(kBufCapacity);
    if (err) {
        char err_buf[64];
        std::snprintf(err_buf, sizeof(err_buf), "%s", simdjson::error_message(err));
        zora::utils::log(LogLevel::Error, {{"parser", "alloc_failed"}, {"reason", err_buf}});
    }
}

void BookTickerHandler::handle(std::string_view raw, uint64_t receive_ts_us) noexcept {
    if (raw.size() > kMaxMessage) {
        char size_buf[16];
        std::snprintf(size_buf, sizeof(size_buf), "%zu", raw.size());
        zora::utils::log(LogLevel::Warn, {{"dropped", "oversized_message"}, {"bytes", size_buf}});
        return;
    }

    // Copy into a pre-allocated, padded buffer so simdjson can operate
    // without allocating — the flat_buffer view from the WebSocket layer
    // isn't guaranteed to have simdjson's required SIMD overread padding.
    std::memcpy(json_buf_, raw.data(), raw.size());

    simdjson::ondemand::document doc;
    auto error = parser_.iterate(json_buf_, raw.size(), kBufCapacity).get(doc);
    if (error) {
        char err_buf[64];
        std::snprintf(err_buf, sizeof(err_buf), "%s", simdjson::error_message(error));
        zora::utils::log(LogLevel::Warn, {{"parse", "failed"}, {"reason", err_buf}});
        return;
    }

    // Binance's *combined* stream (required to subscribe to more than
    // one ticker over a single connection) wraps every message as
    // {"stream":"solusdc@bookTicker","data":{"u":...,"s":"SOLUSDC",
    // "b":"...","B":"...","a":"...","A":"..."}} -- unlike the single-
    // stream endpoint, the actual bookTicker fields are nested one
    // level down under "data", not at the top level. Materializing
    // "data" once and indexing it multiple times (below) is required
    // here: simdjson's on-demand API is forward-only, and re-indexing
    // doc["data"] a second time from the top-level document wouldn't
    // work the way re-indexing the same nested object does.
    simdjson::ondemand::object data;
    error = doc["data"].get_object().get(data);
    if (error) {
        zora::utils::log(LogLevel::Warn, {{"parse", "failed"}, {"reason", "no_data_field"}});
        return;
    }

    std::string_view symbol_sv;
    error = data["s"].get_string().get(symbol_sv);
    if (error) return;

    const PairRoute* route = nullptr;
    for (std::size_t i = 0; i < route_count_; ++i) {
        if (symbol_sv == routes_[i].binance_symbol) {
            route = &routes_[i];
            break;
        }
    }
    if (route == nullptr) return; // unknown/unsubscribed symbol, ignore defensively

    std::string_view bid_sv, ask_sv;
    error = data["b"].get_string().get(bid_sv);
    if (error) return;
    error = data["a"].get_string().get(ask_sv);
    if (error) return;

    double bid = 0.0, ask = 0.0;
    if (!parse_double(bid_sv, bid) || !parse_double(ask_sv, ask)) {
        zora::utils::log(LogLevel::Warn, {{"parse", "failed"}, {"reason", "bid_ask"}});
        return;
    }

    uint64_t write_ts_us = zora::utils::now_micros();
    double mid = (bid + ask) * 0.5;

    uint32_t seq = route->shm->write(bid, ask, write_ts_us);
    uint64_t age_us = write_ts_us - receive_ts_us;

    // One line per tick, matching the DEX ingestor's merged
    // price/reserves/timing line: everything about this update in a
    // single structured log call rather than split across two.
    // 8 decimals, not 3: BONK/USDC's price (~0.0000031) needs it -- 3
    // decimals would print "0.000" for every BONK tick. Harmless extra
    // trailing digits for the larger-priced pairs; buffers sized with
    // headroom for the wider output.
    char bid_buf[24], ask_buf[24], mid_buf[24], seq_buf[16], age_buf[24];
    std::snprintf(bid_buf, sizeof(bid_buf), "%.8f", bid);
    std::snprintf(ask_buf, sizeof(ask_buf), "%.8f", ask);
    std::snprintf(mid_buf, sizeof(mid_buf), "%.8f", mid);
    std::snprintf(seq_buf, sizeof(seq_buf), "%u", seq);
    std::snprintf(age_buf, sizeof(age_buf), "%lluus", static_cast<unsigned long long>(age_us));

    zora::utils::log(LogLevel::Info, {
        {"pair", route->pair_label},
        {"bid", bid_buf}, {"ask", ask_buf}, {"mid", mid_buf},
        {"seq", seq_buf}, {"age", age_buf}
    });
}

} // namespace zora::handlers
