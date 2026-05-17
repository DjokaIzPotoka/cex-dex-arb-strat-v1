#include "decoder.h"
#include <chrono>
#include <iostream>

TradeDecoder::TradeDecoder() = default;

std::optional<Trade> TradeDecoder::decode(const std::string& raw_json) {
    try {
        simdjson::padded_string padded(raw_json);
        auto doc = parser_.iterate(padded);

        Trade t;

        // receive_ts — naš timestamp
        t.receive_ts = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()
        ).count();

        std::string_view event_type = doc["e"].get_string().value();
        if (event_type != "trade") return std::nullopt;

        t.symbol = std::string(doc["s"].get_string().value());

        // Side: false = buy aggressor, true = sell aggressor
        t.side = doc["m"].get_bool().value() ? "sell" : "buy";

        t.price  = std::stod(std::string(doc["p"].get_string().value()));
        t.amount = std::stod(std::string(doc["q"].get_string().value()));

        t.trade_id = doc["t"].get_int64().value();

        // Exchange timestamp
        t.timestamp = doc["T"].get_int64().value() * 1'000'000LL;

        return t;

    } catch (const std::exception& e) {
        std::cerr << "[Decoder] Greska: " << e.what() << "\n";
        return std::nullopt;
    }
}