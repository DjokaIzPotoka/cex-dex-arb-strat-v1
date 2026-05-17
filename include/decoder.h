#ifndef ZORA_TRADE_DECODER_H
#define ZORA_TRADE_DECODER_H

#include "types.h"
#include <simdjson.h>
#include <optional>
#include <string>

class TradeDecoder {
public:
    TradeDecoder();
    std::optional<Trade> decode(const std::string& raw_json);

private:
    simdjson::ondemand::parser parser_;
};

#endif // ZORA_TRADE_DECODER_H