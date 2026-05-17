#ifndef ZORE_CORE_TYPES_H
#define ZORE_CORE_TYPES_H

#include <string>
#include <cstdint>

//uvek dobijamo 64bit int
struct Trade {
    std::string symbol;
    std::string side;
    double      price;
    double      amount;      // bilo je quantity, menjamo u amount
    int64_t     trade_id;
    int64_t     timestamp;   // exchange time (ns)
    int64_t     receive_ts;  // naš timestamp (ns)
};

#endif //ZORE_CORE_TYPES_H