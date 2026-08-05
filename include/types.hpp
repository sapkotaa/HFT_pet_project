#pragma once
#include <cstdint>

enum class Side : uint8_t { Buy, Sell };

using OrderId = uint64_t;
using Price   = int64_t;   // price in integer ticks — never float on the hot path
using Qty     = uint32_t;
using Nanos   = uint64_t;  // timestamp / duration in nanoseconds

struct Order {
    OrderId id    = 0;
    Side    side  = Side::Buy;
    Price   price = 0;
    Qty     qty   = 0;      // remaining (unfilled) quantity
    Nanos   ts_ns = 0;      // time the order was submitted
};

struct Trade {
    OrderId buy_order_id  = 0;
    OrderId sell_order_id = 0;
    Price   price         = 0;
    Qty     qty           = 0;
    Nanos   ts_ns         = 0;
};
