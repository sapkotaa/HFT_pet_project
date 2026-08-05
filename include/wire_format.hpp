#pragma once
#include <cstdint>
#include "types.hpp"

// A deliberately explicit, packed wire format — this is what actually
// goes on the network, as opposed to `Order`, which is free to have
// compiler-dependent padding since it only ever lives in memory.
// Real market data protocols (ITCH, FIX/binary) all do this: the wire
// layout is a contract, independent of any single language's struct
// layout rules.

#pragma pack(push,1)
struct WireOrder {
    uint64_t id = 0;
    uint8_t side = 0;
    int64_t price = 0;
    uint32_t qty = 0;
    uint64_t ts_ns = 0; // sender-side timestamp:information only-
    // the subscriber measures its own receive time

};
#pragma pack(pop)

inline WireOrder to_wire(const Order& o){
    return WireOrder{o.id,static_cast<uint8_t>(o.side),o.price,o.qty,o.ts_ns};

}
inline Order from_wire (const WireOrder& w){
    Order o;
    o.id = w.id;
    o.side = w.side == 0 ? Side::Buy : Side::Sell;
    o.price = w.price;
    o.qty = w.qty;
    o.ts_ns = w.ts_ns;
    return o;
}


