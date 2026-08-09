#pragma once
#include <cstdint>
#include <type_traits>
#include "types.hpp"

enum class EventType : uint8_t {
    NewOrder        = 1,
    CancelOrder     = 2,
    MarketDataOrder = 3,   // from feed replay / ITCH later
    Admin           = 4,   // kill switch, config, etc.
};

enum class ProducerId : uint8_t {
    Gateway    = 0,
    FeedReplay = 1,
    Admin      = 2,
    Count      = 3,
};

// Fixed-size POD: trivially copyable into the ring, and writable
// straight to the WAL with no serialization step.
struct SequencedEvent {
    uint64_t   seq_num;          // stamped by sequencer, global total order
    uint64_t   ingress_ts_ns;    // stamped by producer on arrival
    uint64_t   sequence_ts_ns;   // stamped by sequencer
    EventType  type;
    ProducerId producer;
    uint32_t   session_id;       // which gateway connection, for routing acks back
    uint64_t   client_order_id;
    Side       side;
    uint32_t   price;
    uint32_t   quantity;
};
static_assert(std::is_trivially_copyable_v<SequencedEvent>);