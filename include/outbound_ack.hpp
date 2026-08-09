#pragma once
#include <cstdint>
#include <type_traits>
#include "gateway_protocol.hpp"   // ExecStatus
#include "spsc_ring_buffer.hpp"

// Produced by MatchingEngineConsumer (matcher thread), consumed by
// GatewayServer::poll_once() (gateway/main thread) and turned into a
// wire ExecutionReportMsg. Mirrors ExecutionReportMsg's fields plus the
// session_id (fd) needed to route it back to the right connection.
struct OutboundAck {
    uint32_t   session_id;
    uint64_t   client_order_id;
    uint64_t   exchange_order_id;   // 0 if it never reached the book (unknown-order cancel reject)
    ExecStatus status;
    uint32_t   fill_price;
    uint32_t   fill_quantity;
    uint32_t   leaves_quantity;
};
static_assert(std::is_trivially_copyable_v<OutboundAck>);

using AckQueue = SpscRingBuffer<OutboundAck, 1u << 16>;
