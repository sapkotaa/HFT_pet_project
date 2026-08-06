#pragma once
#include <cstdint>
#include "types.hpp"   // for Side

#pragma pack(push, 1)

enum class MsgType : uint8_t {
    Logon           = 1,
    Logout          = 2,
    Heartbeat       = 3,
    TestRequest     = 4,
    ResendRequest   = 5,
    NewOrder        = 10,
    CancelOrder     = 11,
    ReplaceOrder    = 12,
    ExecutionReport = 20,
    Reject          = 21,
};

enum class ExecStatus : uint8_t {
    Accepted     = 1,
    Rejected     = 2,
    Filled       = 3,
    PartialFill  = 4,
    Canceled     = 5,
};

struct MsgHeader {
    uint16_t body_length;   // bytes following this header
    MsgType  msg_type;
    uint32_t seq_num;
};

struct LogonMsg {
    uint64_t client_id;
    uint32_t heartbeat_interval_ms;
    uint32_t starting_seq_num;
};

struct HeartbeatMsg {};   // header alone carries all the info needed

struct NewOrderMsg {
    uint64_t client_order_id;
    char     symbol[8];
    Side     side;
    uint32_t price;
    uint32_t quantity;
};

struct CancelOrderMsg {
    uint64_t client_order_id;      // the order being canceled
};

struct ExecutionReportMsg {
    uint64_t   client_order_id;
    uint64_t   exchange_order_id;
    ExecStatus status;
    uint32_t   fill_price;
    uint32_t   fill_quantity;
    uint32_t   leaves_quantity;
};

#pragma pack(pop)