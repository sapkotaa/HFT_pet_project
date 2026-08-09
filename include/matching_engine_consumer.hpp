#pragma once
#include <atomic>
#include <cstdint>
#include <functional>
#include <thread>
#include <unordered_map>
#include <vector>
#include "broadcast_ring.hpp"
#include "event.hpp"
#include "order_book.hpp"
#include "outbound_ack.hpp"
#include "sequencer.hpp"   // OutputRing typedef
#include "types.hpp"

// One of (potentially several) independent consumers of the sequencer's
// BroadcastRing. Owns the OrderBook — the matching engine's state lives
// here and nowhere else. Applies events strictly in seq_num order,
// which is what makes replaying a recorded stream reproduce an
// identical book.
//
// Order-id scheme: SequencedEvent::client_order_id is only unique
// *within a session* (different clients can and do reuse the same
// values), but OrderBook::OrderId must be globally unique within this
// one OrderBook instance. Fix: use ev.seq_num — already globally
// unique and monotonic, stamped once per event by the single-writer
// Sequencer — as the exchange-side OrderId for NewOrder events. Cancel
// events never mint an id; they resolve the *existing* order via
// by_client_ below.
struct OrderKey {
    uint32_t session_id;
    uint64_t client_order_id;
    bool operator==(const OrderKey&) const = default;
};
struct OrderKeyHash {
    size_t operator()(const OrderKey& k) const noexcept {
        return std::hash<uint64_t>()(k.client_order_id) ^
               (std::hash<uint32_t>()(k.session_id) * 0x9E3779B97F4A7C15ULL);
    }
};
struct RestingOrderInfo {
    uint32_t session_id;
    uint64_t client_order_id;
    Qty      remaining_qty;
};

template <size_t PoolCapacity = 1 << 20>
class MatchingEngineConsumer {
public:
    MatchingEngineConsumer(OutputRing& ring, AckQueue& acks)
        : ring_(ring), acks_(acks), consumer_id_(ring.register_consumer()) {}

    void start() {
        running_.store(true, std::memory_order_release);
        thread_ = std::thread([this] { run(); });
    }

    // Precondition: the Sequencer (this ring's sole producer) has
    // already been stop()'d and joined. Only then does "ring caught up"
    // unambiguously mean "no more events are ever coming," which is
    // what lets the final drain below run to completion instead of
    // racing a still-live producer.
    void stop() {
        running_.store(false, std::memory_order_release);
        if (thread_.joinable()) thread_.join();
    }

    // Directly callable, no thread required — this is the seam that
    // makes the order-id-mapping / cancel-resolution logic testable in
    // isolation (see tests/test_matching_engine_consumer.cpp).
    void apply(const SequencedEvent& ev) {
        switch (ev.type) {
            case EventType::NewOrder:    apply_new_order(ev); break;
            case EventType::CancelOrder: apply_cancel_order(ev); break;
            default: break;   // MarketDataOrder / Admin: no producer feeds these yet
        }
    }

    size_t resting_orders() const { return book_.resting_orders(); }
    uint64_t acks_dropped() const { return acks_dropped_; }

private:
    void run() {
        while (running_.load(std::memory_order_acquire)) {
            bool did_work = false;
            SequencedEvent ev;
            for (int i = 0; i < 32 && ring_.try_consume(consumer_id_, ev); ++i) {
                apply(ev);
                did_work = true;
            }
            if (!did_work) cpu_pause();
        }
        SequencedEvent ev;
        while (ring_.try_consume(consumer_id_, ev)) apply(ev);
    }

    void apply_new_order(const SequencedEvent& ev) {
        const OrderId exchange_id = ev.seq_num;
        trades_.clear();
        book_.add_limit_order(exchange_id, ev.side, static_cast<Price>(ev.price),
                               ev.quantity, ev.sequence_ts_ns, trades_);

        Qty filled_on_incoming = 0;
        for (const auto& t : trades_) {
            const bool incoming_is_buy = (t.buy_order_id == exchange_id);
            filled_on_incoming += t.qty;

            // The other leg of this trade is a resting order placed by
            // some *other* session earlier — recover its owner and
            // remaining qty so it gets its own fill ack (it doesn't
            // otherwise learn its order was hit; only the incoming
            // order's session is present in `ev`).
            const OrderId counterparty = incoming_is_buy ? t.sell_order_id : t.buy_order_id;
            auto it = resting_.find(counterparty);
            if (it == resting_.end()) continue;   // defensive; shouldn't happen
            auto& info = it->second;
            info.remaining_qty -= t.qty;
            const bool done = (info.remaining_qty == 0);
            push_ack({info.session_id, info.client_order_id, counterparty,
                      done ? ExecStatus::Filled : ExecStatus::PartialFill,
                      static_cast<uint32_t>(t.price), t.qty, info.remaining_qty});
            if (done) {
                by_client_.erase({info.session_id, info.client_order_id});
                resting_.erase(it);
            }
        }

        const Qty leaves = ev.quantity - filled_on_incoming;
        if (leaves > 0) {
            by_client_[{ev.session_id, ev.client_order_id}] = exchange_id;
            resting_[exchange_id] = {ev.session_id, ev.client_order_id, leaves};
        }
        ExecStatus status = (filled_on_incoming == 0) ? ExecStatus::Accepted
                           : (leaves == 0)             ? ExecStatus::Filled
                                                        : ExecStatus::PartialFill;
        // One aggregated ack for the incoming order even if it crossed
        // multiple price levels/trades, using the last trade's price —
        // documented simplification; a real venue sends one report per fill.
        const uint32_t last_px = trades_.empty() ? 0 : static_cast<uint32_t>(trades_.back().price);
        push_ack({ev.session_id, ev.client_order_id, exchange_id, status,
                  last_px, filled_on_incoming, leaves});
    }

    void apply_cancel_order(const SequencedEvent& ev) {
        auto it = by_client_.find({ev.session_id, ev.client_order_id});
        if (it == by_client_.end()) {
            push_ack({ev.session_id, ev.client_order_id, 0, ExecStatus::Rejected, 0, 0, 0});
            return;
        }
        const OrderId exchange_id = it->second;
        const bool removed = book_.cancel(exchange_id);
        by_client_.erase(it);
        resting_.erase(exchange_id);
        push_ack({ev.session_id, ev.client_order_id, exchange_id,
                  removed ? ExecStatus::Canceled : ExecStatus::Rejected, 0, 0, 0});
    }

    // Bounded spin, not unconditional: during shutdown's final drain,
    // nobody may be left draining AckQueue (the gateway's poll_once
    // loop has already exited), so an unconditional spin here — mirroring
    // Sequencer's own "spin, never drop" backpressure policy — could hang
    // stop()'s join() forever if the queue happened to be full at that
    // exact moment. Overflow is dropped and counted instead; on the hot
    // path (normal draining, queue capacity 65536) this cap is never
    // remotely approached.
    void push_ack(const OutboundAck& ack) {
        constexpr int kMaxSpins = 10'000'000;
        for (int spins = 0; !acks_.push(ack); ++spins) {
            if (spins > kMaxSpins) { ++acks_dropped_; return; }
            cpu_pause();
        }
    }

    static void cpu_pause() {
    #if defined(__x86_64__)
        __builtin_ia32_pause();
    #elif defined(__aarch64__)
        asm volatile("yield");
    #endif
    }

    OrderBook<PoolCapacity> book_;
    OutputRing& ring_;
    AckQueue&   acks_;
    size_t      consumer_id_;
    std::unordered_map<OrderKey, OrderId, OrderKeyHash> by_client_;
    std::unordered_map<OrderId, RestingOrderInfo>        resting_;
    std::vector<Trade> trades_;
    uint64_t    acks_dropped_ = 0;
    std::atomic<bool> running_{false};
    std::thread thread_;
};
