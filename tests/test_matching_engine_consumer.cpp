#include <cassert>
#include <iostream>
#include <memory>
#include "matching_engine_consumer.hpp"
#include "outbound_ack.hpp"
#include "sequencer.hpp"

namespace {

using Matcher = MatchingEngineConsumer<1024>;

SequencedEvent make_new_order(uint64_t seq_num, uint32_t session_id, uint64_t client_order_id,
                               Side side, uint32_t price, uint32_t qty) {
    SequencedEvent ev{};
    ev.seq_num = seq_num;
    ev.sequence_ts_ns = seq_num;   // arbitrary, monotonic is enough
    ev.type = EventType::NewOrder;
    ev.session_id = session_id;
    ev.client_order_id = client_order_id;
    ev.side = side;
    ev.price = price;
    ev.quantity = qty;
    return ev;
}

SequencedEvent make_cancel(uint64_t seq_num, uint32_t session_id, uint64_t client_order_id) {
    SequencedEvent ev{};
    ev.seq_num = seq_num;
    ev.sequence_ts_ns = seq_num;
    ev.type = EventType::CancelOrder;
    ev.session_id = session_id;
    ev.client_order_id = client_order_id;
    return ev;
}

struct Harness {
    std::unique_ptr<OutputRing> ring = std::make_unique<OutputRing>();
    std::unique_ptr<AckQueue> acks = std::make_unique<AckQueue>();
    std::unique_ptr<Matcher> matcher = std::make_unique<Matcher>(*ring, *acks);
};

}  // namespace

void test_new_order_rests_when_no_match() {
    Harness h;
    h.matcher->apply(make_new_order(1, /*session*/1, /*client*/100, Side::Buy, 100, 10));

    OutboundAck ack;
    assert(h.acks->pop(ack));
    assert(ack.session_id == 1);
    assert(ack.client_order_id == 100);
    assert(ack.exchange_order_id == 1);
    assert(ack.status == ExecStatus::Accepted);
    assert(ack.fill_quantity == 0);
    assert(ack.leaves_quantity == 10);
    assert(!h.acks->pop(ack));
    assert(h.matcher->resting_orders() == 1);
    std::cout << "test_new_order_rests_when_no_match passed\n";
}

void test_matching_new_order_produces_two_acks_to_different_sessions() {
    Harness h;
    // session 1 rests a bid; session 2 (reusing the same client_order_id
    // as session 1 — deliberately, to also exercise isolation) crosses it.
    h.matcher->apply(make_new_order(1, /*session*/1, /*client*/1, Side::Buy, 100, 10));
    OutboundAck resting_ack;
    assert(h.acks->pop(resting_ack));   // Accepted ack for session 1, drained so the queue below is clean

    h.matcher->apply(make_new_order(2, /*session*/2, /*client*/1, Side::Sell, 100, 10));

    OutboundAck ack1, ack2;
    assert(h.acks->pop(ack1));   // resting counterparty's fill ack, pushed first
    assert(ack1.session_id == 1);
    assert(ack1.client_order_id == 1);
    assert(ack1.exchange_order_id == 1);
    assert(ack1.status == ExecStatus::Filled);
    assert(ack1.fill_price == 100);
    assert(ack1.fill_quantity == 10);
    assert(ack1.leaves_quantity == 0);

    assert(h.acks->pop(ack2));   // aggressor's own ack, pushed second
    assert(ack2.session_id == 2);
    assert(ack2.client_order_id == 1);
    assert(ack2.exchange_order_id == 2);
    assert(ack2.status == ExecStatus::Filled);
    assert(ack2.fill_quantity == 10);
    assert(ack2.leaves_quantity == 0);

    assert(!h.acks->pop(ack1));
    assert(h.matcher->resting_orders() == 0);
    std::cout << "test_matching_new_order_produces_two_acks_to_different_sessions passed\n";
}

void test_partial_fill_leaves_resting_with_reduced_qty() {
    Harness h;
    h.matcher->apply(make_new_order(1, 1, 1, Side::Buy, 100, 10));
    OutboundAck discard;
    assert(h.acks->pop(discard));

    h.matcher->apply(make_new_order(2, 2, 1, Side::Sell, 100, 4));

    OutboundAck resting_ack, aggressor_ack;
    assert(h.acks->pop(resting_ack));
    assert(resting_ack.session_id == 1);
    assert(resting_ack.status == ExecStatus::PartialFill);
    assert(resting_ack.fill_quantity == 4);
    assert(resting_ack.leaves_quantity == 6);

    assert(h.acks->pop(aggressor_ack));
    assert(aggressor_ack.session_id == 2);
    assert(aggressor_ack.status == ExecStatus::Filled);
    assert(aggressor_ack.fill_quantity == 4);
    assert(aggressor_ack.leaves_quantity == 0);

    assert(h.matcher->resting_orders() == 1);   // session 1's order still resting, reduced qty
    std::cout << "test_partial_fill_leaves_resting_with_reduced_qty passed\n";
}

void test_client_order_id_reuse_across_sessions_isolated() {
    Harness h;
    // Two different sessions both use client_order_id = 1. Same side
    // (Buy), so they never cross each other — both simply rest.
    h.matcher->apply(make_new_order(1, /*session*/1, /*client*/1, Side::Buy, 100, 10));
    h.matcher->apply(make_new_order(2, /*session*/2, /*client*/1, Side::Buy, 90, 5));
    OutboundAck discard;
    assert(h.acks->pop(discard));
    assert(h.acks->pop(discard));
    assert(h.matcher->resting_orders() == 2);

    // Canceling session 1's order must not touch session 2's, even
    // though both used client_order_id = 1.
    h.matcher->apply(make_cancel(3, /*session*/1, /*client*/1));
    OutboundAck cancel_ack;
    assert(h.acks->pop(cancel_ack));
    assert(cancel_ack.session_id == 1);
    assert(cancel_ack.exchange_order_id == 1);   // session 1's order resolved to exchange id 1
    assert(cancel_ack.status == ExecStatus::Canceled);
    assert(h.matcher->resting_orders() == 1);

    h.matcher->apply(make_cancel(4, /*session*/2, /*client*/1));
    assert(h.acks->pop(cancel_ack));
    assert(cancel_ack.session_id == 2);
    assert(cancel_ack.exchange_order_id == 2);   // resolved to a distinct exchange id
    assert(cancel_ack.status == ExecStatus::Canceled);
    assert(h.matcher->resting_orders() == 0);
    std::cout << "test_client_order_id_reuse_across_sessions_isolated passed\n";
}

void test_cancel_resolves_via_session_and_client_id() {
    Harness h;
    h.matcher->apply(make_new_order(1, 7, 42, Side::Sell, 200, 3));
    OutboundAck discard;
    assert(h.acks->pop(discard));

    h.matcher->apply(make_cancel(2, 7, 42));
    OutboundAck ack;
    assert(h.acks->pop(ack));
    assert(ack.status == ExecStatus::Canceled);
    assert(ack.exchange_order_id == 1);
    assert(h.matcher->resting_orders() == 0);
    std::cout << "test_cancel_resolves_via_session_and_client_id passed\n";
}

void test_cancel_unknown_order_rejected() {
    Harness h;
    h.matcher->apply(make_cancel(1, 1, 999));
    OutboundAck ack;
    assert(h.acks->pop(ack));
    assert(ack.status == ExecStatus::Rejected);
    assert(ack.exchange_order_id == 0);
    std::cout << "test_cancel_unknown_order_rejected passed\n";
}

void test_cancel_does_not_mint_an_order_id() {
    Harness h;
    h.matcher->apply(make_new_order(5, 1, 1, Side::Buy, 100, 10));   // rests at exchange id 5
    OutboundAck discard;
    assert(h.acks->pop(discard));

    // Unrelated cancel, stamped with seq_num 6 by the (simulated) sequencer.
    h.matcher->apply(make_cancel(6, 1, /*unknown client_order_id*/ 12345));
    OutboundAck ack;
    assert(h.acks->pop(ack));
    assert(ack.status == ExecStatus::Rejected);

    // The original order is untouched — no phantom order exists at id 6.
    assert(h.matcher->resting_orders() == 1);
    std::cout << "test_cancel_does_not_mint_an_order_id passed\n";
}

int main() {
    test_new_order_rests_when_no_match();
    test_matching_new_order_produces_two_acks_to_different_sessions();
    test_partial_fill_leaves_resting_with_reduced_qty();
    test_client_order_id_reuse_across_sessions_isolated();
    test_cancel_resolves_via_session_and_client_id();
    test_cancel_unknown_order_rejected();
    test_cancel_does_not_mint_an_order_id();
    std::cout << "All matching_engine_consumer tests passed.\n";
    return 0;
}
