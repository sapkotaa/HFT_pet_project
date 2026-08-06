#include <cassert>
#include <iostream>
#include <thread>
#include "session_state.hpp"

void test_logon_transitions_status(){
    SessionState session;
    assert(session.status() == SessionStatus::AwaitingLogon);

    bool accepted = session.handle_logon(/*client_id=*/1,/*hb_ms=*/1000,/*start_seq*/0);
    assert(accepted);
    assert(session.status() == SessionStatus ::LoggedOn);
    std::cout <<" test_logon_transitions_status passed \n";
}

void test_double_logon_rejected(){
    SessionState session;
    session.handle_logon(1, 1000, 0);

    bool second_attempt = session.handle_logon(1, 1000, 0);
    assert(!second_attempt);
    assert(session.status() == SessionStatus::LoggedOn);   // unchanged
    std::cout << "test_double_logon_rejected passed\n";
}

void test_in_order_seq_no_gap(){
    SessionState session;
    session.handle_logon(1, 1000, /*start_seq=*/0);

    // Logon itself consumes seq 0, so next expected is 1
    assert(session.track_inbound_seq(1) == std::nullopt);
    assert(session.track_inbound_seq(2) == std::nullopt);
    assert(session.track_inbound_seq(3) == std::nullopt);
    std::cout << "test_in_order_seq_no_gap passed\n";
}

void test_seq_gap_detected(){
  SessionState session;
    session.handle_logon(1, 1000, 0);

    session.track_inbound_seq(1);   // expected 2 next
    auto gap = session.track_inbound_seq(5);   // jumped, skipped 2,3,4

    assert(gap.has_value());
    assert(gap->first == 2);
    assert(gap->second == 4);
    std::cout << "test_seq_gap_detected passed\n";
}

void test_duplicate_seq_ignored(){
    SessionState session;
    session.handle_logon(1, 1000, 0);

    session.track_inbound_seq(1);   // expected 2 next
    session.track_inbound_seq(2);   // expected 3 next

    auto result = session.track_inbound_seq(1);   // stale replay
    assert(result == std::nullopt);
    // confirm it didn't corrupt the real expected seq
    assert(session.track_inbound_seq(3) == std::nullopt);
    std::cout << "test_duplicate_seq_ignored passed\n";

}

void test_heartbeat_timing(){
  SessionState session;
    session.handle_logon(1, /*hb_ms=*/50, 0);   // short interval for the test

    session.mark_outbound_activity();
    assert(!session.should_send_heartbeat());   // just marked, shouldn't fire yet

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    assert(session.should_send_heartbeat());     // interval elapsed
    std::cout << "test_heartbeat_timing passed\n";

}

void test_peer_dead_detection(){
    SessionState session;
    session.handle_logon(1, /*hb_ms=*/50, 0);

    assert(!session.is_peer_dead());   // just logged on, activity is fresh

    std::this_thread::sleep_for(std::chrono::milliseconds(120));   // > 2x interval
    assert(session.is_peer_dead());
    std::cout << "test_peer_dead_detection passed\n";

}

int main(){
    test_logon_transitions_status();
    test_double_logon_rejected();
    test_in_order_seq_no_gap();
    test_seq_gap_detected();
    test_duplicate_seq_ignored();
    test_heartbeat_timing();
    test_peer_dead_detection();
    std::cout << "All session_state tests passed.\n";
    return 0;

}