#pragma once
#include <cstdint>
#include <chrono>
#include <optional>
#include "gateway_protocol.hpp"
#include<utility>

enum class SessionStatus{
    AwaitingLogon,
    LoggedOn,
    LoggedOut,
    Disconnected, // heartbeat timeout - treat connection as dead 
};

class SessionState{
    public:
      SessionStatus status() const { return status_; }

     // Called when a logon frame arrives. Returns true if accepted

     bool handle_logon(uint64_t client_id, uint32_t hearbeat_interval_ms,
                  uint32_t peer_starting_seq){

        if (status_ != SessionStatus::AwaitingLogon) return false;
           client_id_ = client_id;
           heartbeat_interval_ = std::chrono::milliseconds(hearbeat_interval_ms);
           expected_peer_seq_ = peer_starting_seq + 1;
           last_peer_activity_ = clock::now();
           status_ = SessionStatus::LoggedOn;
           return true;

          }

// calls for every inbound mesage, logon included, after parsing seq_num.
//Returns nullopt if the seq is as expected, returns the missing range
// if a gap is detected, so the called can send a ResendRequest;
std::optional<std::pair<uint32_t,uint32_t>> track_inbound_seq(uint32_t seq_num){
    last_peer_activity_ = clock::now();


    if(seq_num == expected_peer_seq_){
        ++expected_peer_seq_;
        return std::nullopt;
    }

    if(seq_num > expected_peer_seq_){
        auto gap = std::make_pair(expected_peer_seq_,seq_num -1);
        expected_peer_seq_ = seq_num + 1; // resync forward , don't get stuck
        return gap;
    }

    //seq_num < exepected : duplicate/replay -- called should drop, not gap-fill
    return std::nullopt;


}

uint32_t next_outbound_seq() {return outbound_seq_++;}

// Poll periodically that is every 100ms from the event loop to decide
// whether a heartbeat needs sending ior the peer's gone quiet..

bool should_send_heartbeat() const {
    return clock::now() - last_outbound_activity_ >= heartbeat_interval_;

}

bool is_peer_dead(int missed_intervals_allowed = 2 ) const {
    return clock::now() - last_peer_activity_ > 
        heartbeat_interval_ * missed_intervals_allowed;
}

void mark_outbound_activity() {last_outbound_activity_ = clock::now();}
void logout() {status_ = SessionStatus::LoggedOut;}
void disconnect() {status_ = SessionStatus::Disconnected;}



private:
    using clock = std::chrono::steady_clock;

    SessionStatus status_ = SessionStatus::AwaitingLogon;
    uint64_t client_id_ = 0;
    std::chrono::milliseconds heartbeat_interval_{1000};
    uint32_t expected_peer_seq_ = 0;
    uint32_t outbound_seq_ = 0;
    clock::time_point last_peer_activity_;
    clock::time_point last_outbound_activity_;

};