#pragma once
#include <sys/event.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>
#include "event.hpp"
#include "sequencer.hpp"
#include "outbound_ack.hpp"
#include <unistd.h>
#include <cstring>
#include <unordered_map>
#include <vector>
#include <iostream>
#include "frame_reader.hpp"
#include "session_state.hpp"
#include "gateway_protocol.hpp"

struct ConnectionState {
    FrameReader   reader;
    SessionState  session;
};

struct RiskEngine {
    bool check(int /*fd*/, const NewOrderMsg& /*order*/) { return true; }
};

class GatewayServer {
public:
    GatewayServer(uint16_t port, InputQueue* sequencer_input, AckQueue* acks)
        : port_(port), sequencer_input_(sequencer_input), acks_(acks) {}

    bool start() {
        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) return false;

        int opt = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port_);

        if (bind(listen_fd_, (sockaddr*)&addr, sizeof(addr)) < 0) return false;
        if (listen(listen_fd_, 128) < 0) return false;
        set_nonblocking(listen_fd_);

        kq_ = kqueue();
        if (kq_ < 0) return false;

        register_read(listen_fd_);
        return true;
    }

    // Call in a loop from main(). Returns roughly every 200ms even when
    // idle, so heartbeat/dead-peer checks keep running without traffic.
    void poll_once() {
        struct timespec timeout{0, 200 * 1000 * 1000};
        struct kevent events[64];
        int n_ready = kevent(kq_, nullptr, 0, events, 64, &timeout);

        for (int i = 0; i < n_ready; ++i) {
            int fd = static_cast<int>(events[i].ident);

            if (fd == listen_fd_) {
                accept_new_connection();
                continue;
            }
            if (events[i].flags & EV_EOF) {
                close_connection(fd);
                continue;
            }
            handle_readable(fd);
        }

        check_heartbeats();
        drain_acks();
    }

private:
    void set_nonblocking(int fd) {
        int flags = fcntl(fd, F_GETFL, 0);
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    void register_read(int fd) {
        struct kevent ev;
        EV_SET(&ev, fd, EVFILT_READ, EV_ADD, 0, 0, nullptr);
        kevent(kq_, &ev, 1, nullptr, 0, nullptr);
    }

    void accept_new_connection() {
        int client_fd = accept(listen_fd_, nullptr, nullptr);
        if (client_fd < 0) return;

        set_nonblocking(client_fd);
        register_read(client_fd);
        connections_[client_fd] = ConnectionState{};
        std::cout << "accepted fd=" << client_fd << "\n";
    }

    void handle_readable(int fd) {
        auto it = connections_.find(fd);
        if (it == connections_.end()) return;

        uint8_t buf[4096];
        while (true) {
            ssize_t n = recv(fd, buf, sizeof(buf), 0);
            if (n > 0) {
                it->second.reader.feed(buf, static_cast<size_t>(n));
                while (auto frame = it->second.reader.next_frame()) {
                    dispatch(fd, *frame, it->second.session);
                }
            } else if (n == 0) {
                close_connection(fd);
                return;
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                close_connection(fd);
                return;
            }
        }
    }

    void close_connection(int fd) {
        struct kevent ev;
        EV_SET(&ev, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
        kevent(kq_, &ev, 1, nullptr, 0, nullptr);
        close(fd);
        connections_.erase(fd);
        std::cout << "closed fd=" << fd << "\n";
    }

    void dispatch(int fd, const std::vector<uint8_t>& frame, SessionState& session) {
        if (frame.size() < sizeof(MsgHeader)) return;   // guaranteed by FrameReader, defensive anyway

        MsgHeader header;
        std::memcpy(&header, frame.data(), sizeof(MsgHeader));
        const uint8_t* body = frame.data() + sizeof(MsgHeader);

        switch (header.msg_type) {
            case MsgType::Logon: {
                if (header.body_length < sizeof(LogonMsg)) return;
                LogonMsg logon;
                std::memcpy(&logon, body, sizeof(LogonMsg));

                bool accepted = session.handle_logon(
                    logon.client_id, logon.heartbeat_interval_ms, header.seq_num);

                if (accepted) {
                    send_logon_ack(fd, session);
                    std::cout << "fd=" << fd << " logged on, client_id=" << logon.client_id << "\n";
                } else {
                    std::cout << "fd=" << fd << " logon rejected\n";
                    close_connection(fd);
                }
                break;
            }

            case MsgType::Heartbeat:
            case MsgType::TestRequest: {
                report_seq_gap_if_any(fd, session, header.seq_num);
                break;
            }

           
            case MsgType::NewOrder: {
    report_seq_gap_if_any(fd, session, header.seq_num);
    if (header.body_length < sizeof(NewOrderMsg)) return;

    NewOrderMsg order;
    std::memcpy(&order, body, sizeof(NewOrderMsg));

  if (!risk_.check(fd, order)) {
        send_execution_report(fd, session, order.client_order_id,
                              ExecStatus::Rejected, 0, 0, 0);
        break;
    }

SequencedEvent ev{};
ev.type = EventType::NewOrder;
ev.session_id = static_cast<uint32_t>(fd);
ev.client_order_id = order.client_order_id;
ev.side = order.side;
ev.price = order.price;
ev.quantity = order.quantity;

    if(!sequencer_input_ -> push(ev)){
        send_execution_report(fd,session,order.client_order_id,ExecStatus::Rejected,0,0,0);
    }
    // No synchronous Accepted here — real Accepted/PartialFill/Filled/
    // Rejected now arrives asynchronously via drain_acks() once the
    // sequencer + matching engine process this event.
    break;
}

            case MsgType::CancelOrder: {
                report_seq_gap_if_any(fd, session, header.seq_num);
                if (header.body_length < sizeof(CancelOrderMsg)) return;

                CancelOrderMsg cancel;
                std::memcpy(&cancel, body, sizeof(CancelOrderMsg));

                SequencedEvent cev{};   // zero-init: side/price/quantity are
                                        // meaningless for a cancel, left at
                                        // their zero defaults, unused.
                cev.type = EventType::CancelOrder;
                cev.session_id = static_cast<uint32_t>(fd);
                cev.client_order_id = cancel.client_order_id;

                if (!sequencer_input_->push(cev)) {
                    send_execution_report(fd, session, cancel.client_order_id,
                                          ExecStatus::Rejected, 0, 0, 0);
                }
                // No synchronous ack on success — real Canceled/Rejected
                // comes back via drain_acks() once the matching engine
                // resolves it.
                break;
            }

            case MsgType::Logout: {
                session.logout();
                close_connection(fd);
                break;
            }

            default:
                break;   // unknown/unhandled type — ignore rather than crash
        }
    }

    void drain_acks() {
        OutboundAck ack;
        while (acks_->pop(ack)) {
            auto it = connections_.find(static_cast<int>(ack.session_id));
            if (it == connections_.end()) continue;   // client disconnected before its ack arrived
            ExecutionReportMsg report{ack.client_order_id, ack.exchange_order_id, ack.status,
                                       ack.fill_price, ack.fill_quantity, ack.leaves_quantity};
            send_frame(static_cast<int>(ack.session_id), MsgType::ExecutionReport,
                       it->second.session.next_outbound_seq(),
                       reinterpret_cast<uint8_t*>(&report), sizeof(report));
            it->second.session.mark_outbound_activity();
        }
    }

    void report_seq_gap_if_any(int fd, SessionState& session, uint32_t seq_num) {
        auto gap = session.track_inbound_seq(seq_num);
        if (gap) {
            std::cout << "fd=" << fd << " seq gap " << gap->first << ".." << gap->second << "\n";
            send_resend_request(fd, session, gap->first, gap->second);
        }
    }

    void check_heartbeats() {
        std::vector<int> fds;
        fds.reserve(connections_.size());
        for (auto& [fd, conn] : connections_) fds.push_back(fd);

        for (int fd : fds) {
            auto it = connections_.find(fd);
            if (it == connections_.end()) continue;   // closed earlier this pass
            auto& session = it->second.session;

            if (session.status() != SessionStatus::LoggedOn) continue;

            if (session.is_peer_dead()) {
                std::cout << "fd=" << fd << " heartbeat timeout, disconnecting\n";
                session.disconnect();
                close_connection(fd);
                continue;
            }
            if (session.should_send_heartbeat()) {
                send_frame(fd, MsgType::Heartbeat, session.next_outbound_seq(), nullptr, 0);
                session.mark_outbound_activity();
            }
        }
    }

    void send_frame(int fd, MsgType type, uint32_t seq_num, const uint8_t* body, uint16_t body_len) {
        MsgHeader header{body_len, type, seq_num};
        std::vector<uint8_t> out(sizeof(MsgHeader) + body_len);
        std::memcpy(out.data(), &header, sizeof(MsgHeader));
        if (body_len > 0) std::memcpy(out.data() + sizeof(MsgHeader), body, body_len);

        ssize_t sent = 0;
        while (sent < static_cast<ssize_t>(out.size())) {
            ssize_t n = send(fd, out.data() + sent, out.size() - sent, 0);
            if (n <= 0) return;   // peer gone; next readable/EOF event cleans it up
            sent += n;
        }
    }

    void send_logon_ack(int fd, SessionState& session) {
        LogonMsg ack{};
        send_frame(fd, MsgType::Logon, session.next_outbound_seq(),
                   reinterpret_cast<uint8_t*>(&ack), sizeof(ack));
        session.mark_outbound_activity();
    }

    void send_resend_request(int fd, SessionState& session, uint32_t from_seq, uint32_t to_seq) {
        struct { uint32_t from; uint32_t to; } body{from_seq, to_seq};
        send_frame(fd, MsgType::ResendRequest, session.next_outbound_seq(),
                   reinterpret_cast<uint8_t*>(&body), sizeof(body));
        session.mark_outbound_activity();
    }

    void send_execution_report(int fd, SessionState& session, uint64_t client_order_id,
                                ExecStatus status, uint32_t fill_price,
                                uint32_t fill_qty, uint32_t leaves_qty) {
        ExecutionReportMsg report{client_order_id, /*exchange_order_id=*/0,
                                   status, fill_price, fill_qty, leaves_qty};
        send_frame(fd, MsgType::ExecutionReport, session.next_outbound_seq(),
                   reinterpret_cast<uint8_t*>(&report), sizeof(report));
        session.mark_outbound_activity();
    }
    RiskEngine risk_;
    InputQueue* sequencer_input_;
    AckQueue* acks_;
    uint16_t port_;
    int listen_fd_ = -1;
    int kq_ = -1;
    std::unordered_map<int, ConnectionState> connections_;
};