// A standalone process — the matching engine, with no direct connection
// to the publisher. It joins a multicast group and processes whatever
// arrives, exactly like a real exchange gateway would. Latency here is
// measured from wire receipt to book update — a genuinely different
// (and more honest) number than the in-process version's tick-to-book,
// since it now includes the OS network stack.
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "../include/latency_histogram.hpp"
#include "../include/order_book.hpp"
#include "../include/results.hpp"
#include "../include/strategy.hpp"
#include "../include/timer.hpp"
#include "../include/wire_format.hpp"

namespace {
constexpr size_t kPoolCapacity = 1 << 20;
}

int main(int argc, char** argv) {
    std::string group_ip = "239.255.0.1";
    uint16_t port = 30001;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--group" && i + 1 < argc) {
            group_ip = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--help") {
            std::printf("usage: engine_subscriber [--group IP] [--port P]\n");
            return 0;
        }
    }

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    int reuse = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    // UDP has no flow control — a fast publisher can overrun the kernel's
    // default receive buffer and silently drop packets. Growing it here
    // reduces loss under burst but doesn't eliminate it; real market data
    // protocols solve this properly with sequence numbers plus a gap-fill
    // recovery channel. See README for what dropped packets look like.
    int rcvbuf = 4 * 1024 * 1024;
    setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));


    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_ANY);
    local.sin_port = htons(port);
    if (bind(sock, reinterpret_cast<sockaddr*>(&local), sizeof(local)) < 0) {
        perror("bind");
        return 1;
    }

    ip_mreq mreq{};
    inet_pton(AF_INET, group_ip.c_str(), &mreq.imr_multiaddr);
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);
    if (setsockopt(sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
        perror("IP_ADD_MEMBERSHIP");
        return 1;
    }

    std::printf("engine_subscriber: listening on %s:%u\n", group_ip.c_str(), port);

    // Heap-allocated: OrderBook embeds a ~1M-slot object pool inline —
    // far too large for the stack (see object_pool.hpp).
    auto book_ptr = std::make_unique<OrderBook<kPoolCapacity>>();
    auto& book = *book_ptr;
    MarketMaker<kPoolCapacity> mm(/*half_spread=*/3, /*quote_size=*/10, /*requote_threshold=*/2);
    OrderId strategy_id_cursor = 1'000'000'000;

    TscClock clock;
    LatencyHistogram hist;
    std::vector<Trade> trades;
    trades.reserve(64);
    uint64_t processed = 0, trade_count = 0;

    WireOrder w;
    uint64_t expected_id = 1;
    uint64_t dropped = 0;
    while (true) {
        ssize_t n = recv(sock, &w, sizeof(w), 0);
        if (n < 0)
        {
            std::printf("engine_subscriber: idle timeout (5s, no traffic) — treating as end of stream\n");
            break;
        }
        if (n != static_cast<ssize_t>(sizeof(w))) continue;  // partial/corrupt packet, skip
        if (w.id == 0) break;                                 // end-of-stream marker

        if (w.id != expected_id) dropped += (w.id > expected_id) ? (w.id - expected_id) : 0;
        expected_id = w.id + 1;

        uint64_t t0 = TscClock::rdtsc();
        uint64_t recv_ns = static_cast<uint64_t>(clock.cycles_to_ns(t0));
        Order o = from_wire(w);

        trades.clear();
        book.add_limit_order(o.id, o.side, o.price, o.qty, recv_ns, trades);
        mm.on_tick(book, strategy_id_cursor, recv_ns, trades);
        uint64_t t1 = TscClock::rdtsc();
        hist.record(static_cast<uint64_t>(clock.cycles_to_ns(t1 - t0)));
        processed++;
        trade_count += trades.size();
    }

    std::printf("\nengine_subscriber: processed %llu orders, %llu trades\n",
                (unsigned long long)processed, (unsigned long long)trade_count);
    std::printf("Dropped packets (sequence gaps): %llu\n", (unsigned long long)dropped);
    std::printf("Resting orders remaining: %zu (%zu bid levels, %zu ask levels)\n\n",
                book.resting_orders(), book.bid_levels(), book.ask_levels());

    hist.print_summary("wire-to-book tick latency");
    append_result_csv("results/history.csv", "udp_multicast", processed, hist);

    close(sock);
    return 0;
}