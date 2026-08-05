// A standalone process — separate from the engine — that publishes
// synthetic (or CSV-replayed) order flow over UDP multicast. This is
// the same shape real exchange feeds use: one publisher, any number of
// subscribers, no direct connection between them.
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <string>
#include <thread>

#include "../include/market_data_feed.hpp"
#include "../include/wire_format.hpp"

int main(int argc, char** argv) {
    std::string data_file;
    size_t synthetic_count = 200'000;
    std::string group_ip = "239.255.0.1";  // administratively-scoped multicast range
    uint16_t port = 30001;
    int delay_us = 0;  // 0 = send as fast as possible

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--data" && i + 1 < argc) {
            data_file = argv[++i];
        } else if (arg == "--count" && i + 1 < argc) {
            synthetic_count = std::stoull(argv[++i]);
        } else if (arg == "--group" && i + 1 < argc) {
            group_ip = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            port = static_cast<uint16_t>(std::stoi(argv[++i]));
        } else if (arg == "--delay-us" && i + 1 < argc) {
            delay_us = std::stoi(argv[++i]);
        } else if (arg == "--help") {
            std::printf(
                "usage: feed_publisher [--data feed.csv] [--count N] [--group IP] "
                "[--port P] [--delay-us US]\n");
            return 0;
        }
    }

    std::vector<Order> events = data_file.empty()
                                     ? MarketDataFeed::generate_synthetic(synthetic_count)
                                     : MarketDataFeed::load_csv(data_file);
    std::printf("feed_publisher: sending %zu events to %s:%u%s\n", events.size(),
                group_ip.c_str(), port, delay_us > 0 ? " (throttled)" : " (as fast as possible)");

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    // TTL=1 keeps this on the local machine/subnet — don't want a demo
    // process accidentally routing packets further than intended.
    unsigned char ttl = 1;
    setsockopt(sock, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));

    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(port);
    inet_pton(AF_INET, group_ip.c_str(), &dest.sin_addr);

    for (auto& o : events) {
        WireOrder w = to_wire(o);
        ssize_t sent = sendto(sock, &w, sizeof(w), 0, reinterpret_cast<sockaddr*>(&dest),
                               sizeof(dest));
        if (sent != static_cast<ssize_t>(sizeof(w))) perror("sendto");
        if (delay_us > 0) std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
    }

    // A zero-id packet is the end-of-stream marker the subscriber watches for.
    WireOrder end_marker{};
    sendto(sock, &end_marker, sizeof(end_marker), 0, reinterpret_cast<sockaddr*>(&dest),
           sizeof(dest));

    std::printf("feed_publisher: done, sent %zu events + end marker\n", events.size());
    close(sock);
    return 0;
}