#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include "gateway_server.hpp"
#include "matching_engine_consumer.hpp"
#include "wal_consumer.hpp"

static volatile std::sig_atomic_t g_shutdown = 0;
void handle_sigint(int) { g_shutdown = 1; }

int main(int argc, char** argv) {
    uint16_t port = 9000;
    if (argc > 1) port = static_cast<uint16_t>(std::atoi(argv[1]));
    std::string wal_path = (argc > 2) ? argv[2] : "wal/events.bin";

    std::signal(SIGINT, handle_sigint);

    // Sequencer sequencer;
    // sequencer.start(); it is in stack causes overflow

    auto sequencer = std::make_unique<Sequencer>(); // heap not stack
    auto acks = std::make_unique<AckQueue>();        // heap — 65536 * sizeof(OutboundAck)

    // Both consumers must register on the ring BEFORE the sequencer
    // starts publishing anything. BroadcastRing::try_consume can't tell
    // "producer paused" from "this consumer registered late and missed
    // a wraparound" — a consumer that registers after the ring has
    // wrapped would silently read stale, overwritten slots. Constructing
    // these here (which calls register_consumer()) before sequencer->start()
    // is what guarantees that never happens.
    auto matcher = std::make_unique<MatchingEngineConsumer<>>(sequencer->output(), *acks);
    auto wal = std::make_unique<WalConsumer>(sequencer->output(), wal_path);

    if (!wal->start()) {
        std::cerr << "Failed to open WAL at " << wal_path << "\n";
        return 1;
    }
    matcher->start();
    sequencer->start();

    GatewayServer server(port, &sequencer->input_for(ProducerId::Gateway), acks.get());
    if (!server.start()) {
        std::cerr << "Failed to start gateway on port " << port << "\n";
        sequencer->stop();
        matcher->stop();
        wal->stop();
        return 1;
    }
    std::cout << "Gateway listening on port " << port << "\n";

    while (!g_shutdown) {
        server.poll_once();
    }

    std::cout << "Shutting down.\n";
    // Shutdown order matters: stop the source first (no new events can
    // enter the ring after this returns), then each consumer in turn.
    // Each consumer's final drain is only safe because the sequencer has
    // already stopped by the time its stop() is called.
    sequencer->stop();
    matcher->stop();
    wal->stop();
    return 0;
}
