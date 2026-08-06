#include <csignal>
#include <cstdlib>
#include <iostream>
#include "gateway_server.hpp"

static volatile std::sig_atomic_t g_shutdown = 0;
void handle_sigint(int) { g_shutdown = 1; }

int main(int argc, char** argv) {
    uint16_t port = 9000;
    if (argc > 1) port = static_cast<uint16_t>(std::atoi(argv[1]));

    std::signal(SIGINT, handle_sigint);

    GatewayServer server(port);
    if (!server.start()) {
        std::cerr << "Failed to start gateway on port " << port << "\n";
        return 1;
    }
    std::cout << "Gateway listening on port " << port << "\n";

    while (!g_shutdown) {
        server.poll_once();
    }

    std::cout << "Shutting down gateway.\n";
    return 0;
}