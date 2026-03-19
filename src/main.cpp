#include <broker/tcp_gateway.hpp>
#include <csignal>
#include <iostream>

static volatile sig_atomic_t g_running = 1;

int main() {
    // Balance between keeping table size low and avoiding reallocation syscall
    constexpr uint32_t fd_table_size   {2048};    // system-level resource bound on file descriptors
    constexpr uint32_t max_connections {1024};    // policy decision on how many clients we can serve
    static_assert(max_connections < fd_table_size, "Max connections MUST be less than fd table size");

    std::signal(SIGINT,  [](int) { g_running = 0; });
    std::signal(SIGTERM, [](int) { g_running = 0; });

    moodycamel::ConcurrentQueue<InboundMessage> queue;


    TcpGateway gateway(
        GatewayConfig{
            .max_connections = max_connections,
            .fd_table_size = fd_table_size,
            .port = 9000,
            .pinned_cpu_core = -1 },
        queue
    );

    gateway.start();
    std::cout << "Listening on port 9000\n";

    while (g_running) pause();

    gateway.stop();
}