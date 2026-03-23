#include <csignal>
#include <print>

#include <broker/tcp_gateway.hpp>
#include <broker/router.hpp>

volatile sig_atomic_t g_running {1};

extern "C" void handle_signal(int) {
    g_running = 0;
}

int main() {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    std::println("Starting broker...");
    moodycamel::ConcurrentQueue<InboundMessage>          inbound_queue;
    moodycamel::BlockingConcurrentQueue<OutboundMessage> outbound_queue;
    Router router(RouterConfig { inbound_queue, outbound_queue, 2 });

    TcpGateway gateway(
        GatewayConfig{
        .max_connections = 32,
        .fd_table_size = 64,
        .port = 9000,
        .pinned_cpu_core = 1
        }, inbound_queue, outbound_queue);

    router.start();
    gateway.start();

    while (g_running) {
        std::this_thread::yield();
    }

    gateway.stop();
    router.stop();

}
