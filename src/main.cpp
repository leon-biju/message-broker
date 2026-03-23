#include <csignal>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <broker/tcp_gateway.hpp>
#include <broker/router.hpp>

volatile sig_atomic_t g_running {1};

extern "C" void handle_signal(int) {
    g_running = 0;
}

int main() {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    // Async logging: simply enqueue from hot-path threads and flush on background thread
    // TODO: add a file sink (rotating) alongside the console sink
    spdlog::init_thread_pool(8192, 1);
    auto logger = spdlog::stdout_color_mt<spdlog::async_factory>("broker");
    spdlog::set_default_logger(logger);
    spdlog::set_level(spdlog::level::debug);
    spdlog::set_pattern("[%H:%M:%S.%e] [%^%l%$] [%t] %v");

    moodycamel::BlockingConcurrentQueue<InboundMessage>  inbound_queue;
    moodycamel::BlockingConcurrentQueue<OutboundMessage> outbound_queue;
    Router router(RouterConfig { inbound_queue, outbound_queue, 2 });

    TcpGateway gateway(
        GatewayConfig{
        .max_connections = 32,
        .fd_table_size = 64,
        .port = 9000,
        .pinned_cpu_core = 1
        }, inbound_queue, outbound_queue);

    spdlog::info("Starting Message broker port={} max_connections={} gateway_cpu={} router_cpu={}",
        9000, 32, 1, 2);

    router.start();
    gateway.start();

    while (g_running) {
        pause(); // I mean, it's not urgent is it
    }

    spdlog::info("Shutting down...");
    gateway.stop();
    router.stop();
    spdlog::info("Bye!");
    spdlog::shutdown();
}
