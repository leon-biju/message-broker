#include <csignal>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/cfg/env.h>

#include <broker/tcp_gateway.hpp>
#include <broker/router.hpp>

volatile sig_atomic_t g_running {1};

extern "C" void handle_signal(int) {
    g_running = 0;
}

int main() {
    constexpr uint32_t max_connections = 32;
    constexpr size_t   fd_table_size = 64; // should be >= max_connections, upper bound for fd indexing
    constexpr uint16_t port = 9000;
    constexpr int      gateway_cpu = 1;
    constexpr int      router_cpu = 2;
    
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    // Async logging: simply enqueue from hot-path threads and flush on background thread
    // TODO: add a file sink (rotating) alongside the console sink
    spdlog::init_thread_pool(8192, 1);
    auto logger = spdlog::stdout_color_mt<spdlog::async_factory>("broker");
    spdlog::set_default_logger(logger);
    spdlog::cfg::load_env_levels();  // respects SPDLOG_LEVEL env var; falls back to debug
    spdlog::set_pattern("[%H:%M:%S.%e] [%^%l%$] [%t] %v");

    moodycamel::BlockingConcurrentQueue<InboundMessage> inbound_queue;
    OutboundTable outbound_table(fd_table_size);
    Router router(inbound_queue, outbound_table, router_cpu);

    TcpGateway gateway(inbound_queue, outbound_table, max_connections, fd_table_size, port, gateway_cpu);
    spdlog::info("Starting Message broker port={} max_connections={} gateway_cpu={} router_cpu={}",
        port, max_connections, gateway_cpu, router_cpu);

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
