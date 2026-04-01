#include <csignal>
#include <print>

#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/cfg/env.h>

#include <toml++/toml.hpp>

#include <broker/tcp_gateway.hpp>
#include <broker/router.hpp>

volatile sig_atomic_t g_running {1};

extern "C" void handle_signal(int) {
    g_running = 0;
}

int main(int argc, char* argv[]) {

    // Setup signal handlers so we close sockets and flush logs properly
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    std::signal(SIGHUP, handle_signal);

    // Set up async logging: simply enqueue from hot-path threads and this will flush on background thread
    spdlog::init_thread_pool(8192, 1);
    auto logger = spdlog::stdout_color_mt<spdlog::async_factory>("broker");
    spdlog::set_default_logger(logger);
    spdlog::cfg::load_env_levels();  // follow SPDLOG_LEVEL env var -- defaults to debug if not set
    spdlog::set_pattern("[%H:%M:%S.%e] [%^%l%$] [%t] %v");

    // Defaults
    uint32_t max_connections = 32;
    size_t   fd_table_size   = 64;
    uint16_t port            = 9000;
    int      gateway_cpu     = 1;
    int      router_cpu      = 2;

    // Optional TOML config: ./broker [config.toml]
    if (argc > 1) {
        try {
            const auto tbl = toml::parse_file(argv[1]);
            if (auto v = tbl["port"].value<int>())             port            = static_cast<uint16_t>(*v);
            if (auto v = tbl["max_connections"].value<int>())  max_connections = static_cast<uint32_t>(*v);
            if (auto v = tbl["fd_table_size"].value<int>())    fd_table_size   = static_cast<size_t>(*v);
            if (auto v = tbl["gateway_cpu"].value<int>())      gateway_cpu     = *v;
            if (auto v = tbl["router_cpu"].value<int>())       router_cpu      = *v;
            spdlog::info("Loaded config from {}", argv[1]);
        } catch (const toml::parse_error& e) {
            std::println(stderr, "Failed to parse config \"{}\": {}", argv[1], e.description());
            return 1;
        }
    }

    if (fd_table_size < max_connections) {
        spdlog::warn("fd_table_size ({}) < max_connections ({}), setting fd_table_size = max_connections * 2",
            fd_table_size, max_connections);
        fd_table_size = max_connections * 2;
    }

    Metrics metrics;

    moodycamel::BlockingConcurrentQueue<InboundMessage> inbound_queue;
    OutboundTable outbound_table(fd_table_size);
    Router router(inbound_queue, outbound_table, metrics, router_cpu);

    TcpGateway gateway(inbound_queue, outbound_table, metrics, max_connections, fd_table_size, port, gateway_cpu);
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
