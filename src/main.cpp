#include <broker/tcp_gateway.hpp>
#include <csignal>
#include <chrono>
#include <iostream>
#include <print>
#include <thread>
#include <variant>

static volatile sig_atomic_t g_running = 1;

static void drain_queue(moodycamel::ConcurrentQueue<InboundMessage>& queue) {
    InboundMessage msg;
    while (g_running) {
        if (queue.try_dequeue(msg)) {
            const auto& hdr = msg.frame.header;
            const auto type = static_cast<MessageType>(hdr.type);

            std::visit([&](const auto& payload) {
                using T = std::decay_t<decltype(payload)>;
                if constexpr (std::is_same_v<T, SubscribeMsg>) {
                    std::println("[SUBSCRIBE]   seq={} topic=\"{}\" fd={}", hdr.sequence, payload.topic, msg.sender_fd);
                } else if constexpr (std::is_same_v<T, UnsubscribeMsg>) {
                    std::println("[UNSUBSCRIBE] seq={} topic=\"{}\" fd={}", hdr.sequence, payload.topic, msg.sender_fd);
                } else if constexpr (std::is_same_v<T, PublishMsg>) {
                    std::println("[PUBLISH]     seq={} topic=\"{}\" payload_len={} fd={}", hdr.sequence, payload.topic, payload.body.size(), msg.sender_fd);
                } else if constexpr (std::is_same_v<T, AckMsg>) {
                    std::println("[ACK]         seq={} acked_seq={} fd={}", hdr.sequence, payload.acked_seq, msg.sender_fd);
                } else if constexpr (std::is_same_v<T, ErrorMsg>) {
                    std::println("[ERROR]       seq={} code={} msg=\"{}\" fd={}", hdr.sequence, to_string(payload.code), payload.error_msg, msg.sender_fd);
                }
            }, msg.frame.payload);
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
}

int main() {
    // Balance between keeping table size low and avoiding reallocation syscall
    constexpr uint32_t fd_table_size   {2048};    // system-level resource bound on file descriptors
    constexpr uint32_t max_connections {1024};    // policy decision on how many clients we can serve
    static_assert(max_connections < fd_table_size, "Max connections MUST be less than fd table size");

    std::signal(SIGINT,  [](int) { g_running = 0; });
    std::signal(SIGTERM, [](int) { g_running = 0; });

    moodycamel::ConcurrentQueue<InboundMessage> queue;

    std::thread consumer(drain_queue, std::ref(queue));

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
    consumer.join();
}