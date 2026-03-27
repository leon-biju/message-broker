// This will be a spam publisher that connects to broker with an adjustable per second rate
// Usage: ./publisher [host] [port] [topic] [rate_hz] [payload]
// Defaults: 127.0.0.1 9000 test/topic 10 msg

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <print>
#include <string_view>
#include <thread>

#include <arpa/inet.h>
#include <csignal>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <broker/protocol.hpp>

static std::atomic<bool> g_running{true};

static void handle_signal(int) { g_running.store(false, std::memory_order_relaxed); }

static bool send_all(const int fd, const std::byte* data, const size_t len) {
    size_t sent = 0;
    while (sent < len) {
        const ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

static bool recv_all(const int fd, std::byte* data, const size_t len) {
    size_t received = 0;
    while (received < len) {
        const ssize_t n = recv(fd, data + received, len - received, 0);
        if (n <= 0) return false;
        received += static_cast<size_t>(n);
    }
    return true;
}

int main(int argc, char* argv[]) {
    std::println("Publisher Starting...");
    const char*    host    = argc > 1 ? argv[1] : "127.0.0.1";
    const uint16_t port    = argc > 2 ? static_cast<uint16_t>(std::stoi(argv[2])) : 9000;
    const char*    topic   = argc > 3 ? argv[3] : "test/topic";
    const int      rate_hz = argc > 4 ? std::stoi(argv[4]) : 10;
    const char*    payload = argc > 5 ? argv[5] : "msg";

    if (rate_hz <= 0) {
        std::println(stderr, "rate_hz must be > 0");
        return 1;
    }

    std::signal(SIGINT,  handle_signal);
    std::signal(SIGTERM, handle_signal);

    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::println(stderr, "socket() failed: {}", strerror(errno));
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    if (inet_pton(AF_INET, host, &addr.sin_addr) <= 0) {
        std::println(stderr, "Invalid host: {}", host);
        close(fd);
        return 1;
    }

    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::println(stderr, "connect() failed: {}", strerror(errno));
        close(fd);
        return 1;
    }
    std::println("Connected to {}:{} — publishing to \"{}\" at {} msg/s", host, port, topic, rate_hz);

    std::array<std::byte, sizeof(FrameHeader) + MAX_PAYLOAD_LEN> buf{};
    std::array<std::byte, sizeof(FrameHeader) + sizeof(uint64_t)> ack_buf{};
    const auto interval = std::chrono::nanoseconds(1'000'000'000 / rate_hz);
    const auto body     = std::as_bytes(std::span(std::string_view{payload}));
    uint64_t seq = 0;
    uint64_t expected_broker_seq = 0;

    while (g_running.load(std::memory_order_relaxed)) {
        const auto result = encode_publish(buf, ++seq, topic, body);
        if (!result) {
            std::println(stderr, "encode_publish failed (seq={})", seq);
            break;
        }
        const auto t0 = std::chrono::steady_clock::now();
        if (!send_all(fd, buf.data(), *result)) {
            std::println(stderr, "send failed (seq={}): {}", seq, strerror(errno));
            break;
        }

        // Read ACK
        if (!recv_all(fd, ack_buf.data(), sizeof(FrameHeader))) {
            std::println(stderr, "recv ACK header failed (seq={}): {}", seq, strerror(errno));
            break;
        }
        const auto ack_hdr = parse_header(std::span{ack_buf.data(), sizeof(FrameHeader)});
        if (!ack_hdr) {
            std::println(stderr, "parse ACK header failed (seq={})", seq);
            break;
        }
        ++expected_broker_seq;
        if (ack_hdr->sequence != expected_broker_seq) {
            std::println(stderr, "WARN: broker seq gap: expected={} got={}", expected_broker_seq, ack_hdr->sequence);
            expected_broker_seq = ack_hdr->sequence;
        }
        if (ack_hdr->payload_len > 0) {
            if (!recv_all(fd, ack_buf.data() + sizeof(FrameHeader), ack_hdr->payload_len)) {
                std::println(stderr, "recv ACK payload failed (seq={}): {}", seq, strerror(errno));
                break;
            }
        }
        const auto ack_frame = decode_frame(*ack_hdr, std::span{
            ack_buf.data() + sizeof(FrameHeader), ack_hdr->payload_len});
        if (!ack_frame) {
            std::println(stderr, "decode ACK failed (seq={})", seq);
            break;
        }
        bool ok = std::visit([&](const auto& msg) -> bool {
            using T = std::decay_t<decltype(msg)>;
            if constexpr (std::is_same_v<T, AckMsg>) {
                if (msg.acked_seq != seq) {
                    std::println(stderr, "ACK seq mismatch: expected={} got={}", seq, msg.acked_seq);
                    return false;
                }
                return true;
            } else if constexpr (std::is_same_v<T, ErrorMsg>) {
                std::println(stderr, "ERROR from broker: {} — {}",
                    to_string(msg.code), msg.error_msg);
                return false;
            } else {
                std::println(stderr, "Unexpected message type instead of ACK (seq={})", seq);
                return false;
            }
        }, ack_frame->payload);
        if (!ok) break;

        const auto rtt_us = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - t0).count();
        std::println(R"(seq={} topic="{}" payload="{}" time={:.3f} ms)",
            seq, topic, payload, rtt_us / 1000.0);
        std::this_thread::sleep_for(interval);
    }

    std::println("Stopping. {} messages published.", seq);
    close(fd);
    return 0;
}
