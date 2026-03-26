// This simply subscribes to a topic and listens and prints/counts messages received
// Usage: ./subscriber [host] [port] [topic]
// Defaults: 127.0.0.1 9000 test/topic

#include <array>
#include <atomic>
#include <cstring>
#include <print>
#include <string_view>

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

// Returns false on connection close or unrecoverable error
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
    std::println("Subscriber starting...");
    const char*    host  = argc > 1 ? argv[1] : "127.0.0.1";
    const uint16_t port  = argc > 2 ? static_cast<uint16_t>(std::stoi(argv[2])) : 9000;
    const char*    topic = argc > 3 ? argv[3] : "test/topic";

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
    std::println("Connected to {}:{} — subscribing to \"{}\"", host, port, topic);

    // Send SUBSCRIBE
    std::array<std::byte, sizeof(FrameHeader) + MAX_TOPIC_LEN + sizeof(uint16_t)> sub_buf{};
    std::array<std::byte, sizeof(FrameHeader) + sizeof(uint64_t)> ack_buf{};
    uint64_t seq = 0;
    uint64_t expected_broker_seq = 0;
    {
        const auto result = encode_subscribe(sub_buf, ++seq, topic, MessageType::SUBSCRIBE);
        if (!result) {
            std::println(stderr, "encode_subscribe failed");
            close(fd);
            return 1;
        }
        if (!send_all(fd, sub_buf.data(), *result)) {
            std::println(stderr, "send SUBSCRIBE failed: {}", strerror(errno));
            close(fd);
            return 1;
        }
        std::println("Sent SUBSCRIBE seq={} topic=\"{}\"", seq, topic);

        // Wait for ACK
        if (!recv_all(fd, ack_buf.data(), sizeof(FrameHeader))) {
            std::println(stderr, "recv SUBSCRIBE ACK header failed: {}", strerror(errno));
            close(fd);
            return 1;
        }
        const auto ack_hdr = parse_header(std::span{ack_buf.data(), sizeof(FrameHeader)});
        if (!ack_hdr) {
            std::println(stderr, "parse SUBSCRIBE ACK header failed");
            close(fd);
            return 1;
        }
        ++expected_broker_seq;
        if (ack_hdr->sequence != expected_broker_seq) {
            std::println(stderr, "WARN: broker seq gap: expected={} got={}", expected_broker_seq, ack_hdr->sequence);
            expected_broker_seq = ack_hdr->sequence;
        }
        if (ack_hdr->payload_len > 0) {
            if (!recv_all(fd, ack_buf.data() + sizeof(FrameHeader), ack_hdr->payload_len)) {
                std::println(stderr, "recv SUBSCRIBE ACK payload failed: {}", strerror(errno));
                close(fd);
                return 1;
            }
        }
        const auto ack_frame = decode_frame(*ack_hdr, std::span{
            ack_buf.data() + sizeof(FrameHeader), ack_hdr->payload_len});
        if (!ack_frame) {
            std::println(stderr, "decode SUBSCRIBE ACK failed");
            close(fd);
            return 1;
        }
        bool subscribed = std::visit([&](const auto& msg) -> bool {
            using T = std::decay_t<decltype(msg)>;
            if constexpr (std::is_same_v<T, AckMsg>) {
                if (msg.acked_seq != seq) {
                    std::println(stderr, "SUBSCRIBE ACK seq mismatch: expected={} got={}", seq, msg.acked_seq);
                    return false;
                }
                std::println("SUBSCRIBE ACK received (acked_seq={})", msg.acked_seq);
                return true;
            } else if constexpr (std::is_same_v<T, ErrorMsg>) {
                std::println(stderr, "Broker rejected SUBSCRIBE: {} — {}",
                    to_string(msg.code), msg.error_msg);
                return false;
            } else {
                std::println(stderr, "Unexpected response to SUBSCRIBE (expected ACK)");
                return false;
            }
        }, ack_frame->payload);
        if (!subscribed) {
            close(fd);
            return 1;
        }
    }

    // Receive loop
    std::array<std::byte, sizeof(FrameHeader) + MAX_PAYLOAD_LEN> recv_buf{};
    uint64_t msg_count = 0;

    while (g_running.load(std::memory_order_relaxed)) {
        // Read header
        if (!recv_all(fd, recv_buf.data(), sizeof(FrameHeader))) break;

        const auto hdr_result = parse_header(std::span{recv_buf.data(), sizeof(FrameHeader)});
        if (!hdr_result) {
            std::println(stderr, "parse_header failed");
            break;
        }
        const FrameHeader& hdr = *hdr_result;
        ++expected_broker_seq;
        if (hdr.sequence != expected_broker_seq) {
            std::println(stderr, "WARN: broker seq gap: expected={} got={}", expected_broker_seq, hdr.sequence);
            expected_broker_seq = hdr.sequence;
        }

        // Read payload
        if (hdr.payload_len > 0) {
            if (!recv_all(fd, recv_buf.data() + sizeof(FrameHeader), hdr.payload_len)) break;
        }

        const auto frame_result = decode_frame(hdr, std::span{
            recv_buf.data() + sizeof(FrameHeader), hdr.payload_len});
        if (!frame_result) {
            std::println(stderr, "decode_frame failed");
            break;
        }

        std::visit([&](const auto& msg) {
            using T = std::decay_t<decltype(msg)>;
            if constexpr (std::is_same_v<T, PublishMsg>) {
                ++msg_count;
                const std::string_view body{
                    reinterpret_cast<const char*>(msg.body.data()), msg.body.size()};
                std::println(R"([{}] topic="{}" body="{}")", msg_count, msg.topic, body);
            } else if constexpr (std::is_same_v<T, AckMsg>) {
                std::println("ACK received (acked_seq={})", msg.acked_seq);
            } else if constexpr (std::is_same_v<T, ErrorMsg>) {
                std::println(stderr, "ERROR from broker: {} — {}",
                    to_string(msg.code), msg.error_msg);
                g_running.store(false, std::memory_order_relaxed);
            }
        }, frame_result->payload);
    }

    std::println("Disconnected. {} messages received.", msg_count);
    close(fd);
    return 0;
}
