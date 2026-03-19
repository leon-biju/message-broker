#include <array>
#include <cstring>
#include <iostream>
#include <print>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

#include <broker/protocol.hpp>


static bool send_all(const int fd, const std::byte* data, const size_t len) {
    size_t sent = 0;
    while (sent < len) {
        const ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

int main() {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::println(stderr, "socket() failed: {}", strerror(errno));
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(9000);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::println(stderr, "connect() failed: {}", strerror(errno));
        close(fd);
        return 1;
    }
    std::println("Connected to localhost:9000");

    std::array<std::byte, 1024> buf{};
    uint64_t seq = 0;

    // 1. SUBSCRIBE to "test/topic"
    {
        const auto res = encode_subscribe(buf, ++seq, "test/topic", MessageType::SUBSCRIBE);
        if (!res) {
            std::println(stderr, "encode_subscribe failed");
            close(fd);
            return 1;
        }
        send_all(fd, buf.data(), *res);
        std::println("Sent SUBSCRIBE seq={} topic=\"test/topic\"", seq);
    }

    // 2. PUBLISH to "test/topic" with payload "hello broker"
    {
        const std::string_view payload_str = "hello broker";
        const auto body = std::as_bytes(std::span(payload_str));
        const auto result = encode_publish(buf, ++seq, "test/topic", body);
        if (!result) { std::println(stderr, "encode_publish failed"); close(fd); return 1; }
        send_all(fd, buf.data(), *result);
        std::println(R"(Sent PUBLISH   seq={} topic="test/topic" payload="hello broker")", seq);
    }

    // 3. UNSUBSCRIBE from "test/topic"
    {
        const auto res = encode_subscribe(buf, ++seq, "test/topic", MessageType::UNSUBSCRIBE);
        if (!res) { std::println(stderr, "encode_unsubscribe failed"); close(fd); return 1; }
        send_all(fd, buf.data(), *res);
        std::println("Sent UNSUBSCRIBE seq={} topic=\"test/topic\"", seq);
    }

    std::println("All messages sent, closing connection.");
    close(fd);
    return 0;
}
