#ifndef INCLUDE_BROKER_TCP_GATEWAY_HPP_
#define INCLUDE_BROKER_TCP_GATEWAY_HPP_

/*************************** tcp_gateway.hpp ***************************
 * This file contains the main TcpGateway class, it's main responsibility is to listen
 * and place all decoded messages into a queue to be dealt with by owner of TcpGateway
 */

#include <array>
#include <cstddef>
#include <print>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include <vector>
#include <netinet/in.h>

#include <concurrentqueue.h>
#include <broker/protocol.hpp>

inline constexpr size_t READ_BUF_SIZE = sizeof(FrameHeader) + MAX_PAYLOAD_LEN;

// Lightweight RAII wrapper for listening socket
class ListeningSocket {
    int fd_;
public:
    explicit ListeningSocket(const uint16_t port) {
        fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (fd_ < 0) throw std::runtime_error("socket() failed");
        const int opt = 1;
        setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_port        = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;

        if (bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            close(fd_);
            throw std::runtime_error("bind() failed");
        }
        if (listen(fd_, SOMAXCONN) < 0) {
            close(fd_);
            throw std::runtime_error("listen() failed");
        }
        std::println("Constructed and listening on fd {}", fd_);
    }
    ListeningSocket(const ListeningSocket&)            = delete;
    ListeningSocket& operator=(const ListeningSocket&) = delete;
    ListeningSocket(ListeningSocket&& other) noexcept : fd_(other.fd_) { other.fd_ = -1; }
    ~ListeningSocket() { if (fd_ >= 0) close(fd_); }
    [[nodiscard]] int fd() const { return fd_; }
};


// Configuration settings for the TcpGateway class
struct GatewayConfig {
    uint32_t max_connections;    // actual client cap
    uint32_t fd_table_size;      // rlimit + meta_ vector size
    uint16_t port;
    int      pinned_cpu_core;    // -1 to disable pinning
};

enum class ParseStage { AwaitingHeader, AwaitingPayload };

// Per-connection metadata. TODO: move buf into a separate buffer pool (hot/cold split)
struct ConnMeta {
    std::array<std::byte, READ_BUF_SIZE> buf{}; //HELL NO THIS IS AWFUL USE OF STACK
    size_t      bytes_in_buf  {0};   // write cursor: end of valid data in buf
    size_t      read_offset   {0};   // parse cursor: start of unprocessed data
    ParseStage  stage         {ParseStage::AwaitingHeader};
    FrameHeader cached_header {};
    bool        active        {false};
};

// Decoded Message + sender fd
struct InboundMessage { DecodedFrame frame; int sender_fd; }; // NOLINT

class TcpGateway {
    GatewayConfig   config_;
    ListeningSocket listening_socket_;
    int             epoll_fd_    { -1 };
    int             shutdown_fd_ { -1 }; // used so that epoll_wait is unblocked then checked in run_loop()

    moodycamel::ConcurrentQueue<InboundMessage>& inbound_queue_;
    std::vector<ConnMeta> meta_;
    size_t                active_count_ { 0 };
    std::thread   worker_;

public:
    TcpGateway(const GatewayConfig& config, moodycamel::ConcurrentQueue<InboundMessage>& queue);
    ~TcpGateway();
    TcpGateway(const TcpGateway&)            = delete;
    TcpGateway& operator=(const TcpGateway&) = delete;

    // Spawn the main thread that will run_loop()
    void start();

    // Unblock epoll wait and sends shutdown signal to thread
    void stop();

private:

    // Functions below are run on an independent thread separate from the public api
    void run_loop();
    void handle_accept();
    void handle_read(int fd);
    void handle_close(int fd);
    bool try_dispatch_frames(int fd);
};

#endif
