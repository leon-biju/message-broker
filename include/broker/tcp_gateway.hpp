#ifndef INCLUDE_BROKER_TCP_GATEWAY_HPP_
#define INCLUDE_BROKER_TCP_GATEWAY_HPP_

/*************************** tcp_gateway.hpp ***************************
 * This file contains the main TcpGateway class. It's responsibilities are twofold:
 *     - Listen to messages decode them and put them on the inbound queue
 *     - Send messages on the outbound queue
 */

#include <array>
#include <cstddef>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include <vector>
#include <netinet/in.h>

#include <concurrentqueue.h>
#include <blockingconcurrentqueue.h>
#include <broker/protocol.hpp>

inline constexpr size_t READ_BUF_SIZE = sizeof(FrameHeader) + MAX_PAYLOAD_LEN;

// Lightweight RAII wrapper for listening socket, probably don't need this tbh, might remove in later versions
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
    uint32_t fd_table_size;      // rlimit + connection_metadata_ vector size
    uint16_t port;
    int      pinned_cpu_core;    // -1 to disable pinning
};

enum class ParseStage { AwaitingHeader, AwaitingPayload };

// Per-connection metadata. TODO: move buf into a separate buffer pool (hot/cold split)
struct ConnMeta {
    //This buf is what the messages rely on to keep the string view alive
    // This MUST outlive all messages that rely on this before they get processed and die
    std::array<std::byte, READ_BUF_SIZE> buf{}; //HELL NO THIS IS AWFUL USE OF STACK
    size_t      bytes_in_buf  {0};   // write cursor: end of valid data in buf
    size_t      read_offset   {0};   // parse cursor: start of unprocessed data
    ParseStage  stage         {ParseStage::AwaitingHeader};
    FrameHeader cached_header {};
    bool        active        {false};
};

// Decoded Message + sender fd
struct InboundMessage { DecodedFrame frame; int sender_fd; }; // NOLINT

// Pre-encoded frame + destination fd. Zero-allocation: router encodes into a local instance and enqueues by value.
struct OutboundMessage {
    std::array<std::byte, READ_BUF_SIZE> data; // pre-encoded wire frame, this is also very bad and disgusting
    size_t  len;    // how many bytes to actually send [0..len)
    int     dest_fd; // destination client socket
};

class TcpGateway {
    GatewayConfig   config_;
    ListeningSocket listening_socket_;

    // Queues used to collect messages coming in and send messages out
    moodycamel::ConcurrentQueue<InboundMessage>&          inbound_queue_;
    moodycamel::BlockingConcurrentQueue<OutboundMessage>& outbound_queue_;

    std::vector<ConnMeta> connection_metadata_;
    size_t                active_connections_count_ {0};

    std::thread receiver_thread_;
    std::thread sender_thread_;

    int epoll_fd_{-1};
    int wake_fd_ {-1}; // Shutdown signal for recv loop, unblocks the epoll wait as well
    std::atomic_bool send_loop_running_ { false }; // Shutdown signal for send loop

public:
    TcpGateway(const GatewayConfig& config,
               moodycamel::ConcurrentQueue<InboundMessage>&          inbound_queue,
               moodycamel::BlockingConcurrentQueue<OutboundMessage>& outbound_queue);
    ~TcpGateway();
    TcpGateway(const TcpGateway&)            = delete;
    TcpGateway& operator=(const TcpGateway&) = delete;

    // Spawn the main thread that will run_loop()
    void start();

    // Unblock epoll wait and sends shutdown signal to thread
    void stop();

private:

    // Functions below are run on receiver thread
    void recv_loop();
    void handle_accept();
    void handle_read(int fd);
    void handle_close(int fd);
    bool try_dispatch_frames(int fd);

    // Functions below run on sender thread
    void send_loop();
    void do_send(const OutboundMessage& msg);
};

#endif
