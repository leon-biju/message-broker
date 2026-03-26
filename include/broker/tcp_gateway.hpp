#ifndef INCLUDE_BROKER_TCP_GATEWAY_HPP_
#define INCLUDE_BROKER_TCP_GATEWAY_HPP_

/*************************** tcp_gateway.hpp ***************************
 * This file contains the main TcpGateway class. It's responsibilities are twofold:
 *     - Listen to messages decode them and put them on the inbound queue
 *     - Send messages on the outbound queue
 */

#include <array>
#include <atomic>
#include <cstddef>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>
#include <thread>
#include <unistd.h>
#include <vector>
#include <netinet/in.h>

#include <concurrentqueue.h>
#include <blockingconcurrentqueue.h>
#include <broker/protocol.hpp>

inline constexpr size_t READ_BUF_SIZE    = sizeof(FrameHeader) + MAX_PAYLOAD_LEN;
inline constexpr size_t CONN_BUF_CAPACITY = 131072; // 128KiB, power of two

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

enum class ParseStage { AwaitingHeader, AwaitingPayload };

// Per-connection structure. Stores parsing state for recv loop and metadata for send loop.
struct Connection {

    // All messages are read into this buffer.
    // After the router consumes the message, the watermark is updated to indicate how many bytes have been consumed, 
    // and the recv loop can compact the buffer by moving remaining data to the front.
    std::unique_ptr<std::byte[]> buf{nullptr}; // allocated on accept, size = CONN_BUF_CAPACITY
    
    uint32_t    parse_pos     {0};   // start of unprocessed data
    uint32_t    write_pos     {0};   // end of valid data in buf
    ParseStage  stage         {ParseStage::AwaitingHeader};
    FrameHeader cached_header {};
    bool        active        {false};
};

// Decoded Message + sender fd + watermark for buffer lifetime signaling
struct InboundMessage { //NOLINT
    DecodedFrame frame;
    int sender_fd;
    std::atomic<uint32_t>* watermark_ptr{nullptr}; // null for Disconnect/Shutdown
    uint32_t consumed_up_to{0};                     // byte offset past this frame in conn buffer
};

// Pre-encoded frame + destination fd. SBO: most messages fit in the inline buffer; large publishes spill to heap.
struct OutboundMessage {
    static constexpr size_t INLINE_CAP = 256;

    std::array<std::byte, INLINE_CAP> inline_buf{};
    std::unique_ptr<std::byte[]> heap_buf{nullptr}; // large buffers 256B+ spill onto the heap
    uint32_t len{0};
    int      dest_fd{-1};

    OutboundMessage() = default;

    OutboundMessage(const OutboundMessage& o)
        : inline_buf(o.inline_buf), len(o.len), dest_fd(o.dest_fd) {
        if (o.heap_buf) {
            heap_buf = std::make_unique<std::byte[]>(o.len);
            std::memcpy(heap_buf.get(), o.heap_buf.get(), o.len);
        }
    }
    OutboundMessage& operator=(const OutboundMessage& o) {
        if (this != &o) {
            inline_buf = o.inline_buf;
            len = o.len;
            dest_fd = o.dest_fd;
            if (o.heap_buf) {
                heap_buf = std::make_unique<std::byte[]>(o.len);
                std::memcpy(heap_buf.get(), o.heap_buf.get(), o.len);
            } else {
                heap_buf.reset();
            }
        }
        return *this;
    }
    OutboundMessage(OutboundMessage&&) = default;
    OutboundMessage& operator=(OutboundMessage&&) = default;

    [[nodiscard]] const std::byte* data() const {
        return heap_buf ? heap_buf.get() : inline_buf.data();
    }

    // Given how many bytes required, return a writable buffer. Using inline buf if
    [[nodiscard]] std::span<std::byte> write_buf(const size_t needed) {
        if (needed <= INLINE_CAP) {
            return {inline_buf.data(), INLINE_CAP};
        }
        heap_buf = std::make_unique<std::byte[]>(needed);
        return {heap_buf.get(), needed};
    }
};

class TcpGateway {
    ListeningSocket listening_socket_;

    uint32_t max_connections_;
    int      pinned_cpu_core_;

    moodycamel::BlockingConcurrentQueue<InboundMessage>&  inbound_;
    moodycamel::BlockingConcurrentQueue<OutboundMessage>& outbound_;

    std::vector<Connection>            connections;
    std::vector<std::atomic_uint32_t>  consumer_watermark_; // fd-indexed, router stores after consuming
    std::vector<std::atomic_uint64_t>  outbound_seq_;       // fd-indexed, broker->client monotonic counter
    size_t                             active_connections_count_ {0};

    std::thread receiver_thread_;
    std::thread sender_thread_;

    int epoll_fd_ {-1};
    int wake_fd_  {-1}; // Shutdown signal for recv loop, unblocks epoll wait

    std::atomic_bool send_loop_running_ {true}; // Shutdown signal for send loop

public:
    TcpGateway(moodycamel::BlockingConcurrentQueue<InboundMessage>&  inbound,
               moodycamel::BlockingConcurrentQueue<OutboundMessage>& outbound,
               uint32_t max_connections, size_t fd_table_size, uint16_t port, int pinned_cpu_core);
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

    // Send error directly from the receiver thread
    // Using queue would be a race, since this thread will also close the fd upon errors and might end up sending on a closed fd
    void send_error_direct(int fd, ErrorCode code, std::string_view msg) noexcept;


    // Functions below run on sender thread
    void send_loop();
    void do_send(const OutboundMessage& msg);

};

#endif
