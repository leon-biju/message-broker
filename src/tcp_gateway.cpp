/*************************** tcp_gateway.cpp ***************************
* This file contains implementation of all non-trivial TcpGateway functions
*/

#include <cerrno>
#include <chrono>
#include <cstring>
#include <pthread.h>
#include <sys/eventfd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <netinet/tcp.h>
#include <netinet/in.h>

#include <spdlog/spdlog.h>

#include <broker/tcp_gateway.hpp>

static constexpr int MAX_EPOLL_EVENTS = 64;

//TODO: Move these guys to protocol
static std::string_view parse_error_str(ParseError e) {
    switch (e) {
        case ParseError::BadMagic:           return "BadMagic";
        case ParseError::UnsupportedVersion: return "UnsupportedVersion";
        case ParseError::UnknownMessageType: return "UnknownMessageType";
        case ParseError::UnknownFlags:       return "UnknownFlags";
        case ParseError::PayloadTooLarge:    return "PayloadTooLarge";
        case ParseError::BufferTooSmall:     return "BufferTooSmall";
    }
    return "Unknown";
}

static ErrorCode parse_error_to_error_code(const ParseError e) {
    switch (e) {
        case ParseError::UnsupportedVersion: return ErrorCode::UNSUPPORTED_VER;
        case ParseError::PayloadTooLarge:    return ErrorCode::PAYLOAD_TOO_LARGE;
        default:                             return ErrorCode::INVALID_FRAME;
    }
}

TcpGateway::TcpGateway(
    moodycamel::BlockingConcurrentQueue<InboundMessage>& inbound,
    OutboundTable& outbound,
    Metrics& metrics,
    uint32_t max_connections, size_t fd_table_size, uint16_t port, int pinned_cpu_core)
    : listening_socket_(port),
      max_connections_(max_connections),
      pinned_cpu_core_(pinned_cpu_core),
      inbound_(inbound),
      outbound_(outbound),
      metrics_(metrics),
      outbound_seq_(fd_table_size),
      connections(fd_table_size)
{

    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) throw std::runtime_error("epoll_create1 failed");

    wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (wake_fd_ < 0) {
        close(epoll_fd_);
        throw std::runtime_error("eventfd failed");
    }

    const rlimit rl {
        .rlim_cur = fd_table_size,
        .rlim_max = fd_table_size
    };
    setrlimit(RLIMIT_NOFILE, &rl);


    epoll_event ev{};
    ev.events  = EPOLLIN | EPOLLET;
    ev.data.fd = listening_socket_.fd();
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listening_socket_.fd(), &ev);

    ev.data.fd = wake_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev);

    spdlog::info("tcp gateway listening on port={}", port);
}

TcpGateway::~TcpGateway() {
    stop();
    if (wake_fd_ >= 0)  close(wake_fd_);
    if (epoll_fd_ >= 0) close(epoll_fd_);
}

void TcpGateway::start() {
    receiver_thread_ = std::thread([this] { recv_loop(); });
    sender_thread_ = std::thread([this] { send_loop(); });
}

void TcpGateway::stop() {
    if (receiver_thread_.joinable()) {
        const uint64_t val = 1;
        write(wake_fd_, &val, sizeof(val));
        receiver_thread_.join();
        spdlog::debug("Receiver thread on tcpgateway joined!");
    }
    if (sender_thread_.joinable()) {
        send_loop_running_.store(false, std::memory_order_relaxed);
        outbound_.dirty.enqueue(-1); // sentinel that unblocks wait_dequeue_bulk in send_loop
        sender_thread_.join();
        spdlog::debug("Sender thread on tcpgateway joined!");
    }
}

void TcpGateway::recv_loop() {
    if (pinned_cpu_core_ >= 0) {
        cpu_set_t cpus{};
        CPU_ZERO(&cpus);
        CPU_SET(pinned_cpu_core_, &cpus);
        pthread_setaffinity_np(pthread_self(), sizeof(cpus), &cpus);
    }

    std::array<epoll_event, MAX_EPOLL_EVENTS> events{};
    while (true) {
        const int n = epoll_wait(epoll_fd_, events.data(), MAX_EPOLL_EVENTS, -1);
        if (n < 0) {
            if (errno == EINTR) continue;
            spdlog::error("epoll_wait failed: {}", strerror(errno));
            break;
        }
        for (int i = 0; i < n; ++i) {
            const int fd = events[i].data.fd;
            if (fd == wake_fd_) {
                // shutdown signal, bye!
                return;
            }
            if (fd == listening_socket_.fd()) {
                handle_accept();
            } else {
                if (events[i].events & (EPOLLHUP | EPOLLERR)) {
                    spdlog::info("fd={} epoll HUP/ERR, disconnecting", fd);
                    handle_close(fd);
                } else {
                    handle_read(fd);
                }
            }
        }
    }
}

void TcpGateway::handle_accept() {
    while (true) {
        sockaddr_in addr{};
        socklen_t addrlen = sizeof(addr);
        const int client_fd = accept4(listening_socket_.fd(),
                                      reinterpret_cast<sockaddr*>(&addr), &addrlen,
                                      SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            spdlog::error("accept4 failed: {}", strerror(errno));
            break;
        }

        if (active_connections_count_ >= max_connections_) {
            spdlog::warn("max connections reached ({}/{}), rejecting fd={}",
                active_connections_count_, max_connections_, client_fd);
            metrics_.on_connection_rejected();
            close(client_fd);
            continue;
        }

        const int nodelay = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        auto& conn = connections[client_fd];
        conn = Connection{};
        conn.buf_state = std::make_shared<BufferState>();
        conn.active = true;
        outbound_seq_[client_fd].store(0, std::memory_order_relaxed);
        ++active_connections_count_;
        metrics_.on_connection_accepted();

        epoll_event ev{};
        ev.events  = EPOLLIN | EPOLLET;
        ev.data.fd = client_fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);

        char ip_buf[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &addr.sin_addr, ip_buf, sizeof(ip_buf));
        spdlog::info("client connected fd={} addr={}:{}", client_fd, ip_buf, ntohs(addr.sin_port));
    }
}

void compact_if_needed(Connection& conn) {
    const uint32_t wm = conn.buf_state->watermark.load(std::memory_order_acquire);
    if (wm > 0) {
        const uint32_t remaining = conn.write_pos - wm;
        if (remaining > 0) {
            std::memmove(conn.buf_state->buf.get(), conn.buf_state->buf.get() + wm, remaining);
        }
        conn.write_pos -= wm;
        conn.parse_pos -= wm;
        conn.buf_state->watermark.store(0, std::memory_order_relaxed);
    }
}

void TcpGateway::handle_read(const int fd) {
    Connection& conn = connections[fd];
    compact_if_needed(conn);

    while (true) {
        const size_t space = CONN_BUF_CAPACITY - conn.write_pos;
        if (space <= 0) {
            spdlog::info("fd={} recv buffer full (consumer not keeping up), disconnecting", fd);
            handle_close(fd);
            return;
        }

        const ssize_t n = recv(fd, conn.buf_state->buf.get() + conn.write_pos, space, 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            spdlog::info("fd={} recv error: {}, disconnecting", fd, strerror(errno));
            handle_close(fd);
            return;
        }
        if (n == 0) {
            spdlog::info("fd={} EOF, disconnecting", fd);
            handle_close(fd);
            return;
        }

        conn.write_pos += static_cast<uint32_t>(n);
        metrics_.on_bytes_received(static_cast<size_t>(n));
        if (!try_dispatch_frames(fd)) return;
    }
}

std::optional<ParseError> dispatch_frames_impl(
    const int fd, Connection& conn,
    moodycamel::BlockingConcurrentQueue<InboundMessage>& inbound)
{
    while (true) {
        const uint32_t available = conn.write_pos - conn.parse_pos;

        if (conn.stage == ParseStage::AwaitingHeader) {
            if (available < sizeof(FrameHeader)) break;

            const auto raw = std::span<const std::byte, sizeof(FrameHeader)>(
                conn.buf_state->buf.get() + conn.parse_pos, sizeof(FrameHeader));
            const auto result = parse_header(raw);
            if (!result) {
                return result.error();
            }
            conn.cached_header = *result;
            conn.stage = ParseStage::AwaitingPayload;
        }

        if (conn.stage == ParseStage::AwaitingPayload) {
            const uint32_t total = sizeof(FrameHeader) + conn.cached_header.payload_len;
            if (available < total) break;

            auto payload = std::span(conn.buf_state->buf.get() + conn.parse_pos + sizeof(FrameHeader),
                                     conn.cached_header.payload_len);
            auto frame = decode_frame(conn.cached_header, payload);
            if (frame) {
                conn.parse_pos += total;
                conn.stage = ParseStage::AwaitingHeader;

                inbound.enqueue(InboundMessage{
                    .frame = *frame,
                    .sender_fd = fd,
                    .buf_state = conn.buf_state,
                    .consumed_up_to = conn.parse_pos
                });
            } else {
                return frame.error();
            }
        }
    }
    return std::nullopt;
}

bool TcpGateway::try_dispatch_frames(const int fd) {
    const auto err = dispatch_frames_impl(fd, connections[fd], inbound_);
    if (err) {
        const auto reason = parse_error_str(*err);
        spdlog::debug("fd={} parse error: {}, disconnecting", fd, reason);
        metrics_.on_parse_error(reason);
        send_error_direct(fd, parse_error_to_error_code(*err), reason);
        handle_close(fd);
        return false;
    }
    return true;
}

void TcpGateway::send_loop() {
    // Dirty here refers to fds that have pending messages to be sent
    static constexpr size_t DIRTY_BATCH = 32;
    int dirty_fds[DIRTY_BATCH];

    while (send_loop_running_.load(std::memory_order_relaxed)) {
        const size_t n = outbound_.dirty.wait_dequeue_bulk(dirty_fds, DIRTY_BATCH);
        for (size_t i = 0; i < n; ++i) {
            const int fd = dirty_fds[i];
            if (fd == -1) continue; // shutdown sentinel

            // Drain all pending messages from this fd's ring buffer
            // this does mean that we may get out-of-order delivery across different fds, but messages for the same fd will be in order, which is what matters most
            while (auto msg = outbound_.queues[fd].try_dequeue()) {
                // Stamp broker->client outbound sequence just before sending it out
                std::byte* buf_ptr = msg->data();
                write_sequence({buf_ptr, msg->len}, outbound_seq_[fd].fetch_add(1, std::memory_order_relaxed) + 1);
                do_send(fd, *msg);
            }
        }
    }
}

void TcpGateway::do_send(const int fd, const OutboundMessage& msg) { //NOLINT
    const std::byte* ptr = msg.data();
    size_t remaining     = msg.len;
    while (remaining > 0) {
        const ssize_t sent = send(fd, ptr, remaining, MSG_NOSIGNAL);
        if (sent < 0) {
            // EAGAIN: kernel send buffer full we will drop for now (post-MVP: buffer remainder)
            // EBADF/EPIPE/ECONNRESET: fd closed by read thread so discard silently
            return;
        }
        metrics_.on_bytes_sent(static_cast<size_t>(sent));
        ptr       += static_cast<size_t>(sent);
        remaining -= static_cast<size_t>(sent);
    }
}

void TcpGateway::send_error_direct(const int fd, const ErrorCode code, const std::string_view msg) noexcept {
    std::array<std::byte, sizeof(FrameHeader) + sizeof(ErrorCode) + sizeof(uint16_t) + 256> buf{};
    const auto result = encode_error(buf, /*seq=*/0, code, msg);
    if (!result) return;

    write_sequence({buf.data(), *result},
                   outbound_seq_[fd].fetch_add(1, std::memory_order_relaxed) + 1);

    const std::byte* ptr = buf.data();
    size_t remaining     = *result;
    while (remaining > 0) {
        const ssize_t sent = send(fd, ptr, remaining, MSG_NOSIGNAL);
        if (sent <= 0) return;
        ptr       += static_cast<size_t>(sent);
        remaining -= static_cast<size_t>(sent);
    }
}

void TcpGateway::handle_close(const int fd) {
    // Reset the connection's shared_ptr (ref--;). Any InboundMessages already in the queue hold their
    // own shared_ptr copies of BufferState, so the buffer and watermark stay alive until necessary
    connections[fd].buf_state.reset();

    inbound_.enqueue(InboundMessage {
        .frame = DecodedFrame {
            .header = {},
            .payload = DisconnectMsg {}
        },
        .sender_fd = fd,
        .buf_state = nullptr, // We could pass the shared_ptr here but the router doesn't need to write into it, so safe to delete before
        .consumed_up_to = 0
    });

    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    connections[fd].active = false;
    --active_connections_count_;
    metrics_.on_connection_closed();
    close(fd);
}
