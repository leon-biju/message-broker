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

static constexpr int    MAX_EPOLL_EVENTS   = 64;
static constexpr size_t COMPACT_THRESHOLD  = READ_BUF_SIZE / 2;

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

TcpGateway::TcpGateway(const GatewayConfig& config,
                       moodycamel::BlockingConcurrentQueue<InboundMessage>&  inbound_queue,
                       moodycamel::BlockingConcurrentQueue<OutboundMessage>& outbound_queue)
    : config_(config)
    , listening_socket_(ListeningSocket(config.port))
    , inbound_queue_(inbound_queue)
    , outbound_queue_(outbound_queue)
{
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) throw std::runtime_error("epoll_create1 failed");

    wake_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (wake_fd_ < 0) {
        close(epoll_fd_);
        throw std::runtime_error("eventfd failed");
    }

    const rlimit rl {
        .rlim_cur = config.fd_table_size,
        .rlim_max = config.fd_table_size
    };
    setrlimit(RLIMIT_NOFILE, &rl);

    connection_metadata_.resize(config.fd_table_size); // use resize() so it zeros all the slots as well

    epoll_event ev{};
    ev.events  = EPOLLIN | EPOLLET;
    ev.data.fd = listening_socket_.fd();
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listening_socket_.fd(), &ev);

    ev.data.fd = wake_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev);

    spdlog::info("tcp gateway listening on port={}", config.port);
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
        outbound_queue_.enqueue(OutboundMessage{ .dest_fd = -1 }); // sentinel: unblocks wait_dequeue_bulk
        sender_thread_.join();
        spdlog::debug("Sender thread on tcpgateway joined!");
    }
}

void TcpGateway::recv_loop() {
    if (config_.pinned_cpu_core >= 0) {
        cpu_set_t cpus{};
        CPU_ZERO(&cpus);
        CPU_SET(config_.pinned_cpu_core, &cpus);
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

        if (active_connections_count_ >= config_.max_connections) {
            spdlog::warn("max connections reached ({}/{}), rejecting fd={}",
                active_connections_count_, config_.max_connections, client_fd);
            close(client_fd);
            continue;
        }

        const int nodelay = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        connection_metadata_[client_fd] = ConnMeta{};
        connection_metadata_[client_fd].active = true;
        ++active_connections_count_;

        epoll_event ev{};
        ev.events  = EPOLLIN | EPOLLET;
        ev.data.fd = client_fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);

        char ip_buf[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &addr.sin_addr, ip_buf, sizeof(ip_buf));
        spdlog::info("client connected fd={} addr={}:{}", client_fd, ip_buf, ntohs(addr.sin_port));
    }
}

void TcpGateway::handle_read(const int fd) {
    ConnMeta& conn = connection_metadata_[fd];
    while (true) {
        const size_t space = conn.buf.size() - conn.bytes_in_buf;
        if (space == 0) {
            spdlog::info("fd={} recv buffer full, disconnecting", fd);
            handle_close(fd);
            return;
        }

        const ssize_t n = recv(fd, conn.buf.data() + conn.bytes_in_buf, space, 0);
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

        conn.bytes_in_buf += static_cast<size_t>(n);
        if (!try_dispatch_frames(fd)) return;
    }
}

bool TcpGateway::try_dispatch_frames(const int fd) {
    ConnMeta& conn = connection_metadata_[fd];
    while (true) {
        const size_t available = conn.bytes_in_buf - conn.read_offset;

        if (conn.stage == ParseStage::AwaitingHeader) {
            if (available < sizeof(FrameHeader)) break;

            const auto raw = std::span<const std::byte, sizeof(FrameHeader)>(
                conn.buf.data() + conn.read_offset, sizeof(FrameHeader));
            const auto result = parse_header(raw);
            if (!result) {
                spdlog::debug("fd={} header parse error: {}, disconnecting",
                    fd, parse_error_str(result.error()));
                handle_close(fd);
                return false;
            }
            conn.cached_header = *result;
            conn.stage = ParseStage::AwaitingPayload;
        }

        if (conn.stage == ParseStage::AwaitingPayload) {
            const size_t total = sizeof(FrameHeader) + conn.cached_header.payload_len;
            if (available < total) break;

            auto payload = std::span(conn.buf.data() + conn.read_offset + sizeof(FrameHeader),
                                     conn.cached_header.payload_len);
            auto frame = decode_frame(conn.cached_header, payload);
            if (frame) {
                inbound_queue_.enqueue({ *frame, fd });
            } else {
                spdlog::debug("fd={} frame decode error: {}", fd, parse_error_str(frame.error()));
            }

            conn.read_offset += total;
            conn.stage = ParseStage::AwaitingHeader;

            // Compact only when the dead prefix exceeds the threshold
            if (conn.read_offset >= COMPACT_THRESHOLD) {
                const size_t remaining = conn.bytes_in_buf - conn.read_offset;
                spdlog::debug("fd={} compacting buffer remaining={}", fd, remaining);
                if (remaining > 0)
                    std::memmove(conn.buf.data(), conn.buf.data() + conn.read_offset, remaining);
                conn.bytes_in_buf = remaining;
                conn.read_offset  = 0;
            }
        }
    }
    return true;
}

void TcpGateway::send_loop() {
    static constexpr size_t BATCH   = 32;

    OutboundMessage batch[BATCH];
    while (send_loop_running_.load(std::memory_order_relaxed)) {
        const size_t n = outbound_queue_.wait_dequeue_bulk(batch, BATCH);
        for (size_t i = 0; i < n; ++i) {

            if (batch[i].dest_fd == -1) {
                // sentinel: this is a shutdown signal. we still care about the rest of the queue tho so just ignore and finish up
                continue;
            }
            do_send(batch[i]);
        }
    }
}

void TcpGateway::do_send(const OutboundMessage& msg) { //NOLINT
    const std::byte* ptr = msg.data.data();
    size_t remaining     = msg.len;
    while (remaining > 0) {
        const ssize_t sent = send(msg.dest_fd, ptr, remaining, MSG_NOSIGNAL);
        if (sent < 0) {
            // EAGAIN: kernel send buffer full we will drop for now (post-MVP: buffer remainder)
            // EBADF/EPIPE/ECONNRESET: fd closed by read thread so discard silently
            return;
        }
        ptr       += static_cast<size_t>(sent);
        remaining -= static_cast<size_t>(sent);
    }
}

void TcpGateway::handle_close(const int fd) {

    // Enqueuing this message ensures the router will remove this fd from all future rounds
    inbound_queue_.enqueue(InboundMessage {
        .frame = DecodedFrame {
            .header = {},
            .payload = DisconnectMsg {}
        },
        .sender_fd = fd
    });

    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    connection_metadata_[fd].active = false;
    --active_connections_count_;
    close(fd);
}
