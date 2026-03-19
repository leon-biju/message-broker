/*************************** tcp_gateway.cpp ***************************
* This file contains implementation of all non-trivial TcpGateway functions
*/

#include <cerrno>
#include <cstring>
#include <print>
#include <pthread.h>
#include <sys/eventfd.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/resource.h>
#include <netinet/tcp.h>

#include <broker/tcp_gateway.hpp>

static constexpr int    MAX_EPOLL_EVENTS   = 64;
static constexpr size_t COMPACT_THRESHOLD  = READ_BUF_SIZE / 2;

TcpGateway::TcpGateway(const GatewayConfig& config, moodycamel::ConcurrentQueue<InboundMessage>& queue)
    : config_(config)
    , listening_socket_(ListeningSocket(config.port))
    , inbound_queue_(queue)
{
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) throw std::runtime_error("epoll_create1 failed");

    shutdown_fd_ = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (shutdown_fd_ < 0) {
        close(epoll_fd_);
        throw std::runtime_error("eventfd failed");
    }

    const rlimit rl {
        .rlim_cur = config.fd_table_size,
        .rlim_max = config.fd_table_size
    };
    setrlimit(RLIMIT_NOFILE, &rl);

    meta_.resize(config.fd_table_size);

    epoll_event ev{};
    ev.events  = EPOLLIN | EPOLLET;
    ev.data.fd = listening_socket_.fd();
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listening_socket_.fd(), &ev);

    ev.data.fd = shutdown_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, shutdown_fd_, &ev);
}

TcpGateway::~TcpGateway() {
    stop();
    if (shutdown_fd_ >= 0) close(shutdown_fd_);
    if (epoll_fd_ >= 0)    close(epoll_fd_);
}

void TcpGateway::start() {
    worker_ = std::thread([this] { run_loop(); });
}

void TcpGateway::stop() {
    if (!worker_.joinable()) return;
    const uint64_t val = 1;
    write(shutdown_fd_, &val, sizeof(val));
    worker_.join();
}

void TcpGateway::run_loop() {
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
            if (errno != EINTR) break;
            continue;
        }
        for (int i = 0; i < n; ++i) {
            const int fd = events[i].data.fd;
            if (fd == shutdown_fd_) return;

            if (fd == listening_socket_.fd()) {
                handle_accept();
            } else {
                if (events[i].events & (EPOLLHUP | EPOLLERR)) {
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
        const int client_fd = accept4(listening_socket_.fd(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            std::println(stderr, "accept4 error: {}", strerror(errno));
            break;
        }

        if (active_count_ >= config_.max_connections) {
            close(client_fd);
            continue;
        }

        const int nodelay = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));

        meta_[client_fd] = ConnMeta{};
        meta_[client_fd].active = true;
        ++active_count_;

        epoll_event ev{};
        ev.events  = EPOLLIN | EPOLLET;
        ev.data.fd = client_fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);
    }
}

void TcpGateway::handle_read(const int fd) {
    ConnMeta& conn = meta_[fd];
    while (true) {
        const size_t space = conn.buf.size() - conn.bytes_in_buf;
        if (space == 0) {
            handle_close(fd);
            return;
        }

        const ssize_t n = recv(fd, conn.buf.data() + conn.bytes_in_buf, space, 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            handle_close(fd);
            return;
        }
        if (n == 0) {
            handle_close(fd);
            return;
        }

        conn.bytes_in_buf += static_cast<size_t>(n);
        if (!try_dispatch_frames(fd)) return;
    }
}

bool TcpGateway::try_dispatch_frames(const int fd) {
    ConnMeta& conn = meta_[fd];
    while (true) {
        const size_t available = conn.bytes_in_buf - conn.read_offset;

        if (conn.stage == ParseStage::AwaitingHeader) {
            if (available < sizeof(FrameHeader)) break;

            const auto raw = std::span<const std::byte, sizeof(FrameHeader)>(
                conn.buf.data() + conn.read_offset, sizeof(FrameHeader));
            const auto result = parse_header(raw);
            if (!result) {
                handle_close(fd);
                return false;
            }
            conn.cached_header = *result;
            conn.stage = ParseStage::AwaitingPayload;
        }

        if (conn.stage == ParseStage::AwaitingPayload) {
            const size_t total = sizeof(FrameHeader) + conn.cached_header.payload_len;
            if (available < total) break;

            // TODO: decode_frame and enqueue once protocol layer is complete
            // auto payload = std::span(conn.buf.data() + conn.read_offset + sizeof(FrameHeader),
            //                          conn.cached_header.payload_len);
            // if (auto frame = decode_frame(conn.cached_header, payload))
            //     inbound_queue_.enqueue({ *frame, fd });

            conn.read_offset += total;
            conn.stage = ParseStage::AwaitingHeader;

            // Compact only when the dead prefix exceeds the threshold
            if (conn.read_offset >= COMPACT_THRESHOLD) {
                const size_t remaining = conn.bytes_in_buf - conn.read_offset;
                if (remaining > 0)
                    std::memmove(conn.buf.data(), conn.buf.data() + conn.read_offset, remaining);
                conn.bytes_in_buf = remaining;
                conn.read_offset  = 0;
            }
        }
    }
    return true;
}

void TcpGateway::handle_close(const int fd) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    meta_[fd].active = false;
    --active_count_;
    close(fd);
}
