/********************************* load_gen.cpp *********************************
* Broker load generator and latency benchmark
*
* Spawns N publisher threads + M subscriber threads over independent TCP connections.
* Iterates over every (payload_size, rate) pair from the config file.
* RTT is measured per-publisher as: time from send() to ACK fully received.
* Subscribers exist only to stress broker fan-out; they discard all received messages.
*
* Usage: ./load_gen <host> <port> <config.toml>
*
* Inline buffer threshold = 256 - 24 (header) - 2 (topic_len) - len(topic) bytes.
* For topic "test/topic" (10 chars): inline threshold = 220 bytes.
*/

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <print>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <toml++/toml.hpp>

#include <broker/protocol.hpp>

static std::atomic<bool>    g_running{false};
static std::atomic<bool>    g_collecting{false};
static std::atomic<int64_t> g_rate_per_pub{0};  // msg/s per publisher thread; 0 = unlimited

struct Config {
    std::string           host;
    uint16_t              port{};
    std::string           topic;
    int                   publisher_count{};
    int                   subscriber_count{};
    int                   warmup_secs{};
    int                   measure_secs{};
    bool                  max_throughput{};
    std::vector<int>      payload_sizes;
    std::vector<int64_t>  rate_list;
};

//  Per-thread state (cache-line padded to avoid false sharing)
struct alignas(64) PubState {
    // RTT in nanoseconds, collected during measure window. signed: guards against
    // any clock underflow (time travel)
    std::vector<int64_t> samples;
    int fd{-1};
};

struct alignas(64) SubState {
    int fd{-1};
};


//  Network helpers 
static bool send_all(int fd, const std::byte* data, size_t len) {
    for (size_t sent = 0; sent < len;) {
        const ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

static bool recv_all(int fd, std::byte* data, size_t len) {
    for (size_t got = 0; got < len;) {
        const ssize_t n = recv(fd, data + got, len - got, 0);
        if (n <= 0) return false;
        got += static_cast<size_t>(n);
    }
    return true;
}

static int make_connection(const Config& cfg) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(cfg.port);
    if (inet_pton(AF_INET, cfg.host.c_str(), &addr.sin_addr) <= 0) { close(fd); return -1; }
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) { close(fd); return -1; }
    return fd;
}

//  Statistics
struct Stats {
    int64_t min_us{}, p50_us{}, p99_us{}, p999_us{}, max_us{};
    size_t  count{};
};

static Stats compute_stats(std::vector<int64_t>& ns_samples) {
    if (ns_samples.empty()) return {};
    std::sort(ns_samples.begin(), ns_samples.end());
    const size_t n = ns_samples.size();
    const auto at = [&](double pct) {
        const size_t i = std::min(static_cast<size_t>(pct / 100.0 * static_cast<double>(n)), n - 1);
        return ns_samples[i] / 1000;
    };
    return { ns_samples.front() / 1000, at(50.0), at(99.0), at(99.9), ns_samples.back() / 1000, n };
}

//  Publisher thread 
static void run_publisher(const Config& cfg, std::span<const std::byte> payload, PubState& state) {
    state.fd = make_connection(cfg);
    if (state.fd < 0) {
        std::println(stderr, "publisher: connect failed: {}", strerror(errno));
        return;
    }

    std::array<std::byte, sizeof(FrameHeader) + MAX_PAYLOAD_LEN>  send_buf{};
    std::array<std::byte, sizeof(FrameHeader) + sizeof(uint64_t)> ack_buf{};
    uint64_t seq = 0;

    while (g_running.load(std::memory_order_relaxed)) {
        const int64_t rate = g_rate_per_pub.load(std::memory_order_relaxed);

        const auto result = encode_publish(send_buf, ++seq, cfg.topic, payload);
        if (!result) break;

        const auto t0 = std::chrono::steady_clock::now();
        if (!send_all(state.fd, send_buf.data(), *result)) break;

        if (!recv_all(state.fd, ack_buf.data(), sizeof(FrameHeader))) break;
        const auto hdr = parse_header(std::span{ack_buf.data(), sizeof(FrameHeader)});
        if (!hdr) break;
        if (hdr->payload_len > 0) {
            if (hdr->payload_len > ack_buf.size() - sizeof(FrameHeader)) break;
            if (!recv_all(state.fd, ack_buf.data() + sizeof(FrameHeader), hdr->payload_len)) break;
        }

        const int64_t rtt_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - t0).count();

        if (g_collecting.load(std::memory_order_relaxed))
            state.samples.push_back(rtt_ns);

        if (rate > 0)
            std::this_thread::sleep_until(t0 + std::chrono::nanoseconds(1'000'000'000LL / rate));
    }

    close(state.fd);
    state.fd = -1;
}

//  Subscriber thread 
static void run_subscriber(const Config& cfg, SubState& state) {
    state.fd = make_connection(cfg);
    if (state.fd < 0) {
        std::println(stderr, "subscriber: connect failed: {}", strerror(errno));
        return;
    }

    std::array<std::byte, sizeof(FrameHeader) + MAX_TOPIC_LEN + sizeof(uint16_t)> sub_buf{};
    std::array<std::byte, sizeof(FrameHeader) + sizeof(uint64_t)>                 ack_buf{};
    uint64_t seq = 0;

    const auto sub_result = encode_subscribe(sub_buf, ++seq, cfg.topic, MessageType::SUBSCRIBE);
    if (!sub_result || !send_all(state.fd, sub_buf.data(), *sub_result)) {
        close(state.fd); state.fd = -1; return;
    }

    if (!recv_all(state.fd, ack_buf.data(), sizeof(FrameHeader))) {
        close(state.fd); state.fd = -1; return;
    }
    const auto ack_hdr = parse_header(std::span{ack_buf.data(), sizeof(FrameHeader)});
    if (!ack_hdr) { close(state.fd); state.fd = -1; return; }
    if (ack_hdr->payload_len > 0)
        recv_all(state.fd, ack_buf.data() + sizeof(FrameHeader), ack_hdr->payload_len);

    // Drain received messages. only here to stress broker fan-out
    std::array<std::byte, sizeof(FrameHeader) + MAX_PAYLOAD_LEN> recv_buf{};
    while (g_running.load(std::memory_order_relaxed)) {
        if (!recv_all(state.fd, recv_buf.data(), sizeof(FrameHeader))) break;
        const auto hdr = parse_header(std::span{recv_buf.data(), sizeof(FrameHeader)});
        if (!hdr) break;
        if (hdr->payload_len > 0 &&
            !recv_all(state.fd, recv_buf.data() + sizeof(FrameHeader), hdr->payload_len)) break;
    }

    close(state.fd);
    state.fd = -1;
}

//  One benchmark block (single payload size) 
static void run_block(const Config& cfg, int payload_bytes,
                      const std::vector<int64_t>& rates) {
    std::vector<std::byte> payload(static_cast<size_t>(payload_bytes), std::byte{0x78}); // 'x'

    // Launch subscribers first; give them time to connect and subscribe
    std::vector<SubState>    sub_states(cfg.subscriber_count);
    std::vector<std::thread> sub_threads;
    sub_threads.reserve(cfg.subscriber_count);
    g_running = true;
    for (auto& s : sub_states)
        sub_threads.emplace_back(run_subscriber, std::cref(cfg), std::ref(s));
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Launch publishers
    std::vector<PubState>    pub_states(cfg.publisher_count);
    std::vector<std::thread> pub_threads;
    pub_threads.reserve(cfg.publisher_count);
    for (auto& s : pub_states)
        pub_threads.emplace_back(run_publisher, std::cref(cfg),
                                 std::span<const std::byte>{payload}, std::ref(s));

    for (const int64_t total_rate : rates) {
        const bool    unlimited = (total_rate == 0);
        const int64_t per_pub   = unlimited ? 0
                                            : std::max<int64_t>(1, total_rate / cfg.publisher_count);
        g_rate_per_pub = per_pub;
        g_collecting   = false;

        // Warmup
        std::this_thread::sleep_for(std::chrono::seconds(cfg.warmup_secs));
        for (auto& s : pub_states) s.samples.clear();

        // Measure window
        g_collecting       = true;
        const auto t_start = std::chrono::steady_clock::now();
        std::this_thread::sleep_for(std::chrono::seconds(cfg.measure_secs));
        g_collecting       = false;
        const double elapsed = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - t_start).count();

        // Collect samples from all publisher threads
        std::vector<int64_t> all_samples;
        for (auto& s : pub_states)
            all_samples.insert(all_samples.end(), s.samples.begin(), s.samples.end());

        const Stats   st       = compute_stats(all_samples);
        const int64_t achieved = st.count > 0
            ? static_cast<int64_t>(static_cast<double>(st.count) / elapsed) : 0;

        if (unlimited) {
            std::println(
                "  rate=    unlimited  "
                "min={:>7}us  p50={:>7}us  p99={:>7}us  p999={:>7}us  max={:>7}us  "
                "achieved={:>8} msg/s  n={}",
                st.min_us, st.p50_us, st.p99_us, st.p999_us, st.max_us, achieved, st.count);
        } else {
            std::println(
                "  rate={:>10} msg/s  "
                "min={:>7}us  p50={:>7}us  p99={:>7}us  p999={:>7}us  max={:>7}us  "
                "n={}",
                total_rate, st.min_us, st.p50_us, st.p99_us, st.p999_us, st.max_us, st.count);
        }
    }

    // Teardown
    g_running = false;
    for (auto& s : pub_states) if (s.fd >= 0) shutdown(s.fd, SHUT_RDWR);
    for (auto& t : pub_threads) t.join();
    for (auto& s : sub_states)  if (s.fd >= 0) shutdown(s.fd, SHUT_RDWR);
    for (auto& t : sub_threads) t.join();
}

//  Config loading 
static Config load_config(std::string_view path, std::string host, uint16_t port) {
    toml::table tbl;
    try {
        tbl = toml::parse_file(path);
    } catch (const toml::parse_error& e) {
        std::println(stderr, "Failed to parse config \"{}\": {}", path, e.description());
        std::exit(1);
    }

    const auto require_str = [&](std::string_view key) -> std::string {
        auto v = tbl[key].value<std::string>();
        if (!v) { std::println(stderr, "Config missing required string: {}", key); std::exit(1); }
        return *v;
    };
    const auto require_int = [&](std::string_view key) -> int {
        auto v = tbl[key].value<int>();
        if (!v) { std::println(stderr, "Config missing required int: {}", key); std::exit(1); }
        return *v;
    };
    const auto require_bool = [&](std::string_view key) -> bool {
        auto v = tbl[key].value<bool>();
        if (!v) { std::println(stderr, "Config missing required bool: {}", key); std::exit(1); }
        return *v;
    };

    Config cfg;
    cfg.host             = std::move(host);
    cfg.port             = port;
    cfg.topic            = require_str("topic");
    cfg.publisher_count  = require_int("publisher_count");
    cfg.subscriber_count = require_int("subscriber_count");
    cfg.warmup_secs      = require_int("warmup_secs");
    cfg.measure_secs     = require_int("measure_secs");
    cfg.max_throughput   = require_bool("max_throughput");

    const auto* payload_arr = tbl["payload_sizes"].as_array();
    if (!payload_arr || payload_arr->empty()) {
        std::println(stderr, "Config missing required non-empty array: payload_sizes");
        std::exit(1);
    }
    for (const auto& v : *payload_arr) {
        if (auto i = v.value<int>()) cfg.payload_sizes.push_back(*i);
        else { std::println(stderr, "payload_sizes must be integers"); std::exit(1); }
    }

    const auto* rate_arr = tbl["rate_list"].as_array();
    if (!rate_arr || rate_arr->empty()) {
        std::println(stderr, "Config missing required non-empty array: rate_list");
        std::exit(1);
    }
    for (const auto& v : *rate_arr) {
        if (auto i = v.value<int64_t>()) cfg.rate_list.push_back(*i);
        else { std::println(stderr, "rate_list must be integers"); std::exit(1); }
    }
    if (cfg.max_throughput)
        cfg.rate_list.push_back(0);  // unlimited phase appended last

    // Validation
    if (cfg.publisher_count <= 0 || cfg.subscriber_count <= 0) {
        std::println(stderr, "publisher_count and subscriber_count must be > 0"); std::exit(1);
    }
    if (cfg.warmup_secs <= 0 || cfg.measure_secs <= 0) {
        std::println(stderr, "warmup_secs and measure_secs must be > 0"); std::exit(1);
    }
    for (int s : cfg.payload_sizes) {
        if (s <= 0 || s > static_cast<int>(MAX_PAYLOAD_LEN)) {
            std::println(stderr, "payload_sizes: each value must be in [1, {}]", MAX_PAYLOAD_LEN);
            std::exit(1);
        }
    }
    for (int64_t r : cfg.rate_list) {
        if (r < 0) {
            std::println(stderr, "rate_list: rates must be >= 0 (0 = unlimited)"); std::exit(1);
        }
    }

    return cfg;
}

//  Entry point 
int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::println(stderr, "Usage: {} <host> <port> <config.toml>", argv[0]);
        return 1;
    }

    const std::string host   = argv[1];
    const uint16_t    port   = static_cast<uint16_t>(std::stoi(argv[2]));
    const Config      cfg    = load_config(argv[3], host, port);

    const int inline_threshold =
        256 - static_cast<int>(sizeof(FrameHeader)) - 2 - static_cast<int>(cfg.topic.size());

    std::println("load_gen  {}:{}  topic=\"{}\"  publishers={}  subscribers={}",
        cfg.host, cfg.port, cfg.topic, cfg.publisher_count, cfg.subscriber_count);
    std::println("warmup={}s  measure={}s  max_throughput={}",
        cfg.warmup_secs, cfg.measure_secs, cfg.max_throughput);
    std::println("OutboundMessage inline threshold: {} bytes payload (topic len={})\n",
        inline_threshold, cfg.topic.size());

    for (const int payload_bytes : cfg.payload_sizes) {
        const char* loc = payload_bytes <= inline_threshold ? "inline buf" : "heap buf";
        std::println("=== Payload: {} B ({}) ===", payload_bytes, loc);
        run_block(cfg, payload_bytes, cfg.rate_list);
        std::println();
    }

    return 0;
}
