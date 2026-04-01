/********************************* stress_test.cpp *********************************
 * Multi-topic stress test for the message broker.
 *
 * Spawns N publisher threads + M subscriber threads over independent TCP connections
 * Each publisher round-robins across its configured topics
 * Each subscriber subscribes to multiple topics on a single connection
 * Optional connection churn threads cycle connect/subscribe/receive/disconnect
 *
 * Publishers embed timestamp in payload so subscribers can measure
 * end-to-end latency (same-host assumption — both use steady_clock).
 *
 * Usage: ./stress_test <host> <port> <config.toml>
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
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <toml++/toml.hpp>

#include <broker/protocol.hpp>

static std::atomic<bool> g_running{false};
static std::atomic<bool> g_collecting{false};



struct PubConfig {
    std::vector<std::string> topics;
    int                      payload_bytes{};
    int64_t                  rate_hz{};       // 0=unlimited
};

struct SubConfig {
    std::vector<std::string> topics;
};

struct ChurnConfig {
    bool                     enabled{false};
    int                      connections{0};
    int                      interval_ms{500};
    std::vector<std::string> subscribe_topics;
};

struct StressConfig {
    std::string              host;
    uint16_t                 port{};
    int                      warmup_secs{};
    int                      measure_secs{};
    std::vector<std::string> topics;
    std::vector<PubConfig>   publishers;
    std::vector<SubConfig>   subscribers;
    ChurnConfig              churn;
};



// Per-thread states
struct alignas(64) PubState {
    std::vector<int64_t> rtt_samples;
    uint64_t             sent{0};
    int                  fd{-1};
};

struct alignas(64) SubState {
    struct TopicStats {
        uint64_t received{0};
        uint64_t seq_gaps{0};
    };
    std::unordered_map<std::string, TopicStats> per_topic;
    std::vector<int64_t> e2e_samples;
    uint64_t             total_received{0};
    int                  fd{-1};
};

struct alignas(64) ChurnState {
    uint64_t reconnects{0};
    uint64_t failures{0};
};



// Payload metadata layout (first 24 bytes of every publish body)
// [0..7]   publisher_id   (uint64_t)
// [8..15]  sequence       (uint64_t)
// [16..23] timestamp_ns   (int64_t, steady_clock)
static constexpr size_t PAYLOAD_META_SIZE = 24;



// Network helpers (same as load_gen)
static bool send_all(int fd, const std::byte* data, size_t len) {
    for (size_t sent = 0; sent < len;) {
        const ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            return false;
        }
        sent += static_cast<size_t>(n);
    }
    return true;
}

static bool recv_all(int fd, std::byte* data, size_t len) {
    for (size_t got = 0; got < len;) {
        const ssize_t n = recv(fd, data + got, len - got, 0);
        if (n <= 0) {
            return false;
        }
        got += static_cast<size_t>(n);
    }
    return true;
}

static int make_connection(const std::string& host, uint16_t port) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        close(fd);
        return -1;
    }
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}



// Statistics (same as load_gen)
struct Stats {
    int64_t min_us{}, p50_us{}, p99_us{}, p999_us{}, max_us{};
    size_t  count{};
};

static Stats compute_stats(std::vector<int64_t>& ns_samples) {
    if (ns_samples.empty()) {
        return {};
    }
    std::sort(ns_samples.begin(), ns_samples.end());
    const size_t n = ns_samples.size();
    const auto at = [&](double pct) {
        const size_t i = std::min(static_cast<size_t>(pct / 100.0 * static_cast<double>(n)), n - 1);
        return ns_samples[i] / 1000;
    };
    return { ns_samples.front() / 1000, at(50.0), at(99.0), at(99.9), ns_samples.back() / 1000, n };
}



// Sends SUBSCRIBE for one topic and waits for ACK. Returns false on failure.
static bool subscribe_to_topic(int fd, uint64_t& seq, std::string_view topic) {
    std::array<std::byte, sizeof(FrameHeader) + MAX_TOPIC_LEN + sizeof(uint16_t)> sub_buf{};
    std::array<std::byte, sizeof(FrameHeader) + sizeof(uint64_t)>                 ack_buf{};

    const auto result = encode_subscribe(sub_buf, ++seq, topic, MessageType::SUBSCRIBE);
    if (!result || !send_all(fd, sub_buf.data(), *result)) {
        return false;
    }

    if (!recv_all(fd, ack_buf.data(), sizeof(FrameHeader))) {
        return false;
    }
    const auto hdr = parse_header(std::span{ack_buf.data(), sizeof(FrameHeader)});
    if (!hdr) {
        return false;
    }
    if (hdr->payload_len > 0) {
        recv_all(fd, ack_buf.data() + sizeof(FrameHeader), hdr->payload_len);
    }
    return true;
}

// Publisher thread
static void run_publisher(const StressConfig& cfg, const PubConfig& pub_cfg,
                          uint64_t publisher_id, PubState& state) {
    state.fd = make_connection(cfg.host, cfg.port);
    if (state.fd < 0) {
        std::println(stderr, "publisher {}: connect failed: {}", publisher_id, strerror(errno));
        return;
    }

    const size_t payload_bytes = static_cast<size_t>(pub_cfg.payload_bytes);
    std::vector<std::byte> payload(payload_bytes, std::byte{0x78});

    std::array<std::byte, sizeof(FrameHeader) + MAX_PAYLOAD_LEN>  send_buf{};
    std::array<std::byte, sizeof(FrameHeader) + sizeof(uint64_t)> ack_buf{};
    uint64_t seq = 0;
    size_t topic_idx = 0;

    // Embed publisher_id (constant for this thread)
    std::memcpy(payload.data(), &publisher_id, sizeof(publisher_id));

    while (g_running.load(std::memory_order_relaxed)) {
        // Round-robin topic selection
        const auto& topic = pub_cfg.topics[topic_idx];
        topic_idx = (topic_idx + 1) % pub_cfg.topics.size();

        ++seq;

        // Stamp per-publisher sequence and timestamp into payload
        std::memcpy(payload.data() + 8, &seq, sizeof(seq));
        const int64_t ts = std::chrono::steady_clock::now().time_since_epoch().count();
        std::memcpy(payload.data() + 16, &ts, sizeof(ts));

        const auto result = encode_publish(send_buf, seq, topic,
            std::span<const std::byte>{payload.data(), payload_bytes});
        if (!result) {
            break;
        }

        const auto t0 = std::chrono::steady_clock::now();
        if (!send_all(state.fd, send_buf.data(), *result)) {
            break;
        }

        // Wait for ACK
        if (!recv_all(state.fd, ack_buf.data(), sizeof(FrameHeader))) {
            break;
        }
        const auto hdr = parse_header(std::span{ack_buf.data(), sizeof(FrameHeader)});
        if (!hdr) {
            break;
        }
        if (hdr->payload_len > 0) {
            if (hdr->payload_len > ack_buf.size() - sizeof(FrameHeader)) {
                break;
            }
            if (!recv_all(state.fd, ack_buf.data() + sizeof(FrameHeader), hdr->payload_len)) {
                break;
            }
        }

        const int64_t rtt_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - t0).count();

        state.sent++;

        if (g_collecting.load(std::memory_order_relaxed)) {
            state.rtt_samples.push_back(rtt_ns);
        }

        if (pub_cfg.rate_hz > 0) {
            std::this_thread::sleep_until(t0 + std::chrono::nanoseconds(1'000'000'000LL / pub_cfg.rate_hz));
        }
    }

    close(state.fd);
    state.fd = -1;
}

//  Subscriber thread

static void run_subscriber(const StressConfig& cfg, const SubConfig& sub_cfg, SubState& state) {
    state.fd = make_connection(cfg.host, cfg.port);
    if (state.fd < 0) {
        std::println(stderr, "subscriber: connect failed: {}", strerror(errno));
        return;
    }

    // Subscribe to all configured topics
    uint64_t seq = 0;
    for (const auto& topic : sub_cfg.topics) {
        if (!subscribe_to_topic(state.fd, seq, topic)) {
            std::println(stderr, "subscriber: failed to subscribe to \"{}\"", topic);
            close(state.fd);
            state.fd = -1;
            return;
        }
        state.per_topic[topic] = {};
    }

    // Receive loop — decode topic + embedded metadata from each PUBLISH
    std::array<std::byte, sizeof(FrameHeader) + MAX_PAYLOAD_LEN> recv_buf{};

    while (g_running.load(std::memory_order_relaxed)) {
        if (!recv_all(state.fd, recv_buf.data(), sizeof(FrameHeader))) {
            break;
        }
        const auto hdr = parse_header(std::span{recv_buf.data(), sizeof(FrameHeader)});
        if (!hdr) {
            break;
        }
        if (hdr->payload_len > 0 &&
            !recv_all(state.fd, recv_buf.data() + sizeof(FrameHeader), hdr->payload_len)) {
            break;
        }

        // Only process PUBLISH frames
        if (static_cast<MessageType>(hdr->type) != MessageType::PUBLISH) {
            continue;
        }

        // Extract topic from payload: [topic_len (2B)] [topic bytes] [body...]
        const std::byte* payload_ptr = recv_buf.data() + sizeof(FrameHeader);
        uint16_t topic_len = 0;
        std::memcpy(&topic_len, payload_ptr, sizeof(topic_len));

        if (topic_len > hdr->payload_len - sizeof(topic_len)) { // malformed
            continue;
        }

        std::string_view topic(reinterpret_cast<const char*>(payload_ptr + sizeof(topic_len)), topic_len);
        const std::byte* body_ptr = payload_ptr + sizeof(topic_len) + topic_len;
        const size_t body_len = hdr->payload_len - sizeof(topic_len) - topic_len;

        state.total_received++;

        // Update per-topic stats
        std::string topic_str(topic);
        auto it = state.per_topic.find(topic_str);
        if (it != state.per_topic.end()) {
            it->second.received++;
        }

        // Extract e2e latency from embedded timestamp if body is large enough
        if (body_len >= PAYLOAD_META_SIZE && g_collecting.load(std::memory_order_relaxed)) {
            int64_t send_ts = 0;
            std::memcpy(&send_ts, body_ptr + 16, sizeof(send_ts));
            const int64_t now_ns = std::chrono::steady_clock::now().time_since_epoch().count();
            const int64_t e2e_ns = now_ns - send_ts;
            if (e2e_ns > 0) {
                state.e2e_samples.push_back(e2e_ns);
            }
        }
    }

    close(state.fd);
    state.fd = -1;
}

//  Churn thread

static void run_churn(const StressConfig& cfg, const ChurnConfig& churn_cfg, ChurnState& state) {
    std::array<std::byte, sizeof(FrameHeader) + MAX_PAYLOAD_LEN> recv_buf{};

    while (g_running.load(std::memory_order_relaxed)) {
        int fd = make_connection(cfg.host, cfg.port);
        if (fd < 0) {
            state.failures++;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
        }

        // Subscribe to churn topics
        uint64_t seq = 0;
        bool sub_ok = true;
        for (const auto& topic : churn_cfg.subscribe_topics) {
            if (!subscribe_to_topic(fd, seq, topic)) {
                sub_ok = false;
                break;
            }
        }

        if (!sub_ok) {
            close(fd);
            state.failures++;
            continue;
        }

        state.reconnects++;

        // Receive for interval_ms then disconnect
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(churn_cfg.interval_ms);

        // Set a receive timeout so we don't block past the deadline
        timeval tv{};
        tv.tv_sec  = churn_cfg.interval_ms / 1000;
        tv.tv_usec = (churn_cfg.interval_ms % 1000) * 1000;
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

        while (g_running.load(std::memory_order_relaxed) &&
               std::chrono::steady_clock::now() < deadline) {
            if (!recv_all(fd, recv_buf.data(), sizeof(FrameHeader))) {
                break;
            }
            const auto hdr = parse_header(std::span{recv_buf.data(), sizeof(FrameHeader)});
            if (!hdr) {
                break;
            }
            if (hdr->payload_len > 0) {
                recv_all(fd, recv_buf.data() + sizeof(FrameHeader), hdr->payload_len);
            }
        }

        close(fd);
    }
}

//  Config loading

static std::vector<std::string> parse_string_array(const toml::array* arr, std::string_view key) {
    std::vector<std::string> result;
    if (!arr || arr->empty()) {
        std::println(stderr, "Config: {} must be a non-empty array of strings", key);
        std::exit(1);
    }
    for (const auto& v : *arr) {
        if (auto s = v.value<std::string>()) {
            result.push_back(*s);
        } else {
            std::println(stderr, "Config: {} must contain only strings", key);
            std::exit(1);
        }
    }
    return result;
}

static StressConfig load_config(std::string_view path, std::string host, uint16_t port) {
    toml::table tbl;
    try {
        tbl = toml::parse_file(path);
    } catch (const toml::parse_error& e) {
        std::println(stderr, "Failed to parse config \"{}\": {}", path, e.description());
        std::exit(1);
    }

    StressConfig cfg;
    cfg.host = std::move(host);
    cfg.port = port;

    // [general]
    const auto* general = tbl["general"].as_table();
    if (!general) {
        std::println(stderr, "Config missing [general] section");
        std::exit(1);
    }
    auto warmup  = (*general)["warmup_secs"].value<int>();
    auto measure = (*general)["measure_secs"].value<int>();
    if (!warmup || !measure || *warmup <= 0 || *measure <= 0) {
        std::println(stderr, "Config: warmup_secs and measure_secs must be positive integers");
        std::exit(1);
    }
    cfg.warmup_secs  = *warmup;
    cfg.measure_secs = *measure;

    // topics
    cfg.topics = parse_string_array(tbl["topics"].as_array(), "topics");
    std::unordered_set<std::string> valid_topics(cfg.topics.begin(), cfg.topics.end());

    auto validate_topics = [&](const std::vector<std::string>& topics, std::string_view context) {
        for (const auto& t : topics) {
            if (!valid_topics.contains(t)) {
                std::println(stderr, "Config: {} references unknown topic \"{}\"", context, t);
                std::exit(1);
            }
        }
    };

    // [[publisher]]
    if (const auto* pub_arr = tbl["publisher"].as_array()) {
        for (const auto& entry : *pub_arr) {
            const auto* pub_tbl = entry.as_table();
            if (!pub_tbl) {
                std::println(stderr, "Config: [[publisher]] entries must be tables");
                std::exit(1);
            }

            PubConfig pc;
            pc.topics = parse_string_array((*pub_tbl)["topics"].as_array(), "publisher.topics");
            validate_topics(pc.topics, "[[publisher]]");

            auto pb = (*pub_tbl)["payload_bytes"].value<int>();
            if (!pb || *pb < static_cast<int>(PAYLOAD_META_SIZE) || *pb > static_cast<int>(MAX_PAYLOAD_LEN)) {
                std::println(stderr, "Config: publisher payload_bytes must be in [{}, {}]",
                    PAYLOAD_META_SIZE, MAX_PAYLOAD_LEN);
                std::exit(1);
            }
            pc.payload_bytes = *pb;

            auto rate = (*pub_tbl)["rate_hz"].value<int64_t>();
            if (!rate || *rate < 0) {
                std::println(stderr, "Config: publisher rate_hz must be >= 0");
                std::exit(1);
            }
            pc.rate_hz = *rate;

            cfg.publishers.push_back(std::move(pc));
        }
    }
    if (cfg.publishers.empty()) {
        std::println(stderr, "Config: at least one [[publisher]] is required");
        std::exit(1);
    }

    // [[subscriber]]
    if (const auto* sub_arr = tbl["subscriber"].as_array()) {
        for (const auto& entry : *sub_arr) {
            const auto* sub_tbl = entry.as_table();
            if (!sub_tbl) {
                std::println(stderr, "Config: [[subscriber]] entries must be tables");
                std::exit(1);
            }

            SubConfig sc;
            sc.topics = parse_string_array((*sub_tbl)["topics"].as_array(), "subscriber.topics");
            validate_topics(sc.topics, "[[subscriber]]");
            cfg.subscribers.push_back(std::move(sc));
        }
    }
    if (cfg.subscribers.empty()) {
        std::println(stderr, "Config: at least one [[subscriber]] is required");
        std::exit(1);
    }

    // [churn] (optional)
    if (const auto* churn_tbl = tbl["churn"].as_table()) {
        auto enabled = (*churn_tbl)["enabled"].value<bool>();
        if (enabled && *enabled) {
            cfg.churn.enabled = true;
            auto conns = (*churn_tbl)["connections"].value<int>();
            if (!conns || *conns <= 0) {
                std::println(stderr, "Config: churn.connections must be > 0 when enabled");
                std::exit(1);
            }
            cfg.churn.connections = *conns;

            auto interval = (*churn_tbl)["interval_ms"].value<int>();
            if (interval) {
                cfg.churn.interval_ms = *interval;
            }

            cfg.churn.subscribe_topics = parse_string_array(
                (*churn_tbl)["subscribe_topics"].as_array(), "churn.subscribe_topics");
            validate_topics(cfg.churn.subscribe_topics, "[churn]");
        }
    }

    // Connection budget validation
    const size_t total_conns = cfg.publishers.size() + cfg.subscribers.size() +
                               (cfg.churn.enabled ? static_cast<size_t>(cfg.churn.connections) : 0);
    // Just warn — the broker config may have been bumped
    if (total_conns > 64) {
        std::println(stderr, "Warning: {} total connections requested (check broker max_connections)", total_conns);
    }

    return cfg;
}

//  Main

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::println(stderr, "Usage: {} <host> <port> <config.toml>", argv[0]);
        return 1;
    }

    const std::string host = argv[1];
    const uint16_t    port = static_cast<uint16_t>(std::stoi(argv[2]));
    const StressConfig cfg = load_config(argv[3], host, port);

    const size_t total_conns = cfg.publishers.size() + cfg.subscribers.size() +
                               (cfg.churn.enabled ? static_cast<size_t>(cfg.churn.connections) : 0);

    std::println("stress_test  {}:{}  topics={}  publishers={}  subscribers={}  churn={}",
        cfg.host, cfg.port, cfg.topics.size(), cfg.publishers.size(),
        cfg.subscribers.size(), cfg.churn.enabled ? cfg.churn.connections : 0);
    std::println("warmup={}s  measure={}s  total_connections={}\n",
        cfg.warmup_secs, cfg.measure_secs, total_conns);

    // Print publisher configs
    for (size_t i = 0; i < cfg.publishers.size(); i++) {
        const auto& p = cfg.publishers[i];
        std::string topics_str;
        for (size_t j = 0; j < p.topics.size(); j++) {
            if (j > 0) topics_str += ", ";
            topics_str += p.topics[j];
        }
        std::println("  pub[{}]  topics=[{}]  payload={}B  rate={}{}",
            i, topics_str, p.payload_bytes,
            p.rate_hz == 0 ? "" : std::to_string(p.rate_hz),
            p.rate_hz == 0 ? "unlimited" : " msg/s");
    }
    std::println();

    //  Launch subscribers
    g_running = true;

    std::vector<SubState>    sub_states(cfg.subscribers.size());
    std::vector<std::thread> sub_threads;
    sub_threads.reserve(cfg.subscribers.size());
    for (size_t i = 0; i < cfg.subscribers.size(); i++) {
        sub_threads.emplace_back(run_subscriber, std::cref(cfg), std::cref(cfg.subscribers[i]),
                                 std::ref(sub_states[i]));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    //  Launch publishers
    std::vector<PubState>    pub_states(cfg.publishers.size());
    std::vector<std::thread> pub_threads;
    pub_threads.reserve(cfg.publishers.size());
    for (size_t i = 0; i < cfg.publishers.size(); i++) {
        pub_threads.emplace_back(run_publisher, std::cref(cfg), std::cref(cfg.publishers[i]),
                                 static_cast<uint64_t>(i), std::ref(pub_states[i]));
    }

    //  Launch churn threads
    std::vector<ChurnState>  churn_states;
    std::vector<std::thread> churn_threads;
    if (cfg.churn.enabled) {
        churn_states.resize(static_cast<size_t>(cfg.churn.connections));
        churn_threads.reserve(static_cast<size_t>(cfg.churn.connections));
        for (int i = 0; i < cfg.churn.connections; i++) {
            churn_threads.emplace_back(run_churn, std::cref(cfg), std::cref(cfg.churn),
                                       std::ref(churn_states[static_cast<size_t>(i)]));
        }
    }

    //  Warmup
    std::println("Warming up for {}s...", cfg.warmup_secs);
    std::this_thread::sleep_for(std::chrono::seconds(cfg.warmup_secs));

    //  Measure window
    std::println("Measuring for {}s...", cfg.measure_secs);
    g_collecting = true;
    const auto t_start = std::chrono::steady_clock::now();
    std::this_thread::sleep_for(std::chrono::seconds(cfg.measure_secs));
    g_collecting = false;
    const double elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - t_start).count();

    //  Teardown
    g_running = false;

    for (auto& s : pub_states) {
        if (s.fd >= 0) {
            shutdown(s.fd, SHUT_RDWR);
        }
    }
    for (auto& t : pub_threads) {
        t.join();
    }
    for (auto& s : sub_states) {
        if (s.fd >= 0) {
            shutdown(s.fd, SHUT_RDWR);
        }
    }
    for (auto& t : sub_threads) {
        t.join();
    }
    for (auto& s : churn_threads) {
        s.join();
    }

    //  Aggregate and report
    std::println("\n=== Stress Test Results ===");
    std::println("Duration: {:.2f}s  Publishers: {}  Subscribers: {}  Topics: {}",
        elapsed, cfg.publishers.size(), cfg.subscribers.size(), cfg.topics.size());

    // Publisher RTT
    std::vector<int64_t> all_rtt;
    uint64_t total_sent = 0;
    for (auto& s : pub_states) {
        all_rtt.insert(all_rtt.end(), s.rtt_samples.begin(), s.rtt_samples.end());
        total_sent += s.sent;
    }
    auto rtt_stats = compute_stats(all_rtt);
    const int64_t throughput = rtt_stats.count > 0
        ? static_cast<int64_t>(static_cast<double>(rtt_stats.count) / elapsed) : 0;

    std::println("\n--- Publisher RTT (send -> ACK) ---");
    std::println("  min={:>7}us  p50={:>7}us  p99={:>7}us  p999={:>7}us  max={:>7}us",
        rtt_stats.min_us, rtt_stats.p50_us, rtt_stats.p99_us, rtt_stats.p999_us, rtt_stats.max_us);
    std::println("  Total sent: {}  Throughput: {} msg/s  (measured window: {} samples)",
        total_sent, throughput, rtt_stats.count);

    // Subscriber e2e latency
    std::vector<int64_t> all_e2e;
    uint64_t total_received = 0;
    for (auto& s : sub_states) {
        all_e2e.insert(all_e2e.end(), s.e2e_samples.begin(), s.e2e_samples.end());
        total_received += s.total_received;
    }
    auto e2e_stats = compute_stats(all_e2e);

    std::println("\n--- Subscriber End-to-End Latency (same-host steady_clock) ---");
    std::println("  min={:>7}us  p50={:>7}us  p99={:>7}us  p999={:>7}us  max={:>7}us",
        e2e_stats.min_us, e2e_stats.p50_us, e2e_stats.p99_us, e2e_stats.p999_us, e2e_stats.max_us);
    std::println("  Total received: {}  (measured window: {} samples)", total_received, e2e_stats.count);

    // Per-topic breakdown
    std::unordered_map<std::string, SubState::TopicStats> topic_totals;
    for (const auto& s : sub_states) {
        for (const auto& [topic, stats] : s.per_topic) {
            topic_totals[topic].received += stats.received;
            topic_totals[topic].seq_gaps += stats.seq_gaps;
        }
    }

    std::println("\n--- Per-Topic Breakdown ---");
    std::println("  {:<24} {:>10}    {:>8}", "Topic", "Received", "Seq Gaps");
    for (const auto& topic : cfg.topics) {
        auto it = topic_totals.find(topic);
        if (it != topic_totals.end()) {
            std::println("  {:<24} {:>10}    {:>8}", topic, it->second.received, it->second.seq_gaps);
        } else {
            std::println("  {:<24} {:>10}    {:>8}", topic, 0, 0);
        }
    }

    // Churn stats
    if (cfg.churn.enabled) {
        uint64_t total_reconnects = 0, total_failures = 0;
        for (const auto& cs : churn_states) {
            total_reconnects += cs.reconnects;
            total_failures   += cs.failures;
        }
        std::println("\n--- Connection Churn ---");
        std::println("  Reconnects: {}  Failures: {}", total_reconnects, total_failures);
    }

    std::println();
    return 0;
}
