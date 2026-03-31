#ifndef INCLUDE_BROKER_METRICS_HPP_
#define INCLUDE_BROKER_METRICS_HPP_

/*************************** metrics.hpp ***************************
 * Owns the Prometheus registry, exposer (HTTP server thread), and
 * all metric families. Constructed once in main() and passed by
 * reference to Router and TcpGateway.
 *
 * Router-thread methods (on_subscribe, on_publish_request, etc.)
 * are single-threaded — caches need no locking.
 * Gateway-thread methods (on_connection_*, on_bytes_*, on_parse_error)
 * touch only label-less counters/gauges whose Increment/Decrement
 * are internally atomic in prometheus-cpp.
 */

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/registry.h>
#include <prometheus/exposer.h>

class Metrics {
    std::shared_ptr<prometheus::Registry> registry_;
    prometheus::Exposer                   exposer_;

    // ── Topic-level counters (router thread) ──────────────────────
    prometheus::Family<prometheus::Counter>& publish_requests_family_;
    prometheus::Family<prometheus::Counter>& delivered_family_;
    prometheus::Family<prometheus::Counter>& dropped_family_;
    prometheus::Family<prometheus::Counter>& subscriptions_family_;
    prometheus::Family<prometheus::Counter>& unsubscriptions_family_;

    // ── Topic-level gauges (router thread) ────────────────────────
    prometheus::Family<prometheus::Gauge>& subscribers_family_;
    prometheus::Gauge&                     active_topics_;

    // ── Per-fd gauge (router thread) ──────────────────────────────
    prometheus::Family<prometheus::Gauge>& queue_depth_family_;

    // ── Connection / network (gateway threads, label-less) ────────
    prometheus::Counter& connections_accepted_;
    prometheus::Counter& connections_rejected_;
    prometheus::Gauge&   connections_active_;
    prometheus::Counter& bytes_received_;
    prometheus::Counter& bytes_sent_;
    prometheus::Family<prometheus::Counter>& parse_errors_family_;

    // ── Caches ────────────────────────────────────────────────────
    // Avoid Family::Add() on the hot path (mutex + map lookup).
    // Populated lazily on first seen key. Topic caches never shrink;
    // fd cache entries are removed on disconnect.

    struct StringHash {
        using is_transparent = void;
        size_t operator()(std::string_view sv) const noexcept {
            return std::hash<std::string_view>{}(sv);
        }
    };

    using CounterCache = std::unordered_map<std::string, prometheus::Counter*, StringHash, std::equal_to<>>;
    using GaugeCache   = std::unordered_map<std::string, prometheus::Gauge*,   StringHash, std::equal_to<>>;

    CounterCache publish_requests_cache_;
    CounterCache delivered_cache_;
    CounterCache dropped_cache_;
    CounterCache subscriptions_cache_;
    CounterCache unsubscriptions_cache_;
    CounterCache parse_errors_cache_;
    GaugeCache   subscribers_cache_;
    std::unordered_map<int, prometheus::Gauge*> queue_depth_cache_;

public:
    explicit Metrics(std::string_view bind_address = "0.0.0.0:9090")
        : registry_   { std::make_shared<prometheus::Registry>() },
          exposer_    { std::string(bind_address) },

          publish_requests_family_ { prometheus::BuildCounter()
            .Name("publish_requests_total")
            .Help("Total publish requests received per topic")
            .Register(*registry_) },
          delivered_family_ { prometheus::BuildCounter()
            .Name("messages_delivered_total")
            .Help("Total messages successfully enqueued to subscriber rings, per topic")
            .Register(*registry_) },
          dropped_family_ { prometheus::BuildCounter()
            .Name("messages_dropped_total")
            .Help("Total messages dropped due to full subscriber queue, per topic")
            .Register(*registry_) },
          subscriptions_family_ { prometheus::BuildCounter()
            .Name("subscriptions_total")
            .Help("Total subscribe events per topic")
            .Register(*registry_) },
          unsubscriptions_family_ { prometheus::BuildCounter()
            .Name("unsubscriptions_total")
            .Help("Total unsubscribe events per topic (includes disconnects)")
            .Register(*registry_) },

          subscribers_family_ { prometheus::BuildGauge()
            .Name("subscribers")
            .Help("Current subscriber count per topic")
            .Register(*registry_) },
          active_topics_ { prometheus::BuildGauge()
            .Name("active_topics")
            .Help("Number of topics with at least one subscriber")
            .Register(*registry_)
            .Add({}) },

          queue_depth_family_ { prometheus::BuildGauge()
            .Name("subscriber_queue_depth")
            .Help("Current SPSC ring buffer fill level per subscriber fd")
            .Register(*registry_) },

          connections_accepted_ { prometheus::BuildCounter()
            .Name("connections_accepted_total")
            .Help("Total TCP connections accepted")
            .Register(*registry_).Add({}) },
          connections_rejected_ { prometheus::BuildCounter()
            .Name("connections_rejected_total")
            .Help("Total TCP connections rejected (max_connections limit)")
            .Register(*registry_).Add({}) },
          connections_active_ { prometheus::BuildGauge()
            .Name("connections_active")
            .Help("Currently open TCP connections")
            .Register(*registry_).Add({}) },
          bytes_received_ { prometheus::BuildCounter()
            .Name("bytes_received_total")
            .Help("Total bytes received from clients")
            .Register(*registry_).Add({}) },
          bytes_sent_ { prometheus::BuildCounter()
            .Name("bytes_sent_total")
            .Help("Total bytes sent to clients")
            .Register(*registry_).Add({}) },
          parse_errors_family_ { prometheus::BuildCounter()
            .Name("parse_errors_total")
            .Help("Total frame parse errors by reason")
            .Register(*registry_) }
    {
        exposer_.RegisterCollectable(registry_);
    }

    // ── Router thread: publish path ───────────────────────────────

    void on_publish_request(std::string_view topic) {
        counter_for(publish_requests_cache_, publish_requests_family_, topic).Increment();
    }

    void on_delivered(std::string_view topic) {
        counter_for(delivered_cache_, delivered_family_, topic).Increment();
    }

    void on_dropped(std::string_view topic) {
        counter_for(dropped_cache_, dropped_family_, topic).Increment();
    }

    // ── Router thread: subscription lifecycle ─────────────────────

    void on_subscribe(std::string_view topic) {
        counter_for(subscriptions_cache_, subscriptions_family_, topic).Increment();
        gauge_for(subscribers_cache_, subscribers_family_, topic).Increment();
    }

    void on_unsubscribe(std::string_view topic) {
        counter_for(unsubscriptions_cache_, unsubscriptions_family_, topic).Increment();
        gauge_for(subscribers_cache_, subscribers_family_, topic).Decrement();
    }

    void on_topic_added()   { active_topics_.Increment(); }
    void on_topic_removed() { active_topics_.Decrement(); }

    // ── Router thread: per-fd queue depth ─────────────────────────

    void set_queue_depth(int fd, double depth) {
        auto it = queue_depth_cache_.find(fd);
        if (it == queue_depth_cache_.end()) {
            auto& g = queue_depth_family_.Add({{"fd", std::to_string(fd)}});
            it = queue_depth_cache_.emplace(fd, &g).first;
        }
        it->second->Set(depth);
    }

    void remove_fd(int fd) {
        auto it = queue_depth_cache_.find(fd);
        if (it == queue_depth_cache_.end()) return;
        queue_depth_family_.Remove(it->second);
        queue_depth_cache_.erase(it);
    }

    // ── Gateway threads: connections / network ────────────────────

    void on_connection_accepted() { connections_accepted_.Increment(); connections_active_.Increment(); }
    void on_connection_rejected() { connections_rejected_.Increment(); }
    void on_connection_closed()   { connections_active_.Decrement(); }

    void on_bytes_received(size_t n) { bytes_received_.Increment(static_cast<double>(n)); }
    void on_bytes_sent(size_t n)     { bytes_sent_.Increment(static_cast<double>(n)); }

    void on_parse_error(std::string_view reason) {
        counter_for(parse_errors_cache_, parse_errors_family_, reason, "reason").Increment();
    }

private:
    static prometheus::Counter& counter_for(
        CounterCache& cache, prometheus::Family<prometheus::Counter>& family,
        std::string_view key, const char* label = "topic")
    {
        if (auto it = cache.find(key); it != cache.end()) return *it->second;
        auto& c = family.Add({{label, std::string(key)}});
        cache.emplace(std::string(key), &c);
        return c;
    }

    static prometheus::Gauge& gauge_for(
        GaugeCache& cache, prometheus::Family<prometheus::Gauge>& family,
        std::string_view key, const char* label = "topic")
    {
        if (auto it = cache.find(key); it != cache.end()) return *it->second;
        auto& g = family.Add({{label, std::string(key)}});
        cache.emplace(std::string(key), &g);
        return g;
    }
};

#endif // INCLUDE_BROKER_METRICS_HPP_
