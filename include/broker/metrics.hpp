#ifndef INCLUDE_BROKER_METRICS_HPP_
#define INCLUDE_BROKER_METRICS_HPP_

/*************************** metrics.hpp ***************************
 * Owns the Prometheus registry, exposer (HTTP server thread), and
 * all metric families. Intended to be constructed once in main() and
 * passed by pointer to the Router.
 *
 * All update methods are called only from the router thread,
 * so the per-topic/per-fd caches need no locking.
 * The exposer scrape thread only reads from the registry via
 * prometheus-cpp's own internal locking.
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
    std::shared_ptr<prometheus::Registry>    registry_;
    prometheus::Exposer                      exposer_;

    prometheus::Family<prometheus::Counter>& published_family_;
    prometheus::Family<prometheus::Counter>& dropped_family_;
    prometheus::Family<prometheus::Gauge>&   queue_depth_family_;
    prometheus::Gauge&                       subscriber_count_;


    // We want to avoid Family::Add() since it uses mutex + map lookup per publish 
    // Populated lazily on first seen topic / fd. Never shrunk for topics
    // So topics live forever, removed on disconnect for fds.

    struct StringHash { // So we can look up easily with string_views as well without constructing std::strings
        using is_transparent = void;
        size_t operator()(std::string_view sv) const noexcept {
            return std::hash<std::string_view>{}(sv);
        }
    };
    std::unordered_map<std::string, prometheus::Counter*, StringHash, std::equal_to<>> published_cache_;
    std::unordered_map<std::string, prometheus::Counter*, StringHash, std::equal_to<>> dropped_cache_;
    std::unordered_map<int, prometheus::Gauge*>                                        queue_depth_cache_;

public:
    explicit Metrics(std::string_view bind_address = "0.0.0.0:9090")
        : registry_   { std::make_shared<prometheus::Registry>() },
          exposer_    { std::string(bind_address) },
          published_family_ { prometheus::BuildCounter()
            .Name("messages_published_total")
            .Help("Total messages published per topic")
            .Register(*registry_) },
          dropped_family_ { prometheus::BuildCounter()
            .Name("messages_dropped_total")
            .Help("Total messages dropped due to full subscriber queue, per topic")
            .Register(*registry_) },
          queue_depth_family_ { prometheus::BuildGauge()
            .Name("subscriber_queue_depth")
            .Help("Current SPSC ring buffer fill level per subscriber fd")
            .Register(*registry_) },
          subscriber_count_ { prometheus::BuildGauge()
            .Name("subscriber_count")
            .Help("Number of currently active subscribers")
            .Register(*registry_)
            .Add({}) }
    {
        exposer_.RegisterCollectable(registry_);
    }

    // Called by Router on each successful publish dispatch
    void on_published(std::string_view topic) {
        counter_for(published_cache_, published_family_, topic).Increment();
    }

    // Called by Router when an SPSC push fails (drop-on-full backpressure)
    void on_dropped(std::string_view topic) {
        counter_for(dropped_cache_, dropped_family_, topic).Increment();
    }

    // Called by Router after each push so the gauge reflects current fill level
    void set_queue_depth(int fd, double depth) {
        auto it = queue_depth_cache_.find(fd);
        if (it == queue_depth_cache_.end()) {
            auto& g = queue_depth_family_.Add({{"fd", std::to_string(fd)}});
            queue_depth_cache_.emplace(fd, &g);
            g.Set(depth);
        } else {
            it->second->Set(depth);
        }
    }

    // Called by Router on disconnect. Removes the gauge label set completetely
    void remove_fd(int fd) {
        auto it = queue_depth_cache_.find(fd);
        if (it == queue_depth_cache_.end()) return;
        queue_depth_family_.Remove(it->second);
        queue_depth_cache_.erase(it);
    }

    void on_subscriber_added()   { subscriber_count_.Increment(); }
    void on_subscriber_removed() { subscriber_count_.Decrement(); }

private:
    // If counter exists in cache return it, otherwise create it and cache it before returning reference
    static prometheus::Counter& counter_for(
        std::unordered_map<std::string, prometheus::Counter*, StringHash, std::equal_to<>>& cache,
        prometheus::Family<prometheus::Counter>& family,
        std::string_view topic) 
    {   
        if (auto it = cache.find(topic); it != cache.end()) {
            return *it->second;
        }
        auto& c = family.Add({{"topic", std::string(topic)}});
        cache.emplace(std::string(topic), &c);
        return c;
    }
};

#endif // INCLUDE_BROKER_METRICS_HPP_
