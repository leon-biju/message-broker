/*************************** router.cpp ***************************/

#include <pthread.h>
#include <thread>

#include <broker/router.hpp>

static constexpr size_t BATCH_SIZE = 64;

void Router::start() {
    shutdown_ = false;
    worker_ = std::thread([this]() { run_loop(); });
}

void Router::stop() {
    shutdown_ = true;
    worker_.join();
}

void Router::run_loop() {
    if (pinned_cpu_core_ >= 0) {
        cpu_set_t cpus{};
        CPU_ZERO(&cpus);
        CPU_SET(pinned_cpu_core_, &cpus);
        pthread_setaffinity_np(pthread_self(), sizeof(cpus), &cpus);
    }

    InboundMessage msgs[BATCH_SIZE];
    while (!shutdown_.load(std::memory_order_relaxed)) {
        const size_t n = inbound_.try_dequeue_bulk(msgs, BATCH_SIZE);
        if (n == 0) {
            std::this_thread::yield();
            continue;
        }
        for (size_t i = 0; i < n; ++i) {
            std::visit([&](auto& payload) {
                using T = std::decay_t<decltype(payload)>;
                if      constexpr (std::is_same_v<T, SubscribeMsg>)   handle_subscribe  (msgs[i].sender_fd, payload);
                else if constexpr (std::is_same_v<T, UnsubscribeMsg>) handle_unsubscribe(msgs[i].sender_fd, payload);
                else if constexpr (std::is_same_v<T, PublishMsg>)     handle_publish    (msgs[i].sender_fd, payload);
                else if constexpr (std::is_same_v<T, DisconnectMsg>)  handle_disconnect (msgs[i].sender_fd);
            }, msgs[i].frame.payload);
        }
    }
}

void Router::handle_subscribe(const int fd, const SubscribeMsg& msg) {
    auto& subscribed_map = fd_topic_slot_[fd];  // creates entry if absent

    if (subscribed_map.contains(msg.topic)) return;  // already subscribed

    auto& subs = topic_subscribers_[std::string(msg.topic)];  // creates vec if absent
    subscribed_map.emplace(msg.topic, subs.size());
    subs.push_back(fd);
}

void Router::handle_unsubscribe(const int fd, const UnsubscribeMsg& msg) {
    const auto fd_it = fd_topic_slot_.find(fd);
    if (fd_it == fd_topic_slot_.end()) return;

    auto& subscribed_map = fd_it->second;
    const auto slot_it = subscribed_map.find(msg.topic);
    if (slot_it == subscribed_map.end()) return;  // not subscribed to this topic

    const size_t slot = slot_it->second;
    subscribed_map.erase(slot_it);

    const auto topic_it = topic_subscribers_.find(msg.topic);
    if (topic_it == topic_subscribers_.end()) return;

    auto& subs = topic_it->second;
    std::swap(subs[slot], subs.back());
    subs.pop_back();

    // If the swap moved a different fd into `slot`, update its reverse-index entry
    if (slot < subs.size()) {
        fd_topic_slot_[subs[slot]][std::string(msg.topic)] = slot;
    }

    if (subs.empty()) {
        topic_subscribers_.erase(topic_it);
    }
}

void Router::handle_publish(const int /*sender_fd*/, const PublishMsg& msg) {
    const auto it = topic_subscribers_.find(msg.topic);
    if (it == topic_subscribers_.end()) return;

    // Encode frame once, then enqueue a copy per subscriber with the correct dst_fd
    OutboundMessage out{};
    const auto result = encode_publish(out.data, /*seq=*/0, msg.topic, msg.body);
    if (!result) {
        return;
    }
    out.len = *result;

    for (const int fd : it->second) {
        out.dest_fd = fd;
        outbound_.enqueue(out);
    }
}

void Router::handle_disconnect(const int fd) {
    const auto fd_it = fd_topic_slot_.find(fd);
    if (fd_it == fd_topic_slot_.end()) return;

    for (auto& [topic, slot] : fd_it->second) {
        const auto topic_it = topic_subscribers_.find(topic);
        if (topic_it == topic_subscribers_.end()) {
            continue;
        }

        auto& subs = topic_it->second;
        std::swap(subs[slot], subs.back());
        subs.pop_back();

        if (slot < subs.size()) {
            fd_topic_slot_[subs[slot]][topic] = slot;
        }
        if (subs.empty()) {
            topic_subscribers_.erase(topic_it);
        }
    }

    fd_topic_slot_.erase(fd_it);
}
