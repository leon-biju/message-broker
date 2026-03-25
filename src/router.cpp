/*************************** router.cpp ***************************/

#include <pthread.h>
#include <thread>

#include <spdlog/spdlog.h>

#include <broker/router.hpp>

static constexpr size_t BATCH_SIZE = 64;

void Router::start() {
    shutdown_ = false;
    worker_ = std::thread([this]() { run_loop(); });
}

void Router::stop() {
    shutdown_ = true;
    inbound_.enqueue(InboundMessage{
        .frame = DecodedFrame{ .payload = ShutdownMsg{} },
        .sender_fd = -1,
        .watermark_ptr = nullptr,
        .consumed_up_to = 0
    }); // sentinel: unblocks wait_dequeue_bulk
    worker_.join();
    spdlog::debug("Router worker thread joined!");
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
        const size_t n = inbound_.wait_dequeue_bulk(msgs, BATCH_SIZE);

        for (size_t i = 0; i < n; ++i) {
            std::visit([&](auto& payload) {
                using T = std::decay_t<decltype(payload)>;
                if      constexpr (std::is_same_v<T, SubscribeMsg>)   handle_subscribe  (msgs[i].sender_fd, payload, msgs[i].frame.header);
                else if constexpr (std::is_same_v<T, UnsubscribeMsg>) handle_unsubscribe(msgs[i].sender_fd, payload, msgs[i].frame.header);
                else if constexpr (std::is_same_v<T, PublishMsg>)     handle_publish    (msgs[i].sender_fd, payload, msgs[i].frame.header);
                else if constexpr (std::is_same_v<T, DisconnectMsg>)  handle_disconnect (msgs[i].sender_fd);
                // ignore the shutdown message since we want to drain the queue anyway
            }, msgs[i].frame.payload);

            // Signal receiver that we're done with this message's buffer region
            if (msgs[i].watermark_ptr)
                msgs[i].watermark_ptr->store(msgs[i].consumed_up_to, std::memory_order_release);
        }
    }
}

void Router::handle_subscribe(const int fd, const SubscribeMsg& msg, const FrameHeader& header) {
    auto& subscribed_map = fd_topic_slot_[fd];  // creates entry if absent

    if (!subscribed_map.contains(msg.topic)) {
        auto& subs = topic_subscribers_[std::string(msg.topic)];  // creates vec if absent
        subscribed_map.emplace(msg.topic, subs.size());
        subs.push_back(fd);
        spdlog::debug("fd={} subscribed topic={}", fd, msg.topic);
    }

    if (!has_flag(header.flags, Flags::NO_ACK)) {
        enqueue_ack(fd, header.sequence);
    }
}

void Router::handle_unsubscribe(const int fd, const UnsubscribeMsg& msg, const FrameHeader& header) {
    const auto fd_it = fd_topic_slot_.find(fd);
    if (fd_it != fd_topic_slot_.end()) {
        auto& subscribed_map = fd_it->second;
        const auto slot_it = subscribed_map.find(msg.topic);
        if (slot_it != subscribed_map.end()) {
            const size_t slot = slot_it->second;
            subscribed_map.erase(slot_it);

            const auto topic_it = topic_subscribers_.find(msg.topic);
            if (topic_it != topic_subscribers_.end()) {
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
            spdlog::debug("fd={} unsubscribed topic={}", fd, msg.topic);
        }
    }

    if (!has_flag(header.flags, Flags::NO_ACK))
        enqueue_ack(fd, header.sequence);
}

void Router::handle_publish(const int sender_fd, const PublishMsg& msg, const FrameHeader& header) {
    const auto it = topic_subscribers_.find(msg.topic);
    const size_t subscriber_count = (it != topic_subscribers_.end()) ? it->second.size() : 0;

    if (it != topic_subscribers_.end()) {
        // Encode frame once, then enqueue a copy per subscriber with the correct dst_fd
        const size_t needed_size = sizeof(FrameHeader) + sizeof(uint16_t) + msg.topic.size() + msg.body.size();
        OutboundMessage out{};
        const auto result = encode_publish(out.write_buf(needed_size), /*seq=*/0, msg.topic, msg.body);
        if (result) {
            out.len = *result;
            for (const int fd : it->second) {
                out.dest_fd = fd;
                outbound_.enqueue(out);
            }
        }
    }

    if (!has_flag(header.flags, Flags::NO_ACK)) {
        enqueue_ack(sender_fd, header.sequence);
    }

    spdlog::debug("fd={} publish topic={} payload_bytes={} subscribers={}",
        sender_fd, msg.topic, msg.body.size(), subscriber_count);
}

void Router::handle_disconnect(const int fd) {
    const auto fd_it = fd_topic_slot_.find(fd);
    if (fd_it == fd_topic_slot_.end()) return;

    const size_t topic_count = fd_it->second.size();

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

    spdlog::debug("fd={} disconnected, removed from {} topics", fd, topic_count);
}

void Router::enqueue_ack(const int fd, const uint64_t acked_seq) {
    OutboundMessage ack{};
    const auto result = encode_ack(ack.write_buf(sizeof(FrameHeader) + sizeof(uint64_t)), /*seq=*/0, acked_seq);
    if (result) {
        ack.len     = *result;
        ack.dest_fd = fd;
        outbound_.enqueue(ack);
    }
}
