#include <broker/tcp_gateway.hpp>
#include <cassert>
#include <chrono>
#include <csignal>
#include <print>
#include <ranges>
#include <thread>

#include "broker/router.hpp"

// ---------------------------------------------------------------------------
// Helpers to build inbound messages without going through the TCP layer
// ---------------------------------------------------------------------------

static InboundMessage make_subscribe(const int fd, const std::string_view topic) {
    return InboundMessage {
        .frame     = DecodedFrame { .header = {}, .payload = SubscribeMsg { topic } },
        .sender_fd = fd
    };
}

static InboundMessage make_unsubscribe(const int fd, const std::string_view topic) {
    return InboundMessage {
        .frame     = DecodedFrame { .header = {}, .payload = UnsubscribeMsg { topic } },
        .sender_fd = fd
    };
}

// ---------------------------------------------------------------------------
// Print helpers — dump both maps so we can eyeball correctness
// ---------------------------------------------------------------------------

static void print_topic_map(const Router& r) {
    std::println("  topic_subscribers_:");
    for (const auto& [topic, fds] : r.topic_subscribers_) {
        std::print("    {:10} -> [", topic);
        for (size_t i = 0; i < fds.size(); ++i)
            std::print("{}{}", fds[i], i + 1 < fds.size() ? ", " : "");
        std::println("]");
    }
}

static void print_slot_map(const Router& r) {
    std::println("  fd_topic_slot_:");
    for (const auto& [fd, topics] : r.fd_topic_slot_) {
        std::print("    fd {:2} -> {{", fd);
        bool first = true;
        for (const auto& [topic, slot] : topics) {
            std::print("{}{}@{}", first ? "" : ", ", topic, slot);
            first = false;
        }
        std::println("}}");
    }
}

static void print_state(const Router& r) {
    print_topic_map(r);
    print_slot_map(r);
    std::println("");
}

// For each subscriber fd, check that its recorded slot matches where it
// actually sits in the topic vector. Asserts on any mismatch.
static void assert_indices_consistent(const Router& r) {
    for (const auto& [fd, topics] : r.fd_topic_slot_) {
        for (const auto& [topic, slot] : topics) {
            const auto it = r.topic_subscribers_.find(topic);
            assert(it != r.topic_subscribers_.end() && "topic missing from topic_subscribers_");
            const auto& vec = it->second;
            assert(slot < vec.size()      && "slot out of range");
            assert(vec[slot] == fd        && "slot points to wrong fd");
        }
    }
}

// ---------------------------------------------------------------------------

int main() {
    moodycamel::ConcurrentQueue<InboundMessage> inbound_queue;
    Router router(RouterConfig { inbound_queue, -1 });

    // -----------------------------------------------------------------------
    // 1. Enqueue subscribe messages for ~10 fictional fds across several topics
    //
    //  "sports"  <- fd 3, 4, 5, 6
    //  "finance" <- fd 4, 7, 8
    //  "weather" <- fd 5, 9, 10
    //  "tech"    <- fd 6, 7, 11
    //  "music"   <- fd 3, 9, 12
    // -----------------------------------------------------------------------

    const std::pair<int, std::string_view> subscriptions[] = {
        {3,  "sports"}, {4,  "sports"}, {5,  "sports"}, {6,  "sports"},
        {4,  "finance"},{7,  "finance"},{8,  "finance"},
        {5,  "weather"},{9,  "weather"},{10, "weather"},
        {6,  "tech"},   {7,  "tech"},   {11, "tech"},
        {3,  "music"},  {9,  "music"},  {12, "music"},
        // Duplicate — router must silently ignore this
        {4,  "sports"},
    };

    for (const auto& [fd, topic] : subscriptions)
        inbound_queue.enqueue(make_subscribe(fd, topic));

    router.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(20)); // let worker drain queue
    router.stop();

    // -----------------------------------------------------------------------
    // 2. Verify state after subscribes
    // -----------------------------------------------------------------------

    std::println("=== After subscribes ===");
    print_state(router);
    assert_indices_consistent(router);

    // Spot-check: "sports" should have exactly 4 subscribers
    assert(router.topic_subscribers_.at("sports").size()  == 4);
    assert(router.topic_subscribers_.at("finance").size() == 3);
    assert(router.topic_subscribers_.at("weather").size() == 3);
    assert(router.topic_subscribers_.at("tech").size()    == 3);
    assert(router.topic_subscribers_.at("music").size()   == 3);

    std::println("All post-subscribe assertions passed.\n");

    // -----------------------------------------------------------------------
    // 3. Unsubscribe some fds, then verify the swap-and-pop bookkeeping
    //
    //  Remove fd 4  from "sports"  -> sports loses a mid-vector entry
    //  Remove fd 7  from "tech"    -> tech loses a mid-vector entry
    //  Remove fd 9  from "music"   -> music loses a mid-vector entry
    // -----------------------------------------------------------------------

    const std::pair<int, std::string_view> unsubscriptions[] = {
        {4,  "sports"},
        {7,  "tech"},
        {9,  "music"},
    };

    router.start();
    for (const auto& [fd, topic] : unsubscriptions)
        inbound_queue.enqueue(make_unsubscribe(fd, topic));

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    router.stop();

    // -----------------------------------------------------------------------
    // 4. Verify state after unsubscribes
    // -----------------------------------------------------------------------

    std::println("=== After unsubscribes ===");
    print_state(router);
    assert_indices_consistent(router);

    assert(router.topic_subscribers_.at("sports").size()  == 3); // lost fd 4
    assert(router.topic_subscribers_.at("tech").size()    == 2); // lost fd 7
    assert(router.topic_subscribers_.at("music").size()   == 2); // lost fd 9

    // fd 4 must still track "finance" but NOT "sports"
    assert( router.fd_topic_slot_.at(4).contains("finance"));
    assert(!router.fd_topic_slot_.at(4).contains("sports"));

    std::println("All post-unsubscribe assertions passed.");
}
