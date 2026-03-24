#ifndef INCLUDE_BROKER_ROUTER_HPP_
#define INCLUDE_BROKER_ROUTER_HPP_

/*************************** router.hpp ***************************
 * This file contains everything we need to correctly route messages
 *     - Subscribe messages get added to that topics list
 *     - Publish messages push all the subscriber's messages to outbound queue
 *     - Process unsubscribe and disconnect messages as well
 *
 * Responsible for the following:
 *     - Sole consumer of the inbound queue
 *     - Sole producer of the outbound queue
 *     - Routes each inbound message to the appropriate handler
 */
#include <atomic>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <concurrentqueue.h>

#include <broker/tcp_gateway.hpp>

struct StringHash {
    // Single string_view overload covers string, const char*, and string literals
    using is_transparent = void;
    size_t operator()(const std::string_view sv) const { return std::hash<std::string_view>{}(sv); }
};

class Router {
    // Used for sending out messages to subscribers
    std::unordered_map<std::string, std::vector<int>, StringHash, std::equal_to<>> topic_subscribers_;

    // Used to track what topics each subscriber is subscribed to and where on the list for easy removal if unsub
    std::unordered_map<
        int,
        std::unordered_map<std::string, size_t, StringHash, std::equal_to<>>
    > fd_topic_slot_;
    moodycamel::BlockingConcurrentQueue<InboundMessage>&  inbound_;
    moodycamel::BlockingConcurrentQueue<OutboundMessage>& outbound_;

    std::atomic_bool shutdown_ {false};

    std::thread worker_;
    int         pinned_cpu_core_;
public:
    explicit Router(
            moodycamel::BlockingConcurrentQueue<InboundMessage>&  inbound,
            moodycamel::BlockingConcurrentQueue<OutboundMessage>& outbound,
            const int pinned_cpu_core
        ): inbound_(inbound), outbound_(outbound), pinned_cpu_core_(pinned_cpu_core) {};
    void start();
    void stop();

private:
    void run_loop();
    void handle_subscribe  (int fd, const SubscribeMsg&   msg, const FrameHeader& header);
    void handle_unsubscribe(int fd, const UnsubscribeMsg& msg, const FrameHeader& header);
    void handle_publish    (int fd, const PublishMsg& msg, const FrameHeader& header);
    void handle_disconnect (int fd);

    //Helper to send ACK messages
    void enqueue_ack(int fd, uint64_t acked_seq);
};


#endif