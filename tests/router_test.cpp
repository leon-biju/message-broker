#include <gtest/gtest.h>
#include <broker/router.hpp>
#include <broker/protocol.hpp>

#include <blockingconcurrentqueue.h>
#include <chrono>
#include <optional>
#include <set>
#include <span>
#include <string_view>
#include <vector>

using InQ = moodycamel::BlockingConcurrentQueue<InboundMessage>;

static constexpr std::chrono::milliseconds MSG_TIMEOUT{20};
static constexpr std::chrono::milliseconds EMPTY_TIMEOUT{10};

// fd numbers used in tests go up to 99, size the table to cover them
static constexpr size_t TEST_FD_TABLE_SIZE = 128;

// Reinterpret a string literal's bytes as a byte span (static lifetime so safe as span)
static std::span<const std::byte> as_bytes(const std::string_view sv) {
    return {reinterpret_cast<const std::byte*>(sv.data()), sv.size()};
}

static FrameHeader make_hdr(MessageType type, uint64_t seq, bool no_ack) {
    FrameHeader hdr{};
    hdr.magic    = MAGIC;
    hdr.version  = PROTO_VERSION;
    hdr.type     = std::to_underlying(type);
    hdr.flags    = no_ack ? std::to_underlying(Flags::NO_ACK) : uint8_t{0};
    hdr.sequence = seq;
    return hdr;
}

static InboundMessage sub_msg(int fd, std::string_view topic, uint64_t seq = 0, bool no_ack = true) {
    return {.frame = {make_hdr(MessageType::SUBSCRIBE, seq, no_ack), SubscribeMsg{topic}},
            .sender_fd = fd, .watermark_ptr = nullptr, .consumed_up_to = 0};
}

static InboundMessage unsub_msg(int fd, std::string_view topic, uint64_t seq = 0, bool no_ack = true) {
    return {.frame = {make_hdr(MessageType::UNSUBSCRIBE, seq, no_ack), UnsubscribeMsg{topic}},
            .sender_fd = fd, .watermark_ptr = nullptr, .consumed_up_to = 0};
}

static InboundMessage pub_msg(int fd, std::string_view topic, std::span<const std::byte> body,
                              uint64_t seq = 0, bool no_ack = true) {
    return {.frame = {make_hdr(MessageType::PUBLISH, seq, no_ack), PublishMsg{topic, body}},
            .sender_fd = fd, .watermark_ptr = nullptr, .consumed_up_to = 0};
}

static InboundMessage disc_msg(int fd) {
    return {.frame = {FrameHeader{}, DisconnectMsg{}},
            .sender_fd = fd, .watermark_ptr = nullptr, .consumed_up_to = 0};
}

// Decode an outbound message back to a DecodedFrame (string_views/spans into out.data())
static std::optional<DecodedFrame> decode_outbound(const OutboundMessage& out) {
    const std::byte* data = out.data();
    auto hdr = parse_header(std::span<const std::byte>{data, out.len});
    if (!hdr) return std::nullopt;
    auto frame = decode_frame(*hdr, {data + sizeof(FrameHeader), hdr->payload_len});
    if (!frame) return std::nullopt;
    return *frame;
}

// Drain up to n messages from the outbound table by consuming dirty notifications
// and draining each fd's ring buffer. Returns (fd, message) pairs; fewer than n if table stays quiet.
static std::vector<std::pair<int, OutboundMessage>> drain_n(OutboundTable& tbl, size_t n) {
    std::vector<std::pair<int, OutboundMessage>> out;
    out.reserve(n);
    while (out.size() < n) {
        int fd;
        if (!tbl.dirty.wait_dequeue_timed(fd, MSG_TIMEOUT)) break;
        while (out.size() < n) {
            auto msg = tbl.queues[fd].try_dequeue();
            if (!msg) break;
            out.emplace_back(fd, std::move(*msg));
        }
    }
    return out;
}

// Returns true if no dirty notification arrives within EMPTY_TIMEOUT (table is quiet)
static bool queue_empty(OutboundTable& tbl) {
    int fd;
    return !tbl.dirty.wait_dequeue_timed(fd, EMPTY_TIMEOUT);
}

// Test Fixture
class RouterTest: public ::testing::Test {
protected:
    InQ           inbound_;
    OutboundTable outbound_{TEST_FD_TABLE_SIZE};
    Router        router_{inbound_, outbound_, /*pinned_cpu_core=*/-1};

    void SetUp()    override { router_.start(); }
    void TearDown() override { router_.stop();  }
};

// Tests

// Publish to a topic with N subscribers: all N must receive the message.
TEST_F(RouterTest, FanOut) {
    inbound_.enqueue(sub_msg(10, "t/fanout"));
    inbound_.enqueue(sub_msg(11, "t/fanout"));
    inbound_.enqueue(sub_msg(12, "t/fanout"));
    inbound_.enqueue(pub_msg(99, "t/fanout", as_bytes("hello")));

    auto msgs = drain_n(outbound_, 3);
    ASSERT_EQ(msgs.size(), 3u);

    std::set<int> dests;
    for (const auto& [fd, msg] : msgs) {
        dests.insert(fd);
        auto frame = decode_outbound(msg);
        ASSERT_TRUE(frame.has_value());
        ASSERT_TRUE(std::holds_alternative<PublishMsg>(frame->payload));
        const auto& pub = std::get<PublishMsg>(frame->payload);
        EXPECT_EQ(pub.topic, "t/fanout");
        std::string_view body{reinterpret_cast<const char*>(pub.body.data()), pub.body.size()};
        EXPECT_EQ(body, "hello");
    }
    EXPECT_EQ(dests, (std::set<int>{10, 11, 12}));
    EXPECT_TRUE(queue_empty(outbound_));
}

// Unsubscribe the first subscriber: the last subscriber is swapped into that slot and
// its reverse-index entry must be updated, or subsequent operations on it will corrupt state.
TEST_F(RouterTest, SwapAndPopUnsubscribe) {
    // slots after subscribe: fd10=0, fd11=1, fd12=2
    inbound_.enqueue(sub_msg(10, "t/swap"));
    inbound_.enqueue(sub_msg(11, "t/swap"));
    inbound_.enqueue(sub_msg(12, "t/swap"));
    // Unsubscribe fd=10 (slot 0): fd=12 must be swapped to slot 0 and its index updated.
    inbound_.enqueue(unsub_msg(10, "t/swap"));
    inbound_.enqueue(pub_msg(99, "t/swap", as_bytes("x")));

    auto msgs = drain_n(outbound_, 2);
    ASSERT_EQ(msgs.size(), 2u);
    std::set<int> dests;
    for (const auto& [fd, msg] : msgs) dests.insert(fd);
    EXPECT_EQ(dests, (std::set<int>{11, 12}));
    EXPECT_TRUE(queue_empty(outbound_));

    // Now unsubscribe fd=12. If its slot was NOT updated to 0 this will either crash
    // (ASan out-of-bounds) or leave fd=12 still receiving messages.
    inbound_.enqueue(unsub_msg(12, "t/swap"));
    inbound_.enqueue(pub_msg(99, "t/swap", as_bytes("x")));

    auto msgs2 = drain_n(outbound_, 1);
    ASSERT_EQ(msgs2.size(), 1u);
    EXPECT_EQ(msgs2[0].first, 11);
    EXPECT_TRUE(queue_empty(outbound_));
}

// Disconnecting a client must remove it from all K subscribed topics.
TEST_F(RouterTest, DisconnectCleansUpAllSubscriptions) {
    inbound_.enqueue(sub_msg(5, "a/1"));
    inbound_.enqueue(sub_msg(5, "a/2"));
    inbound_.enqueue(sub_msg(5, "a/3"));
    inbound_.enqueue(sub_msg(6, "a/1"));  // fd=6 stays connected
    inbound_.enqueue(disc_msg(5));
    inbound_.enqueue(pub_msg(99, "a/1", as_bytes("x")));
    inbound_.enqueue(pub_msg(99, "a/2", as_bytes("x")));
    inbound_.enqueue(pub_msg(99, "a/3", as_bytes("x")));

    // Only fd=6 should receive the publish on a/1; a/2 and a/3 have no subscribers.
    auto msgs = drain_n(outbound_, 1);
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_EQ(msgs[0].first, 6);
    auto frame = decode_outbound(msgs[0].second);
    ASSERT_TRUE(frame.has_value());
    ASSERT_TRUE(std::holds_alternative<PublishMsg>(frame->payload));
    EXPECT_EQ(std::get<PublishMsg>(frame->payload).topic, "a/1");
    EXPECT_TRUE(queue_empty(outbound_));
}

// Subscribing the same fd to the same topic twice must do nothing. ensure only one delivery back
TEST_F(RouterTest, DuplicateSubscribeIsNoOp) {
    inbound_.enqueue(sub_msg(10, "t/dup"));
    inbound_.enqueue(sub_msg(10, "t/dup"));  // duplicate
    inbound_.enqueue(pub_msg(99, "t/dup", as_bytes("y")));

    auto msgs = drain_n(outbound_, 1);
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_EQ(msgs[0].first, 10);
    EXPECT_TRUE(queue_empty(outbound_));
}

// Publishing to a topic with no subscribers must do nothing. no error, no deliveries
TEST_F(RouterTest, PublishUnknownTopicIsNoOp) {
    inbound_.enqueue(pub_msg(99, "no/subscribers", as_bytes("z")));
    EXPECT_TRUE(queue_empty(outbound_));
}

// The ACK sent back to the publisher must carry the sequence number from the inbound frame.
TEST_F(RouterTest, AckSequencing) {
    // Subscribe fd=10 without NO_ACK, seq=42 -> expect ACK(acked_seq=42) destined for fd=10
    inbound_.enqueue(sub_msg(10, "t/ack", /*seq=*/42, /*no_ack=*/false));

    auto sub_acks = drain_n(outbound_, 1);
    ASSERT_EQ(sub_acks.size(), 1u);
    EXPECT_EQ(sub_acks[0].first, 10);
    {
        auto frame = decode_outbound(sub_acks[0].second);
        ASSERT_TRUE(frame.has_value());
        ASSERT_TRUE(std::holds_alternative<AckMsg>(frame->payload));
        EXPECT_EQ(std::get<AckMsg>(frame->payload).acked_seq, 42u);
    }

    // Publish from fd=99 without NO_ACK, seq=77
    // -> PUBLISH delivery to fd=10, ACK(acked_seq=77) to fd=99
    inbound_.enqueue(pub_msg(99, "t/ack", as_bytes("data"), /*seq=*/77, /*no_ack=*/false));

    auto pub_msgs = drain_n(outbound_, 2);
    ASSERT_EQ(pub_msgs.size(), 2u);

    using Delivery = std::pair<int, OutboundMessage>;
    const Delivery* ack_out     = nullptr;
    const Delivery* deliver_out = nullptr;
    for (const auto& m : pub_msgs) {
        auto f = decode_outbound(m.second);
        ASSERT_TRUE(f.has_value());
        if (std::holds_alternative<AckMsg>(f->payload))     ack_out     = &m;
        if (std::holds_alternative<PublishMsg>(f->payload)) deliver_out = &m;
    }

    ASSERT_NE(ack_out,     nullptr);
    ASSERT_NE(deliver_out, nullptr);

    EXPECT_EQ(ack_out->first, 99);
    auto ack_frame = decode_outbound(ack_out->second);
    ASSERT_TRUE(ack_frame.has_value());
    EXPECT_EQ(std::get<AckMsg>(ack_frame->payload).acked_seq, 77u);

    EXPECT_EQ(deliver_out->first, 10);
    EXPECT_TRUE(queue_empty(outbound_));
}
