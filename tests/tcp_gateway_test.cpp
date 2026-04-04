#include <gtest/gtest.h>
#include <broker/tcp_gateway.hpp>

#include <cstring>
#include <vector>

// Helpers

// Encode a SUBSCRIBE frame into a vector.
static std::vector<std::byte> make_subscribe_frame(std::string_view topic, uint64_t seq = 1) {
    std::vector<std::byte> buf(sizeof(FrameHeader) + 2 + topic.size());
    auto r = encode_subscribe(std::span{buf}, seq, topic);
    EXPECT_TRUE(r.has_value()) << "encode_subscribe failed";
    buf.resize(*r);
    return buf;
}

// Frame reassembly state machine
//
// Feeds synthetic bytes directly into a Connection buffer and calls
// dispatch_frames_impl without any real sockets.

class DispatchFramesTest : public ::testing::Test {
protected:
    moodycamel::BlockingConcurrentQueue<InboundMessage> inbound_;
    Connection conn_;

    void SetUp() override {
        conn_.buf_state = std::make_shared<BufferState>();
        conn_.parse_pos = 0;
        conn_.write_pos = 0;
        conn_.stage     = ParseStage::AwaitingHeader;
        conn_.active    = true;
    }

    // Copy bytes into conn_.buf_state->buf at write_pos and advance write_pos.
    void feed(const void* data, size_t len) {
        std::memcpy(conn_.buf_state->buf.get() + conn_.write_pos, data, len);
        conn_.write_pos += static_cast<uint32_t>(len);
    }
    void feed(const std::vector<std::byte>& v) { feed(v.data(), v.size()); }

    // Call dispatch_frames_impl (no-error callback since all tests use valid frames).
    std::optional<ParseError> dispatch() {
        return dispatch_frames_impl(/*fd=*/42, conn_, inbound_);
    }

    // Drain all enqueued InboundMessages (non-blocking).
    // Must be called at the end of each test so watermark_ is not referenced
    // after the fixture destructs (TSan safety).
    std::vector<InboundMessage> drain() {
        std::vector<InboundMessage> out;
        InboundMessage msg;
        while (inbound_.try_dequeue(msg))
            out.push_back(std::move(msg));
        return out;
    }
};

// First 12 bytes of a 30-byte frame arrive; no dispatch until the remainder arrives.
TEST_F(DispatchFramesTest, HeaderSplitAcrossReads) {
    const auto frame = make_subscribe_frame("test", /*seq=*/1);
    ASSERT_GT(frame.size(), sizeof(FrameHeader));  // lowk sanity check

    // Feed only the first half of the 24-byte header.
    feed(frame.data(), 12);
    EXPECT_FALSE(dispatch());
    EXPECT_EQ(drain().size(), 0u);
    EXPECT_EQ(conn_.stage, ParseStage::AwaitingHeader);
    EXPECT_EQ(conn_.parse_pos, 0u);

    // Feed the rest.
    feed(frame.data() + 12, frame.size() - 12);
    EXPECT_FALSE(dispatch());

    auto msgs = drain();
    ASSERT_EQ(msgs.size(), 1u);
    EXPECT_EQ(msgs[0].consumed_up_to, static_cast<uint32_t>(frame.size()));
    EXPECT_EQ(conn_.stage, ParseStage::AwaitingHeader);
}

// Header complete, but only half the payload arrives; no dispatch until the rest.
TEST_F(DispatchFramesTest, PayloadSplitAcrossReads) {
    // Use a longer topic so the payload is meaningfully sized.
    const auto frame = make_subscribe_frame("sensors/temperature", /*seq=*/2);
    ASSERT_GT(frame.size(), sizeof(FrameHeader) + 4u);  // at least 4 payload bytes

    const size_t half_payload = (frame.size() - sizeof(FrameHeader)) / 2;
    const size_t first_chunk  = sizeof(FrameHeader) + half_payload;

    // Feed header + half payload.
    feed(frame.data(), first_chunk);
    EXPECT_FALSE(dispatch());
    EXPECT_EQ(drain().size(), 0u);
    EXPECT_EQ(conn_.stage, ParseStage::AwaitingPayload);  // header parsed, waiting for payload
    EXPECT_EQ(conn_.parse_pos, 0u);                       // not advanced yet

    // Feed the remaining payload bytes.
    feed(frame.data() + first_chunk, frame.size() - first_chunk);
    EXPECT_FALSE(dispatch());

    auto msgs = drain();
    ASSERT_EQ(msgs.size(), 1u);
    auto& sub = std::get<SubscribeMsg>(msgs[0].frame.payload);
    EXPECT_EQ(sub.topic, "sensors/temperature");
}

// Two complete frames concatenated in the buffer — both dispatched in one call.
TEST_F(DispatchFramesTest, MultipleFramesInOneBuffer) {
    const auto f1 = make_subscribe_frame("a/1", /*seq=*/1);
    const auto f2 = make_subscribe_frame("b/2", /*seq=*/2);

    feed(f1);
    feed(f2);
    EXPECT_FALSE(dispatch());

    auto msgs = drain();
    ASSERT_EQ(msgs.size(), 2u);

    auto& sub1 = std::get<SubscribeMsg>(msgs[0].frame.payload);
    EXPECT_EQ(sub1.topic, "a/1");
    EXPECT_EQ(msgs[0].consumed_up_to, static_cast<uint32_t>(f1.size()));

    auto& sub2 = std::get<SubscribeMsg>(msgs[1].frame.payload);
    EXPECT_EQ(sub2.topic, "b/2");
    EXPECT_EQ(msgs[1].consumed_up_to, static_cast<uint32_t>(f1.size() + f2.size()));

    EXPECT_EQ(conn_.stage, ParseStage::AwaitingHeader);
}

// Feed exactly one byte less than a complete frame — must not dispatch.
TEST_F(DispatchFramesTest, ExactlyOneByteShort) {
    // Use a topic long enough that the total frame is well above 24 bytes
    // so that "one byte short" unambiguously lands in AwaitingPayload.
    const auto frame = make_subscribe_frame("topic/with/path", /*seq=*/3);
    ASSERT_GT(frame.size(), sizeof(FrameHeader) + 2u);

    feed(frame.data(), frame.size() - 1);
    EXPECT_FALSE(dispatch());
    EXPECT_EQ(drain().size(), 0u);
    // Header was parsed; state advanced to AwaitingPayload but no frame dispatched.
    EXPECT_EQ(conn_.stage, ParseStage::AwaitingPayload);
    EXPECT_EQ(conn_.parse_pos, 0u);
}

// Watermark-gated buffer compaction

class CompactionTest : public ::testing::Test {
protected:
    Connection conn_;

    void SetUp() override {
        conn_.buf_state = std::make_shared<BufferState>();
        conn_.parse_pos = 0;
        conn_.write_pos = 0;
        conn_.stage     = ParseStage::AwaitingHeader;
    }
};

// When watermark == 0 no compaction fires — pointers and buffer are untouched.
TEST_F(CompactionTest, NoCompactionWhenWatermarkZero) {
    conn_.write_pos = 100;
    conn_.parse_pos = 60;
    // Place a sentinel byte at parse_pos.
    conn_.buf_state->buf.get()[60] = std::byte{0x42};

    compact_if_needed(conn_);

    EXPECT_EQ(conn_.write_pos, 100u);
    EXPECT_EQ(conn_.parse_pos, 60u);
    EXPECT_EQ(conn_.buf_state->watermark.load(), 0u);
    EXPECT_EQ(conn_.buf_state->buf.get()[60], std::byte{0x42});  // untouched
}

// When watermark > 0 compaction fires: remaining bytes shift to front and
// both pointers decrease by the watermark value.
TEST_F(CompactionTest, CompactionFiresAndResetsPointers) {
    // Bytes [40..69] are "remaining" (not yet consumed by the parser).
    conn_.write_pos = 70;
    conn_.parse_pos = 40;
    conn_.buf_state->watermark.store(40);

    // Write a known pattern into [40..69].
    for (uint32_t i = 0; i < 30; ++i) {
        conn_.buf_state->buf.get()[40 + i] = std::byte{static_cast<uint8_t>(i)};
    }

    compact_if_needed(conn_);

    EXPECT_EQ(conn_.write_pos, 30u);                          // 70 - 40
    EXPECT_EQ(conn_.parse_pos, 0u);                           // 40 - 40
    EXPECT_EQ(conn_.buf_state->watermark.load(), 0u);         // reset

    // Bytes [0..29] must now match the original [40..69] pattern.
    for (uint32_t i = 0; i < 30; ++i) {
        EXPECT_EQ(conn_.buf_state->buf.get()[i], std::byte{static_cast<uint8_t>(i)}) << "mismatch at i=" << i;
    }
}

// A valid frame whose bytes were shifted by compaction is still decoded correctly.
TEST_F(CompactionTest, FrameStradlingBoundaryDecodedCorrectly) {
    moodycamel::BlockingConcurrentQueue<InboundMessage> inbound;

    // Build a SUBSCRIBE frame for "check/it".
    const auto frame = make_subscribe_frame("check/it", /*seq=*/7);
    const auto fsize = static_cast<uint32_t>(frame.size());

    // Simulate: 50 bytes of a previously-consumed frame sit at [0..49].
    // The new frame starts at byte 50.
    for (uint32_t i = 0; i < 50; ++i) {
        conn_.buf_state->buf.get()[i] = std::byte{0xFF};  // garbage from old frame
    }
    std::memcpy(conn_.buf_state->buf.get() + 50, frame.data(), fsize);

    conn_.write_pos = 50 + fsize;
    conn_.parse_pos = 50;
    conn_.buf_state->watermark.store(50);

    // Compact: shifts the frame bytes to the front.
    compact_if_needed(conn_);
    EXPECT_EQ(conn_.write_pos, fsize);
    EXPECT_EQ(conn_.parse_pos, 0u);
    EXPECT_EQ(conn_.buf_state->watermark.load(), 0u);

    // Now dispatch — the frame should be decoded from its new position.
    const auto err = dispatch_frames_impl(/*fd=*/99, conn_, inbound);
    EXPECT_FALSE(err);

    InboundMessage msg;
    ASSERT_TRUE(inbound.try_dequeue(msg));
    auto& sub = std::get<SubscribeMsg>(msg.frame.payload);
    EXPECT_EQ(sub.topic, "check/it");
    EXPECT_EQ(msg.consumed_up_to, fsize);

    // Drain to avoid dangling watermark_ reference.
    InboundMessage leftover;
    while (inbound.try_dequeue(leftover)) {}
}
