/*************************** protocol_test.cpp ***************************
 * Testing the wire protocol, including:
 *     - Every ParseError code path
 *     - Every EncodeError path
 *     - Round trip on each message type
 */

#include <gtest/gtest.h>
#include <broker/protocol.hpp>

#include <array>
#include <cstring>
#include <string>
#include <vector>

// Helpers

static std::span<const std::byte> as_bytes(const auto& buf, size_t len) {
    return {reinterpret_cast<const std::byte*>(buf.data()), len};
}

static std::span<const std::byte> payload_span(const auto& buf, const FrameHeader& hdr) {
    return {reinterpret_cast<const std::byte*>(buf.data()) + sizeof(FrameHeader), hdr.payload_len};
}

// Shared buffer for round-trip tests. Kept at file scope so string_views
// into it remain valid for the duration of each TEST().
static thread_local std::array<std::byte, 70000> g_buf{};

// Encode -> parse_header -> decode_frame, return the DecodedFrame. This is the expected flow
// The buffer g_buf must outlive the returned DecodedFrame (it does in this file).
template <typename Enc>
static DecodedFrame round_trip(Enc encode_fn) {
    g_buf.fill(std::byte{0});
    auto enc = encode_fn(std::span{g_buf});
    EXPECT_TRUE(enc.has_value());
    auto hdr = parse_header(as_bytes(g_buf, *enc));
    EXPECT_TRUE(hdr.has_value());
    auto frame = decode_frame(*hdr, payload_span(g_buf, *hdr));
    EXPECT_TRUE(frame.has_value());
    return *frame;
}

// parse_header error cases
class ParseHeaderErrors: public ::testing::Test {
protected:
    FrameHeader hdr_{};
    std::array<std::byte, sizeof(FrameHeader)> raw_{};

    // Build a valid header in raw_, then mutate before calling parse_header.
    void SetUp() override {
        hdr_.magic       = MAGIC;
        hdr_.version     = PROTO_VERSION;
        hdr_.type        = std::to_underlying(MessageType::ACK);
        hdr_.flags       = std::to_underlying(Flags::NONE);
        std::memset(hdr_.reserved0, 0, sizeof(hdr_.reserved0));
        hdr_.sequence    = 1;
        hdr_.payload_len = 8;
        std::memset(hdr_.reserved1, 0, sizeof(hdr_.reserved1));
        std::memcpy(raw_.data(), &hdr_, sizeof(hdr_));
    }

    void write_header() { std::memcpy(raw_.data(), &hdr_, sizeof(hdr_)); }
};

TEST_F(ParseHeaderErrors, BufferTooSmall) {
    auto r = parse_header(std::span{raw_}.first(sizeof(FrameHeader) - 1));
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), ParseError::BufferTooSmall);
}

TEST_F(ParseHeaderErrors, BadMagic) {
    hdr_.magic = 0xDEAD;
    write_header();
    auto r = parse_header(std::span<const std::byte>{raw_});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), ParseError::BadMagic);
}

TEST_F(ParseHeaderErrors, UnsupportedVersion) {
    hdr_.version = PROTO_VERSION + 1;
    write_header();
    auto r = parse_header(std::span<const std::byte>{raw_});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), ParseError::UnsupportedVersion);
}

TEST_F(ParseHeaderErrors, UnknownMessageType) {
    hdr_.type = 0xFF;
    write_header();
    auto r = parse_header(std::span<const std::byte>{raw_});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), ParseError::UnknownMessageType);
}

TEST_F(ParseHeaderErrors, UnknownFlags) {
    hdr_.flags = 0x80; // bit outside valid mask
    write_header();
    auto r = parse_header(std::span<const std::byte>{raw_});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), ParseError::UnknownFlags);
}

TEST_F(ParseHeaderErrors, PayloadTooLarge) {
    hdr_.payload_len = MAX_PAYLOAD_LEN + 1;
    write_header();
    auto r = parse_header(std::span<const std::byte>{raw_});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), ParseError::PayloadTooLarge);
}

TEST_F(ParseHeaderErrors, ValidHeaderSucceeds) {
    auto r = parse_header(std::span<const std::byte>{raw_});
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->magic, MAGIC);
    EXPECT_EQ(r->sequence, 1u);
}

// parse_header: valid flags combinations
TEST_F(ParseHeaderErrors, AllValidFlagsBits) {
    hdr_.flags = std::to_underlying(Flags::RETAIN | Flags::NO_ACK | Flags::COMPRESSED);
    write_header();
    EXPECT_TRUE(parse_header(std::span<const std::byte>{raw_}).has_value());
}

// Subscribe / Unsubscribe round-trips
TEST(Protocol, SubscribeRoundTrip) {
    const auto f = round_trip([](auto buf) {
        return encode_subscribe(buf, 1, "sensors/temp");
    });
    EXPECT_EQ(static_cast<MessageType>(f.header.type), MessageType::SUBSCRIBE);
    EXPECT_EQ(f.header.sequence, 1u);
    EXPECT_EQ(std::get<SubscribeMsg>(f.payload).topic, "sensors/temp");
}

TEST(Protocol, UnsubscribeRoundTrip) {
    auto f = round_trip([](auto buf) {
        return encode_subscribe(buf, 7, "events/click", MessageType::UNSUBSCRIBE);
    });
    EXPECT_EQ(static_cast<MessageType>(f.header.type), MessageType::UNSUBSCRIBE);
    EXPECT_EQ(std::get<UnsubscribeMsg>(f.payload).topic, "events/click");
}

TEST(Protocol, SubscribeEmptyTopic) {
    auto f = round_trip([](auto buf) {
        return encode_subscribe(buf, 0, "");
    });
    EXPECT_EQ(std::get<SubscribeMsg>(f.payload).topic, "");
}

TEST(Protocol, SubscribeMaxTopic) {
    const std::string topic(MAX_TOPIC_LEN, 'x');
    auto f = round_trip([&](auto buf) {
        return encode_subscribe(buf, 99, topic);
    });
    EXPECT_EQ(std::get<SubscribeMsg>(f.payload).topic, topic);
}

TEST(Protocol, SubscribeTopicTooLong) {
    std::array<std::byte, 512> buf{};
    const std::string topic(MAX_TOPIC_LEN + 1, 'x');
    auto r = encode_subscribe(std::span{buf}, 1, topic);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), EncodeError::TopicTooLong);
}

TEST(Protocol, SubscribeBufferTooSmall) {
    std::array<std::byte, sizeof(FrameHeader)> buf{}; // no room for payload
    auto r = encode_subscribe(std::span{buf}, 1, "abc");
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), EncodeError::BufferTooSmall);
}

// Publish round-trips
TEST(Protocol, PublishRoundTrip) {
    const std::string body_str = "hello world";
    std::span<const std::byte> body{reinterpret_cast<const std::byte*>(body_str.data()), body_str.size()};

    auto f = round_trip([&](auto buf) {
        return encode_publish(buf, 42, "chat/room1", body);
    });
    EXPECT_EQ(static_cast<MessageType>(f.header.type), MessageType::PUBLISH);
    auto& msg = std::get<PublishMsg>(f.payload);
    EXPECT_EQ(msg.topic, "chat/room1");
    EXPECT_EQ(msg.body.size(), body_str.size());
    EXPECT_EQ(std::memcmp(msg.body.data(), body_str.data(), body_str.size()), 0);
}

TEST(Protocol, PublishEmptyBody) {
    auto f = round_trip([](auto buf) {
        return encode_publish(buf, 1, "t", std::span<const std::byte>{});
    });
    auto& msg = std::get<PublishMsg>(f.payload);
    EXPECT_EQ(msg.topic, "t");
    EXPECT_TRUE(msg.body.empty());
}

TEST(Protocol, PublishWithFlags) {
    std::array<std::byte, 256> buf{};
    auto enc = encode_publish(std::span{buf}, 10, "f", std::span<const std::byte>{}, Flags::RETAIN | Flags::NO_ACK);
    ASSERT_TRUE(enc.has_value());

    auto hdr = parse_header(as_bytes(buf, *enc));
    ASSERT_TRUE(hdr.has_value());
    EXPECT_EQ(static_cast<Flags>(hdr->flags), Flags::RETAIN | Flags::NO_ACK);
}

TEST(Protocol, PublishLargePayload) {
    // Largest valid: body fills remaining space up to MAX_PAYLOAD_LEN
    const std::string topic = "t";
    const size_t max_body = MAX_PAYLOAD_LEN - sizeof(uint16_t) - topic.size();
    std::vector<std::byte> body(max_body);
    std::vector<std::byte> buf(sizeof(FrameHeader) + MAX_PAYLOAD_LEN + 64);

    auto r = encode_publish(std::span{buf}, 1, topic,
                            std::span<const std::byte>{body});
    ASSERT_TRUE(r.has_value());
}

TEST(Protocol, PublishPayloadTooLarge) {
    const std::string topic = "t";
    const size_t over_body = MAX_PAYLOAD_LEN; // topic_len + topic + body > MAX_PAYLOAD_LEN
    std::vector<std::byte> body(over_body);
    std::vector<std::byte> buf(sizeof(FrameHeader) + over_body + 64);

    auto r = encode_publish(std::span{buf}, 1, topic,
                            std::span<const std::byte>{body});
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), EncodeError::PayloadTooLarge);
}

TEST(Protocol, PublishBufferTooSmall) {
    std::array<std::byte, sizeof(FrameHeader) + 2> buf{};
    const std::string body_str = "data";
    std::span<const std::byte> body{reinterpret_cast<const std::byte*>(body_str.data()), body_str.size()};

    auto r = encode_publish(std::span{buf}, 1, "topic", body);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), EncodeError::BufferTooSmall);
}

// ---------------------------------------------------------------------------
// ACK round-trip
// ---------------------------------------------------------------------------

TEST(Protocol, AckRoundTrip) {
    auto f = round_trip([](auto buf) { return encode_ack(buf, 5, 4); });
    EXPECT_EQ(static_cast<MessageType>(f.header.type), MessageType::ACK);
    EXPECT_EQ(f.header.sequence, 5u);
    EXPECT_EQ(std::get<AckMsg>(f.payload).acked_seq, 4u);
}

TEST(Protocol, AckMaxSequence) {
    auto f = round_trip([](auto buf) {
        return encode_ack(buf, UINT64_MAX, UINT64_MAX);
    });
    EXPECT_EQ(std::get<AckMsg>(f.payload).acked_seq, UINT64_MAX);
}

TEST(Protocol, AckBufferTooSmall) {
    std::array<std::byte, sizeof(FrameHeader)> buf{};
    auto r = encode_ack(std::span{buf}, 1, 1);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), EncodeError::BufferTooSmall);
}

// Error message round-trip
TEST(Protocol, ErrorWithMessage) {
    auto f = round_trip([](auto buf) {
        return encode_error(buf, 3, ErrorCode::UNKNOWN_TOPIC, "no such topic");
    });
    EXPECT_EQ(static_cast<MessageType>(f.header.type), MessageType::ERROR);
    auto& msg = std::get<ErrorMsg>(f.payload);
    EXPECT_EQ(msg.code, ErrorCode::UNKNOWN_TOPIC);
    EXPECT_EQ(msg.error_msg, "no such topic");
}

TEST(Protocol, ErrorWithoutMessage) {
    auto f = round_trip([](auto buf) {
        return encode_error(buf, 1, ErrorCode::QUEUE_FULL);
    });
    auto& msg = std::get<ErrorMsg>(f.payload);
    EXPECT_EQ(msg.code, ErrorCode::QUEUE_FULL);
    EXPECT_TRUE(msg.error_msg.empty());
}

TEST(Protocol, ErrorBufferTooSmall) {
    std::array<std::byte, sizeof(FrameHeader)> buf{};
    auto r = encode_error(std::span{buf}, 1, ErrorCode::OK);
    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), EncodeError::BufferTooSmall);
}

// decode_frame — truncated payloads

class DecodeFrameTruncated : public ::testing::Test {
protected:
    // Encode a valid frame, then feed decode_frame a short payload span.
    void expect_buffer_too_small(MessageType type) {
        std::array<std::byte, 512> buf{};
        size_t enc_len = 0;

        switch (type) {
            case MessageType::SUBSCRIBE:
                enc_len = *encode_subscribe(std::span{buf}, 1, "topic");
                break;
            case MessageType::PUBLISH: {
                const std::byte b{0x42};
                enc_len = *encode_publish(std::span{buf}, 1, "topic", std::span{&b, 1});
                break;
            }
            case MessageType::ACK:
                enc_len = *encode_ack(std::span{buf}, 1, 1);
                break;
            case MessageType::ERROR:
                enc_len = *encode_error(std::span{buf}, 1, ErrorCode::OK, "err");
                break;
            default: FAIL();
        }

        auto hdr = *parse_header(as_bytes(buf, enc_len));

        // Give decode_frame only 1 byte of payload — should fail
        std::span<const std::byte> short_payload{buf.data() + sizeof(FrameHeader), 1};
        auto r = decode_frame(hdr, short_payload);
        ASSERT_FALSE(r.has_value());
        EXPECT_EQ(r.error(), ParseError::BufferTooSmall);
    }
};

TEST_F(DecodeFrameTruncated, Subscribe)   { expect_buffer_too_small(MessageType::SUBSCRIBE); }
TEST_F(DecodeFrameTruncated, Publish)     { expect_buffer_too_small(MessageType::PUBLISH); }
TEST_F(DecodeFrameTruncated, Ack)         { expect_buffer_too_small(MessageType::ACK); }
TEST_F(DecodeFrameTruncated, Error)       { expect_buffer_too_small(MessageType::ERROR); }

// Flags bitwise operators
TEST(ProtocolFlags, BitwiseOr) {
    auto combined = Flags::RETAIN | Flags::NO_ACK;
    EXPECT_EQ(static_cast<uint8_t>(combined), 0x01 | 0x02);
}

TEST(ProtocolFlags, BitwiseAnd) {
    auto combined = Flags::RETAIN | Flags::NO_ACK;
    EXPECT_EQ(combined & Flags::RETAIN, Flags::RETAIN);
    EXPECT_EQ(combined & Flags::COMPRESSED, Flags::NONE);
}

// to_string coverage
TEST(ProtocolStrings, MessageTypes) {
    EXPECT_EQ(to_string(MessageType::SUBSCRIBE),   "SUBSCRIBE");
    EXPECT_EQ(to_string(MessageType::UNSUBSCRIBE), "UNSUBSCRIBE");
    EXPECT_EQ(to_string(MessageType::PUBLISH),     "PUBLISH");
    EXPECT_EQ(to_string(MessageType::ACK),         "ACK");
    EXPECT_EQ(to_string(MessageType::ERROR),       "ERROR");
}

TEST(ProtocolStrings, ErrorCodes) {
    EXPECT_EQ(to_string(ErrorCode::OK),                "OK");
    EXPECT_EQ(to_string(ErrorCode::UNKNOWN_TOPIC),     "UNKNOWN_TOPIC");
    EXPECT_EQ(to_string(ErrorCode::PAYLOAD_TOO_LARGE), "PAYLOAD_TOO_LARGE");
    EXPECT_EQ(to_string(ErrorCode::INVALID_FRAME),     "INVALID_FRAME");
    EXPECT_EQ(to_string(ErrorCode::QUEUE_FULL),        "QUEUE_FULL");
    EXPECT_EQ(to_string(ErrorCode::UNSUPPORTED_VER),   "UNSUPPORTED_VER");
}

// FrameHeader struct layout
TEST(ProtocolLayout, HeaderSize) {
    EXPECT_EQ(sizeof(FrameHeader), 24u);
    EXPECT_EQ(alignof(FrameHeader), 1u);
}
