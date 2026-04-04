#ifndef INCLUDE_BROKER_PROTOCOL_HPP_
#define INCLUDE_BROKER_PROTOCOL_HPP_

/*************************** protocol.hpp ***************************
 * This file contains everything the caller needs to encode and decode frames
 *     - FrameHeader struct is a packed 24B struct (little Endian)
 *     - All enums: MessageType, Flags, ErrorCode
 *     - Parsing/Endoding error enums: ParseError, EncodeError
 *     - Inlined encode_(MessageType) functions
 *     - Inlined parse_header function
 *     - Declaration of decode_frame function
 *
 * Encoding/decoding is zero allocation. The caller MUST provide the allocated buffer for these functions
 * to write to.
 * It is the caller's responsibility to keep alive the receive-buffer.
 *
 * This assumes little endian throughout. So any big endian ports must byte-swap struct members.
 */

// TODO: Namespace this file

#include <bit>
#include <cstdint>
#include <cstring>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <variant>

static_assert(std::endian::native == std::endian::little, "Wire protocol assumes little-endian");

/*
 * MESSAGE STRUCTURE:
 *     HEADER  (Fixed 24B) -- contains payload size
 *     PAYLOAD (Variable size)
 */

/*************************** Constants ***************************/
inline constexpr uint16_t MAGIC           {0xBEEF}; // Sync keyword to detect misaligned reads
inline constexpr uint8_t  PROTO_VERSION   {1};
inline constexpr size_t   MAX_TOPIC_LEN   {128};    // To cap stack allocation
inline constexpr size_t   MAX_PAYLOAD_LEN {65536};  // 64KiB cap


enum class MessageType: uint8_t {
    SUBSCRIBE   = 0x01,
    UNSUBSCRIBE = 0x02,
    PUBLISH     = 0x03,
    ACK         = 0x04,
    ERROR       = 0x05,
};

enum class Flags : uint8_t {
    NONE = 0x00,
    RETAIN = 0x01,
    NO_ACK = 0x02,
    COMPRESSED = 0x04, //todo: implement this
};
constexpr Flags operator|(Flags lhs, Flags rhs) {
    return static_cast<Flags>(static_cast<uint8_t>(lhs) | static_cast<uint8_t>(rhs));
}
constexpr Flags operator&(Flags lhs, Flags rhs) {
    return static_cast<Flags>(static_cast<uint8_t>(lhs) & static_cast<uint8_t>(rhs));
}
constexpr bool has_flag(uint8_t raw_flags, Flags flag) {
    return (static_cast<Flags>(raw_flags) & flag) != Flags::NONE;
}

enum class ErrorCode : uint16_t {
    OK                = 0,
    UNKNOWN_TOPIC     = 1,
    PAYLOAD_TOO_LARGE = 2,
    INVALID_FRAME     = 3,
    QUEUE_FULL        = 4, //used for backpressure to tell producer to slow down
    UNSUPPORTED_VER   = 5,
};
/*************************** Parsing/Encoding errors ***************************/
// For std::expected returns
enum class ParseError {
    BadMagic,
    UnsupportedVersion,
    UnknownMessageType,
    UnknownFlags,
    PayloadTooLarge,
    BufferTooSmall,
};
enum class EncodeError {
    BufferTooSmall,
    TopicTooLong,
    PayloadTooLarge
};


/*
 * HEADER STRUCTURE (fixed 24 Bytes)
 *  0 - 1 : Magic value       0xBEEF
 *      2 : version           PROTO_VERSION
 *      3 : type              MessageType
 *      4 : flags             Flags
 *  5 - 7 : reserved0         (zero on send, ignore on receive)
 *  8 - 15: sequence          monotonic sequence number per-connection
 * 16 - 19: payload_len       byte length of payload that follows
 * 20 - 23: reserved1         (zero on send, ignore on receive)
 */

struct FrameHeader {
    uint16_t    magic;           // must equal MAGIC
    uint8_t     version;         // must equal PROTO_VERSION
    uint8_t     type;            // cast to MessageType
    uint8_t     flags;           // cast to Flags
    uint8_t     reserved0[3];    // pad first 5 bytes to 8 bytes for sequence alignment
    uint64_t    sequence;
    uint32_t    payload_len;
    uint8_t     reserved1[4];    // pad rest of header to 24 bytes; todo: CRC32 Checksum here
};

static_assert(sizeof(FrameHeader) == 24, "bro frame header must be 24B");

/*  Payload Layouts depending on MessageType
 *  SUBSCRIBE/UNSUBSCRIBE:
 *      topic_len (2B), topic bytes (UTF-8 no \0)
 *  PUBLISH:
 *      topic_len (2B), topic bytes, message bytes
 *  ACK:
 *      acked_seq (8B)
 *  ERROR:
 *      error_code(2B), msg_length(2B), UTF-8 message
 */

/*************************** Decoded Payload Types ***************************/
struct SubscribeMsg   { std::string_view topic; };
struct UnsubscribeMsg { std::string_view topic; };

struct PublishMsg {
    std::string_view topic;
    std::span<const std::byte> body;
};

struct AckMsg   { uint64_t acked_seq; };

struct ErrorMsg {
    ErrorCode        code;
    std::string_view error_msg;
};

// Not an actual message that we can receive, but if a client disconnects then this gets pushed onto the inbound queue
struct DisconnectMsg {};

// Another non-message message, enqueued by Router::stop() to unblock wait_dequeue_bulk on the inbound queue
struct ShutdownMsg {};

struct DecodedFrame {
    FrameHeader header;
    std::variant<SubscribeMsg, UnsubscribeMsg, PublishMsg, AckMsg, ErrorMsg, DisconnectMsg, ShutdownMsg> payload;
};



/*************************** Decoding functions ***************************/
[[nodiscard]] inline std::expected<FrameHeader, ParseError> parse_header (
    // Careful, this assumes that raw begins exactly at the header start so misaligned bytes will mess it up
    const std::span<const std::byte> raw) noexcept {
    if (raw.size() < sizeof(FrameHeader)) return std::unexpected(ParseError::BufferTooSmall);

    FrameHeader header; // NOLINT (literally copy into it straight after)
    std::memcpy(&header, raw.data(), sizeof(FrameHeader));

    if (header.magic != MAGIC) return std::unexpected(ParseError::BadMagic);
    if (header.version != PROTO_VERSION) return std::unexpected(ParseError::UnsupportedVersion);

    switch (static_cast<MessageType>(header.type)) {
        case MessageType::SUBSCRIBE:
        case MessageType::UNSUBSCRIBE:
        case MessageType::PUBLISH:
        case MessageType::ACK:
        case MessageType::ERROR:
            break;
        default:
            return std::unexpected(ParseError::UnknownMessageType);
    }
    constexpr uint8_t VALID_FLAGS_MASK = static_cast<uint8_t>(Flags::RETAIN)
                                       | static_cast<uint8_t>(Flags::NO_ACK)
                                       | static_cast<uint8_t>(Flags::COMPRESSED);
    if (header.flags & ~VALID_FLAGS_MASK) {
        return std::unexpected(ParseError::UnknownFlags);
    }
    if (header.payload_len > MAX_PAYLOAD_LEN) {
        return std::unexpected(ParseError::PayloadTooLarge);
    }
    return header;
}


/***  ENCODING ***/

// Stamp the sequence field on an already-encoded frame. Called by the gateway
// immediately before writing to the socket
// Since the gateway owns outbound sequence numbering; encoders leave it as 0.
inline void write_sequence(const std::span<std::byte> frame, const uint64_t seq) noexcept {
    reinterpret_cast<FrameHeader*>(frame.data())->sequence = seq;
}


[[nodiscard]] inline std::expected<size_t, EncodeError> encode_subscribe ( //also unsubscribe
    std::span<std::byte> buf,
    const uint64_t seq,
    const std::string_view topic,
    const MessageType type = MessageType::SUBSCRIBE) noexcept
{
    if (topic.size() > MAX_TOPIC_LEN) {
        return std::unexpected(EncodeError::TopicTooLong);
    }

    const uint16_t topic_len   = topic.size();  //has to fit into 2B
    const uint32_t payload_len = sizeof(topic_len) + topic_len;
    const size_t   total_len   = sizeof(FrameHeader) + payload_len;

    if (total_len > buf.size()) {
        return std::unexpected(EncodeError::BufferTooSmall);
    }

    const FrameHeader header {
        .magic       = MAGIC,
        .version     = PROTO_VERSION,
        .type        = std::to_underlying(type),
        .flags       = std::to_underlying(Flags::NONE),

        // shuts up compiler and disappears basically at -O3
        .reserved0   = {},

        .sequence    = seq,
        .payload_len = payload_len,
        .reserved1   = {}
    };

    auto* ptr = buf.data();

    std::memcpy(ptr, &header, sizeof(header));
    ptr += sizeof(header);
    std::memcpy(ptr, &topic_len, sizeof(topic_len));
    ptr += sizeof(topic_len);
    std::memcpy(ptr, topic.data(), topic_len);

    return total_len;
}

[[nodiscard]] inline std::expected<size_t, EncodeError> encode_publish (
    std::span<std::byte> buf,
    const uint64_t seq,
    const std::string_view topic,
    const std::span<const std::byte> body,
    const Flags flags = Flags::NONE) noexcept
{
    if (topic.size() > MAX_TOPIC_LEN) {
        return std::unexpected(EncodeError::TopicTooLong);
    }
    const uint16_t topic_len   = topic.size();
    const uint32_t payload_len = sizeof(topic_len) + topic_len + body.size();
    const size_t   total_len   = sizeof(FrameHeader) + payload_len;

    if (payload_len > MAX_PAYLOAD_LEN) {
        return std::unexpected(EncodeError::PayloadTooLarge);
    }
    if (total_len > buf.size()) {
        return std::unexpected(EncodeError::BufferTooSmall);
    }

    const FrameHeader header {
        .magic       = MAGIC,
        .version     = PROTO_VERSION,
        .type        = std::to_underlying(MessageType::PUBLISH),
        .flags       = std::to_underlying(flags),

        // shuts up compiler and disappears basically at -O3
        .reserved0   = {},

        .sequence    = seq,
        .payload_len = payload_len,
        .reserved1   = {}
    };

    auto* ptr = buf.data();

    std::memcpy(ptr, &header, sizeof(header));
    ptr += sizeof(header);
    std::memcpy(ptr, &topic_len, sizeof(topic_len));
    ptr += sizeof(topic_len);
    std::memcpy(ptr, topic.data(), topic_len);
    ptr += topic_len;
    std::memcpy(ptr, body.data(), body.size());

    return total_len;
}


[[nodiscard]] inline std::expected<size_t, EncodeError> encode_ack (
    std::span<std::byte> buf,
    const uint64_t seq,
    const uint64_t acked_seq) noexcept
{
    constexpr uint32_t payload_len = sizeof(acked_seq);
    static_assert(payload_len <= MAX_PAYLOAD_LEN, "Buddy why is acked_seq so big???");
    constexpr size_t   total_len   = sizeof(FrameHeader) + payload_len;
    if (total_len > buf.size()) {
        return std::unexpected(EncodeError::BufferTooSmall);
    }

    const FrameHeader header {
        .magic       = MAGIC,
        .version     = PROTO_VERSION,
        .type        = std::to_underlying(MessageType::ACK),
        .flags       = std::to_underlying(Flags::NONE),

        // shuts up compiler and disappears basically at -O3
        .reserved0   = {},

        .sequence    = seq,
        .payload_len = payload_len,
        .reserved1   = {}
    };

    auto* ptr = buf.data();
    std::memcpy(ptr, &header, sizeof(header));
    ptr += sizeof(header);
    std::memcpy(ptr, &acked_seq, sizeof(acked_seq));

    return total_len;
}

[[nodiscard]] inline std::expected<size_t, EncodeError> encode_error (
    std::span<std::byte> buf,
    const uint64_t seq,
    const ErrorCode error_code,
    const std::optional<std::string_view> error_msg = std::nullopt) noexcept
{
    const uint16_t msg_length  = (error_msg ? error_msg->size() : 0);
    const uint32_t payload_len = sizeof(error_code) + sizeof(msg_length) + msg_length;
    if (payload_len > MAX_PAYLOAD_LEN) {
        return std::unexpected(EncodeError::PayloadTooLarge);
    }
    const size_t   total_len   = sizeof(FrameHeader) + payload_len;

    if (total_len > buf.size()) {
        return std::unexpected(EncodeError::BufferTooSmall);
    }

    const FrameHeader header {
        .magic       = MAGIC,
        .version     = PROTO_VERSION,
        .type        = std::to_underlying(MessageType::ERROR),
        .flags       = std::to_underlying(Flags::NONE),

        // shuts up compiler and disappears basically at -O3
        .reserved0   = {},
        
        .sequence    = seq,
        .payload_len = payload_len,
        .reserved1   = {}
    };

    auto* ptr = buf.data();
    std::memcpy(ptr, &header, sizeof(header));
    ptr += sizeof(header);
    std::memcpy(ptr, &error_code, sizeof(error_code));
    ptr += sizeof(error_code);
    std::memcpy(ptr, &msg_length, sizeof(msg_length));

    if (error_msg) {
        ptr += sizeof(msg_length);
        std::memcpy(ptr, error_msg->data(), error_msg->size());
    }
    return total_len;
}

[[nodiscard]] std::expected<DecodedFrame, ParseError>
decode_frame(const FrameHeader& hdr, std::span<const std::byte> payload) noexcept;

std::string_view to_string(MessageType t);
std::string_view to_string(ErrorCode t);

#endif
