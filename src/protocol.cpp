
/*************************** protocol.cpp ***************************
 * Definitions for all non-inline parts of the wire protocol.
 * So essentially functions that are either:
 *     - Too large to benefit from inlining
 *     - Debug/logging only so no point clogging up header
 *     - Called at most once per frame with negligible call overhead
 */

#include <broker/protocol.hpp>

std::expected<DecodedFrame, ParseError> decode_frame(const FrameHeader &hdr, std::span<const std::byte> payload) noexcept {
    if (payload.size() < hdr.payload_len) {
        return std::unexpected(ParseError::BufferTooSmall);
    }

    const auto type = static_cast<MessageType>(hdr.type);
    const auto data = payload.first(hdr.payload_len);

    switch (type) {
        case MessageType::SUBSCRIBE:
        case MessageType::UNSUBSCRIBE: {
            if (data.size() < sizeof(uint16_t)) return std::unexpected(ParseError::BufferTooSmall);

            uint16_t topic_len;
            std::memcpy(&topic_len, data.data(), sizeof(topic_len));

            if (data.size() < sizeof(uint16_t) + topic_len) return std::unexpected(ParseError::BufferTooSmall);

            const auto* topic_ptr = reinterpret_cast<const char*>(data.data() + sizeof(uint16_t));
            std::string_view topic{topic_ptr, topic_len};

            if (type == MessageType::SUBSCRIBE)
                return DecodedFrame{hdr, SubscribeMsg{topic}};
            return DecodedFrame{hdr, UnsubscribeMsg{topic}};
        }

        case MessageType::PUBLISH: {
            if (data.size() < sizeof(uint16_t)) return std::unexpected(ParseError::BufferTooSmall);

            uint16_t topic_len;
            std::memcpy(&topic_len, data.data(), sizeof(topic_len));

            const size_t topic_end = sizeof(uint16_t) + topic_len;
            if (data.size() < topic_end) return std::unexpected(ParseError::BufferTooSmall);

            const auto* topic_ptr = reinterpret_cast<const char*>(data.data() + sizeof(uint16_t));
            std::string_view topic{topic_ptr, topic_len};

            auto body = data.subspan(topic_end);

            return DecodedFrame{hdr, PublishMsg{topic, body}};
        }

        case MessageType::ACK: {
            if (data.size() < sizeof(uint64_t)) return std::unexpected(ParseError::BufferTooSmall);

            uint64_t acked_seq;
            std::memcpy(&acked_seq, data.data(), sizeof(acked_seq));

            return DecodedFrame{hdr, AckMsg{acked_seq}};
        }

        case MessageType::ERROR: {
            if (data.size() < sizeof(uint16_t) + sizeof(uint16_t)) return std::unexpected(ParseError::BufferTooSmall);

            uint16_t error_code_raw;
            std::memcpy(&error_code_raw, data.data(), sizeof(error_code_raw));

            uint16_t msg_length;
            std::memcpy(&msg_length, data.data() + sizeof(error_code_raw), sizeof(msg_length));

            const size_t msg_end = sizeof(error_code_raw) + sizeof(msg_length) + msg_length;
            if (data.size() < msg_end) return std::unexpected(ParseError::BufferTooSmall);

            const auto* msg_ptr = reinterpret_cast<const char*>(
                data.data() + sizeof(error_code_raw) + sizeof(msg_length));
            std::string_view error_msg{msg_ptr, msg_length};

            return DecodedFrame{hdr, ErrorMsg{static_cast<ErrorCode>(error_code_raw), error_msg}};
        }

        default:
            return std::unexpected(ParseError::UnknownMessageType);
    }
}


std::string_view to_string(const MessageType t) {
    switch (t) {
        case MessageType::SUBSCRIBE:   return "SUBSCRIBE";
        case MessageType::UNSUBSCRIBE: return "UNSUBSCRIBE";
        case MessageType::PUBLISH:     return "PUBLISH";
        case MessageType::ACK:         return "ACK";
        case MessageType::ERROR:       return "ERROR";
        default:                       return "UNKNOWN";
    }
}
std::string_view to_string(const ErrorCode t) {
    switch (t) {
        case ErrorCode::INVALID_FRAME:         return "INVALID_FRAME";
        case ErrorCode::OK:                    return "OK";
        case ErrorCode::PAYLOAD_TOO_LARGE:     return "PAYLOAD_TOO_LARGE";
        case ErrorCode::QUEUE_FULL:            return "QUEUE_FULL";
        case ErrorCode::UNKNOWN_TOPIC:         return "UNKNOWN_TOPIC";
        case ErrorCode::UNSUPPORTED_VER:       return "UNSUPPORTED_VER";
        default:                               return "UNKNOWN";
    }
}
