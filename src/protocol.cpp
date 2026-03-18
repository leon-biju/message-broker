
/*************************** protocol.cpp ***************************
 * Definitions for all non-inline parts of the wire protocol.
 * So essentially functions that are either:
 *     - Too large to benefit from inlining
 *     - Debug/logging only so no point clogging up header
 *     - Called at most once per frame with negligible call overhead
 *
 * Heap allocation is banned here bud
 * decode
 */

#include <broker/protocol.hpp>

// Non-trivial header validation checks here that too expensive to inline in header
bool validate_header(const FrameHeader &hdr) noexcept {
    //TODO: Check for these
    return true;
}

std::expected<DecodedFrame, ParseError> decode_frame(const FrameHeader &hdr, std::span<const std::byte> payload) noexcept {
    // Check total length of payload
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
