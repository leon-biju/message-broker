#include <gtest/gtest.h>
#include <broker/tcp_gateway.hpp>

#include <cstring>

// OutboundMessage: Test that it does do the small buffer optimisation correctly and 
// that copying an OutboundMessage with a heap buffer does a deep copy.
//
// fyi INLINE_CAP == 256.  write_buf uses inline storage when needed <= INLINE_CAP.

TEST(OutboundMessageSBO, InlinePath) {
    OutboundMessage msg;
    auto buf = msg.write_buf(255);

    EXPECT_FALSE(msg.heap_buf);
    EXPECT_EQ(msg.data(), msg.inline_buf.data());
    // write_buf returns a span over the full INLINE_CAP even for smaller requests
    EXPECT_EQ(buf.size(), OutboundMessage::INLINE_CAP);

    // Verify data() points into the same storage after a write
    std::memset(buf.data(), 0xAB, 255);
    msg.len = 255;
    EXPECT_EQ(static_cast<uint8_t>(msg.data()[0]), 0xABu);
}

TEST(OutboundMessageSBO, BoundaryIsInline) {
    // 256 == INLINE_CAP: condition is needed <= INLINE_CAP, so this is still inline
    OutboundMessage msg;
    [[maybe_unused]] auto buf = msg.write_buf(256);

    EXPECT_FALSE(msg.heap_buf);
    EXPECT_EQ(msg.data(), msg.inline_buf.data());
}

TEST(OutboundMessageSBO, HeapPath) {
    OutboundMessage msg;
    auto buf = msg.write_buf(257);

    ASSERT_TRUE(msg.heap_buf);
    EXPECT_EQ(msg.data(), msg.heap_buf.get());
    // write_buf returns a span over exactly the requested size for heap allocations
    EXPECT_EQ(buf.size(), 257u);

    // Verify data() is consistent with the heap pointer
    std::memset(buf.data(), 0xCD, 257);
    msg.len = 257;
    EXPECT_EQ(static_cast<uint8_t>(msg.data()[0]),   0xCDu);
    EXPECT_EQ(static_cast<uint8_t>(msg.data()[256]), 0xCDu);
}

TEST(OutboundMessageSBO, CopyWithHeapBufDeepCopies) {
    OutboundMessage src;
    auto buf = src.write_buf(257);
    std::memset(buf.data(), 0xEE, 257);
    src.len = 257;

    OutboundMessage dst(src);  // copy constructor

    // Distinct heap pointers
    ASSERT_TRUE(src.heap_buf);
    ASSERT_TRUE(dst.heap_buf);
    EXPECT_NE(src.heap_buf.get(), dst.heap_buf.get());

    // Same content
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), 257), 0);

    // Mutating dst does not affect src
    std::memset(dst.heap_buf.get(), 0x00, 257);
    EXPECT_EQ(static_cast<uint8_t>(src.data()[0]), 0xEEu);
}
