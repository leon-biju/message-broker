#ifndef SPSC_QUEUE_SPSC_QUEUE_H
#define SPSC_QUEUE_SPSC_QUEUE_H

#include <array>
#include <atomic>

// The tails and heads are monotonically increasing
// They WILL overflow, but this is valid and expected

// The cache values may be non-atomic since only one thread can access them

struct alignas(64) ProducerData {
    std::atomic<size_t> tail {0};
    size_t head_cache {0};
};

struct alignas(64) ConsumerData {
    std::atomic<size_t> head {0};
    size_t tail_cache {0};
};

template<typename T, size_t Capacity>
class SPSCQueue {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
    static constexpr size_t MASK = Capacity - 1;


    std::array<T, Capacity> data_;
    ProducerData producer_;
    ConsumerData consumer_;

public:
    SPSCQueue() = default;

    SPSCQueue(const SPSCQueue&) = delete;
    SPSCQueue& operator=(const SPSCQueue&) = delete;
    SPSCQueue(SPSCQueue&&) = delete;
    SPSCQueue& operator=(SPSCQueue&&) = delete;

    ~SPSCQueue() = default;

    bool push(T val) {
        const auto tail = producer_.tail.load(std::memory_order_relaxed);

        // Check against cached head first and only if it looks full cross the cache boundary and check the actual head
        if (tail - producer_.head_cache == Capacity) {
            producer_.head_cache = consumer_.head.load(std::memory_order_acquire); // update cache and recheck
            if (tail - producer_.head_cache == Capacity) {
                return false;
            }
        }


        data_[tail & MASK] = std::move(val);
        producer_.tail.store(tail + 1, std::memory_order_release);
        return true;
    }

    std::optional<T> pop() {
        const auto head = consumer_.head.load(std::memory_order_relaxed);

        // Check against cached tail first and only if it looks full cross the cache boundary and check the actual tail
        if (head == consumer_.tail_cache) {
            consumer_.tail_cache = producer_.tail.load(std::memory_order_acquire); // update cache and recheck
            if (head == consumer_.tail_cache) {
                return std::nullopt;
            }
        }


        T val = std::move(data_[head & MASK]);
        consumer_.head.store(head + 1, std::memory_order_release);
        return val;
    }

    [[nodiscard]] bool empty() const {
        const auto head {consumer_.head.load(std::memory_order_relaxed)};
        const auto tail {producer_.tail.load(std::memory_order_relaxed)};

        if (head == tail) {
            return true;
        }
        return false;
    }

    [[nodiscard]] bool full() const {
        const auto head { consumer_.head.load(std::memory_order_relaxed)};
        const auto tail { producer_.tail.load(std::memory_order_relaxed)};

        if (head + Capacity - tail == 0) {
            return true;
        }
        return false;
    }

    // WARNING: should only be used for metrics, not for control flow
    [[nodiscard]] size_t approx_size() const { 
        const auto head { consumer_.head.load(std::memory_order_relaxed)};
        const auto tail { producer_.tail.load(std::memory_order_relaxed)};

        return tail - head;
    }
};

#endif //SPSC_QUEUE_SPSC_QUEUE_H