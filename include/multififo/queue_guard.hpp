/**
******************************************************************************
* @file:   queue_guard.hpp
*
* @author: Marvin Williams
* @date:   2021/11/09 18:05
* @brief:
*******************************************************************************
**/

#pragma once

#include "multififo/build_config.hpp"

#include <atomic>
#include <chrono>
#include <type_traits>

namespace multififo {

template <typename Queue>
class alignas(build_config::l1_cache_line_size) QueueGuard {
    using queue_type = Queue;
    std::atomic<std::uint64_t> top_tick_ = std::numeric_limits<std::uint64_t>::max();
    std::atomic_uint32_t lock_ = 0;
    queue_type queue_;

   public:
    explicit QueueGuard() = default;

    explicit QueueGuard(queue_type queue) : queue_(std::move(queue)) {
    }

    [[nodiscard]] std::uint64_t top_tick() const noexcept {
        return top_tick_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool empty() const noexcept {
        return top_tick() == std::numeric_limits<std::uint64_t>::max();
    }

    bool try_lock() noexcept {
        // Test first to not invalidate the cache line
        return (lock_.load(std::memory_order_relaxed) & 1U) == 0U && (lock_.exchange(1U, std::memory_order_acquire) & 1) == 0U;
    }

    bool try_lock(bool force, uint32_t mark) noexcept {
        auto current = lock_.load(std::memory_order_relaxed);
        while (true) {
            if ((current & 1U) == 1U) {
                return false;
            }
            if (!force && (current >> 1) != 0U && (current >> 1) != mark + 1) {
                return false;
            }
            if (lock_.compare_exchange_strong(current, ((mark + 1) << 1) | 1, std::memory_order_acquire,
                                              std::memory_order_relaxed)) {
                return true;
            }
        }
    }

    void popped() {
        auto tick = (queue_.empty() ? std::numeric_limits<std::uint64_t>::max() : queue_.top().tick);
        top_tick_.store(tick, std::memory_order_relaxed);
    }

    void pushed() {
        auto tick = queue_.top().tick;
        if (tick != top_tick()) {
            top_tick_.store(tick, std::memory_order_relaxed);
        }
    }

    void unlock() {
        lock_.store(0U, std::memory_order_release);
    }

    void unlock(uint32_t mark) {
        lock_.store((mark + 1) << 1, std::memory_order_release);
    }

    queue_type& get_queue() noexcept {
        return queue_;
    }
};

}  // namespace multififo
