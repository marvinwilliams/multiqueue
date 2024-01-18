/**
******************************************************************************
* @file:   pq_guard.hpp
*
* @author: Marvin Williams
* @date:   2021/11/09 18:05
* @brief:
*******************************************************************************
**/

#pragma once

#include "multiqueue/build_config.hpp"

#include <atomic>
#include <cassert>
#include <limits>
#include <memory>
#include <stdexcept>
#include <type_traits>

namespace multiqueue {

template <typename Key, typename Value, typename KeyOfValue, typename PriorityQueue, typename Sentinel>
class alignas(build_config::l1_cache_line_size) PQGuard {
    using key_type = Key;
    using value_type = Value;
    using priority_queue_type = PriorityQueue;
    static_assert(std::is_same_v<value_type, typename priority_queue_type::value_type>,
                  "PriorityQueue::value_type must be the same as Value");
    static_assert(std::atomic<key_type>::is_always_lock_free, "std::atomic<key_type> must be lock-free");
    std::atomic<key_type> top_key_ = Sentinel::sentinel();
    std::atomic_bool lock_ = false;
    priority_queue_type pq_;

   public:
    explicit PQGuard() = default;

    explicit PQGuard(priority_queue_type pq) : pq_(std::move(pq)) {
    }

    [[nodiscard]] key_type top_key() const noexcept {
        return top_key_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool empty() const noexcept {
        return Sentinel::is_sentinel(top_key());
    }

    bool try_lock() noexcept {
        // Test first to not invalidate the cache line
        return !(lock_.load(std::memory_order_relaxed) || lock_.exchange(true, std::memory_order_acquire));
    }

    void popped() {
        auto key = (pq_.empty() ? Sentinel::sentinel() : KeyOfValue::get(pq_.top()));
        top_key_.store(key, std::memory_order_relaxed);
    }

    void pushed() {
        auto key = KeyOfValue::get(pq_.top());
        if (key != top_key()) {
            top_key_.store(key, std::memory_order_relaxed);
        }
    }

    void unlock() {
        assert(lock_.load());
        lock_.store(false, std::memory_order_release);
    }

    priority_queue_type& get_pq() noexcept {
        return pq_;
    }
};

}  // namespace multiqueue
