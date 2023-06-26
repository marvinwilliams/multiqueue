/**
******************************************************************************
* @file:   guarded_pq.hpp
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
class alignas(build_config::L1CacheLinesize) GuardedPQ {
   public:
    using key_type = Key;
    using value_type = Value;
    using priority_queue_type = PriorityQueue;
    using const_reference = Value const&;
    using size_type = typename PriorityQueue::size_type;

   private:
    std::atomic<key_type> top_key_ = Sentinel::get();
    std::atomic_bool lock_ = false;
    alignas(build_config::L1CacheLinesize) priority_queue_type pq_;

   public:
    explicit GuardedPQ(priority_queue_type const& pq = priority_queue_type{}) : pq_(pq) {
    }

    explicit GuardedPQ(priority_queue_type&& pq = priority_queue_type{}) : pq_(std::move(pq)) {
    }

    [[nodiscard]] key_type concurrent_top_key() const noexcept {
        return top_key_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool concurrent_empty() const noexcept {
        return concurrent_top_key() == Sentinel::get();
    }

    bool try_lock() noexcept {
        // Test first but expect success
        return !(lock_.load(std::memory_order_relaxed) || lock_.exchange(true, std::memory_order_acquire));
    }

    void unlock() {
        assert(lock_.load());
        lock_.store(false, std::memory_order_release);
    }

    [[nodiscard]] size_type size() const noexcept {
        return pq_.size();
    }

    [[nodiscard]] bool unsafe_empty() const noexcept {
        return pq_.empty();
    }

    const_reference unsafe_top() const {
        assert(!unsafe_empty());
        return pq_.top();
    }

    void unsafe_pop() {
        assert(!unsafe_empty());
        pq_.pop();
        top_key_.store(pq_.empty() ? Sentinel::get() : KeyOfValue::get(pq_.top()), std::memory_order_relaxed);
    }

    void unsafe_push(const_reference value) {
        pq_.push(value);
        if (KeyOfValue::get(pq_.top()) != top_key_.load(std::memory_order_relaxed)) {
            top_key_.store(KeyOfValue::get(pq_.top()), std::memory_order_relaxed);
        }
    }

    void reserve(size_type new_cap) {
        pq_.reserve(new_cap);
    }
};

}  // namespace multiqueue
