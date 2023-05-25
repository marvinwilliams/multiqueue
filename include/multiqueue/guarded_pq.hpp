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

template <typename PriorityQueue>
class alignas(BuildConfiguration::L1CacheLinesize) GuardedPQ {
   public:
    using value_type = typename PriorityQueue::value_type;
    static_assert(std::is_same_v<value_type, typename PriorityQueue::value_type>,
                  "PriorityQueue must have the same value_type as its ValueTraits");

    using pq_type = PriorityQueue;
    using value_compare = typename pq_type::value_compare;
    using size_type = typename pq_type::size_type;
    using reference = typename pq_type::reference;
    using const_reference = typename pq_type::const_reference;

   private:
    std::atomic<unsigned long> top_key_ = std::numeric_limits<unsigned long>::max();
    std::atomic_bool lock_ = false;
    alignas(BuildConfiguration::L1CacheLinesize) pq_type pq_;

   public:
    explicit GuardedPQ(std::size_t cap, value_compare const& comp = value_compare()) : pq_(cap, comp) {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<pq_type, Alloc>>>
    explicit GuardedPQ(std::size_t cap, value_compare const& comp, Alloc const& alloc) : pq_(cap, comp, alloc) {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<pq_type, Alloc>>>
    explicit GuardedPQ(std::size_t cap, Alloc const& alloc) : pq_(cap, alloc) {
    }

    [[nodiscard]] unsigned long concurrent_top_key() const noexcept {
        return top_key_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool concurrent_empty() const noexcept {
        return concurrent_top_key() == std::numeric_limits<unsigned long>::max();
    }

    bool try_lock() noexcept {
        // Test first but expect success
        return !(lock_.load(std::memory_order_relaxed) || lock_.exchange(true, std::memory_order_acquire));
    }

    void unlock() {
        assert(lock_.load());
        lock_.store(false, std::memory_order_release);
    }

    [[nodiscard]] bool unsafe_empty() const {
        return pq_.empty();
    }

    size_type unsafe_size() const {
        return pq_.size();
    }

    const_reference unsafe_top() const {
        assert(!unsafe_empty());
        return pq_.top();
    }

    void unsafe_pop() {
        assert(!unsafe_empty());
        pq_.pop();
        top_key_.store(pq_.empty() ? std::numeric_limits<unsigned long>::max() : pq_.top().prior(),
                       std::memory_order_relaxed);
    }

    void unsafe_push(const_reference value) {
        pq_.push(value);
        if (pq_.top().prior() != top_key_.load(std::memory_order_relaxed)) {
            top_key_.store(pq_.top().prior(), std::memory_order_relaxed);
        }
    }
};

}  // namespace multiqueue

namespace std {

template <typename PriorityQueue, typename Alloc>
struct uses_allocator<multiqueue::GuardedPQ<PriorityQueue>, Alloc> : uses_allocator<PriorityQueue, Alloc>::type {};

}  // namespace std
