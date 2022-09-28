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
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace multiqueue {

template <typename PriorityQueue, typename ValueTraits, typename SentinelTraits>
class alignas(BuildConfiguration::Pagesize) GuardedPQ {
   public:
    using key_type = typename ValueTraits::key_type;
    using value_type = typename ValueTraits::value_type;
    static_assert(std::is_same_v<value_type, typename PriorityQueue::value_type>,
                  "PriorityQueue must have the same value_type as its ValueTraits");

    using pq_type = PriorityQueue;
    using value_compare = typename pq_type::value_compare;
    using size_type = typename pq_type::size_type;
    using reference = typename pq_type::reference;
    using const_reference = typename pq_type::const_reference;

   private:
    std::atomic_bool lock_ = false;
    std::atomic<key_type> top_key_ = SentinelTraits::sentinel();
    alignas(BuildConfiguration::L1CacheLinesize) pq_type pq_;

   public:
    explicit GuardedPQ(value_compare const& comp = value_compare()) : pq_(comp) {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<pq_type, Alloc>>>
    explicit GuardedPQ(value_compare const& comp, Alloc const& alloc) : pq_(comp, alloc) {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<pq_type, Alloc>>>
    explicit GuardedPQ(Alloc const& alloc) : pq_(alloc) {
    }

    key_type concurrent_top_key() const noexcept {
        return top_key_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool concurrent_empty() const noexcept {
        return concurrent_top_key() == SentinelTraits::sentinel();
    }

    bool try_lock() noexcept {
        // Maybe test, but expect unlocked
        return !lock_.exchange(true, std::memory_order_acquire);
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
        top_key_.store(pq_.empty() ? SentinelTraits::sentinel() : ValueTraits::key_of_value(pq_.top()), std::memory_order_relaxed);
    }

    void unsafe_push(const_reference value) {
        pq_.push(value);
        if (ValueTraits::key_of_value(pq_.top()) == ValueTraits::key_of_value(value)) {
            top_key_.store(ValueTraits::key_of_value(pq_.top()), std::memory_order_relaxed);
        }
    }
};

}  // namespace multiqueue

namespace std {

template <typename PriorityQueue, typename ValueTraits, typename SentinelTraits, typename Alloc>
struct uses_allocator<multiqueue::GuardedPQ<PriorityQueue, ValueTraits, SentinelTraits>, Alloc>
    : uses_allocator<PriorityQueue, Alloc>::type {};

}  // namespace std
