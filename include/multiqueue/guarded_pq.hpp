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
#ifndef GUARDED_PQ
#define GUARDED_PQ

#include <atomic>
#include <cassert>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

#ifndef L1_CACHE_LINESIZE
#error Need to define L1_CACHE_LINESIZE
#endif

namespace multiqueue {

template <typename ValueTraits, typename SentinelTraits, typename PriorityQueue>
class alignas(2 * L1_CACHE_LINESIZE) GuardedPQ {
   public:
    using key_type = typename ValueTraits::key_type;
    using value_type = typename ValueTraits::value_type;
    using pq_type = PriorityQueue;
    using value_compare = typename pq_type::value_compare;
    using reference = typename pq_type::reference;
    using const_reference = typename pq_type::const_reference;
    using size_type = typename pq_type::size_type;

   private:
    alignas(L1_CACHE_LINESIZE) std::atomic_bool lock_;
    alignas(L1_CACHE_LINESIZE) std::atomic<key_type> top_key_;
    pq_type pq_;

   public:
    explicit GuardedPQ(value_compare const& comp = value_compare())
        : lock_{false}, top_key_{SentinelTraits::sentinel()}, pq_(comp) {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<pq_type, Alloc>>>
    explicit GuardedPQ(value_compare const& comp, Alloc const& alloc)
        : lock_{false}, top_key_{SentinelTraits::sentinel()}, pq_(comp, alloc) {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<pq_type, Alloc>>>
    explicit GuardedPQ(Alloc const& alloc) : lock_{false}, top_key_{SentinelTraits::sentinel()}, pq_(alloc) {
    }

    bool is_locked() noexcept {
        return lock_.load(std::memory_order_relaxed);
    }

    bool try_lock() noexcept {
        // Maybe do not test?
        return !is_locked() && !lock_.exchange(true, std::memory_order_acquire);
    }

    bool try_lock_if_nonempty() noexcept {
        if (!try_lock()) {
            return false;
        }
        if (empty()) {
            lock_.store(false, std::memory_order_release);
            return false;
        }
        return true;
    }

    void unlock() noexcept {
        assert(is_locked());
        if (!pq_.empty()) {
            top_key_.store(ValueTraits::key_of_value(pq_.top()), std::memory_order_relaxed);
        } else {
            top_key_.store(SentinelTraits::sentinel(), std::memory_order_relaxed);
        }
        lock_.store(false, std::memory_order_release);
    }

    [[nodiscard]] bool concurrent_empty() const noexcept {
        return top_key_.load(std::memory_order_relaxed) == SentinelTraits::sentinel();
    }

    value_type pop() {
        assert(!empty());
        return pq_.pop();
    }

    void push(const_reference value) {
        pq_.push(value);
    }

    void push(value_type&& value) {
        pq_.push(std::move(value));
    }

    void clear() noexcept {
        pq_.clear();
    }

    [[nodiscard]] bool empty() const noexcept {
        return pq_.empty();
    }

    size_type size() const noexcept {
        return pq_.size();
    }

    const_reference top() const {
        assert(!empty());
        return pq_.top();
    }
};

}  // namespace multiqueue

namespace std {

template <typename ValueTraits, typename SentinelTraits, typename PriorityQueue, typename Alloc>
struct uses_allocator<multiqueue::GuardedPQ<ValueTraits, SentinelTraits, PriorityQueue>, Alloc>
    : uses_allocator<typename multiqueue::GuardedPQ<ValueTraits, SentinelTraits, PriorityQueue>::pq_type, Alloc>::type {
};

}  // namespace std

#endif  //! GUARDED_PQ
