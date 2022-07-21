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

#ifndef PAGESIZE
#define PAGESIZE 4096
#endif

namespace multiqueue {

template <typename ValueTraits, typename SentinelTraits, typename PriorityQueue>
class alignas(PAGESIZE) GuardedPQ {
   public:
    using key_type = typename ValueTraits::key_type;
    using value_type = typename ValueTraits::value_type;
    using pq_type = PriorityQueue;
    using value_compare = typename pq_type::value_compare;
    using reference = typename pq_type::reference;
    using const_reference = typename pq_type::const_reference;
    using size_type = typename pq_type::size_type;

   private:
    std::atomic_bool lock_;
    std::atomic<key_type> top_key_;
    alignas(L1_CACHE_LINESIZE) pq_type pq_;

    bool try_lock() noexcept {
        // Maybe test, but expect unlocked
        return !lock_.exchange(true, std::memory_order_acquire);
    }

    void unlock() noexcept {
        assert(lock_.load());
        lock_.store(false, std::memory_order_release);
    }

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

    key_type concurrent_top_key() const noexcept {
        return top_key_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool concurrent_empty() const noexcept {
        return concurrent_top_key() == SentinelTraits::sentinel();
    }

    [[nodiscard]] bool unsafe_empty() const noexcept {
        return pq_.empty();
    }

    size_type unsafe_size() const noexcept {
        return pq_.size();
    }

    const_reference unsafe_top() const {
        assert(!unsafe_empty());
        return pq_.top();
    }

    bool lock_pop(value_type& retval) {
        if (!try_lock()) {
            return false;
        }
        if (pq_.empty()) {
            unlock();
            return false;
        }
        retval = pq_.top();
        pq_.pop();
        top_key_.store(pq_.empty() ? SentinelTraits::sentinel() : ValueTraits::key_of_value(pq_.top()),
                       std::memory_order_relaxed);
        unlock();
        return true;
    }

    bool lock_push(const_reference value) {
        if (!try_lock()) {
            return false;
        }
        pq_.push(value);
        if (ValueTraits::key_of_value(pq_.top()) == ValueTraits::key_of_value(value)) {
            top_key_.store(ValueTraits::key_of_value(pq_.top()), std::memory_order_relaxed);
        }
        unlock();
        return true;
    }

    value_type unsafe_pop() {
        assert(!unsafe_empty());
        auto retval = pq_.top();
        pq_.pop();
        top_key_.store(pq_.empty() ? SentinelTraits::sentinel() : ValueTraits::key_of_value(pq_.top()),
                       std::memory_order_relaxed);
        return retval;
    }

    void unsafe_push(const_reference value) {
        pq_.push(value);
        if (ValueTraits::key_of_value(pq_.top()) == ValueTraits::key_of_value(value)) {
            top_key_.store(ValueTraits::key_of_value(pq_.top()), std::memory_order_relaxed);
        }
    }

    void unsafe_clear() noexcept {
        pq_.clear();
        top_key_.store(SentinelTraits::sentinel(), std::memory_order_relaxed);
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
