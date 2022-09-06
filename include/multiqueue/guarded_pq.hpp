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
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

#ifndef L1_CACHE_LINESIZE
#error Need to define L1_CACHE_LINESIZE
#endif

#ifndef PAGESIZE
#error Need to define PAGESIZE
#endif

#define GUARDED_PQ_ALIGNMENT PAGESIZE

namespace multiqueue {

template <typename Key, typename KeyOfValue, typename PriorityQueue, typename Sentinel>
class alignas(GUARDED_PQ_ALIGNMENT) GuardedPQ {
   public:
    using key_type = Key;
    using value_type = typename PriorityQueue::value_type;

   private:
    using pq_type = PriorityQueue;

   public:
    using value_compare = typename pq_type::value_compare;
    using size_type = typename pq_type::size_type;
    using reference = typename pq_type::reference;
    using const_reference = typename pq_type::const_reference;

   private:
    std::atomic_bool lock_;
    std::atomic<key_type> top_key_;
    alignas(L1_CACHE_LINESIZE) pq_type pq_;

   public:
    explicit GuardedPQ(value_compare const& comp = value_compare())
        : lock_{false}, top_key_{Sentinel::get()}, pq_(comp) {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<pq_type, Alloc>>>
    explicit GuardedPQ(value_compare const& comp, Alloc const& alloc)
        : lock_{false}, top_key_{Sentinel::get()}, pq_(comp, alloc) {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<pq_type, Alloc>>>
    explicit GuardedPQ(Alloc const& alloc) : lock_{false}, top_key_{Sentinel::get()}, pq_(alloc) {
    }

    key_type concurrent_top_key() const noexcept {
        return top_key_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool concurrent_empty() const noexcept {
        return concurrent_top_key() == Sentinel::get();
    }

    bool try_lock() noexcept {
        // Maybe test, but expect unlocked
        return !lock_.exchange(true, std::memory_order_acquire);
    }

    void unlock() noexcept {
        assert(lock_.load());
        lock_.store(false, std::memory_order_release);
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

    void unsafe_pop() {
        assert(!unsafe_empty());
        pq_.pop();
        top_key_.store(pq_.empty() ? Sentinel::get() : KeyOfValue::get(pq_.top()),
                       std::memory_order_relaxed);
    }

    void unsafe_push(const_reference value) {
        pq_.push(value);
        if (KeyOfValue::get(pq_.top()) == KeyOfValue::get(value)) {
            top_key_.store(KeyOfValue::get(pq_.top()), std::memory_order_relaxed);
        }
    }

    void unsafe_clear() noexcept {
        pq_.clear();
        top_key_.store(Sentinel::get(), std::memory_order_relaxed);
    }
};

}  // namespace multiqueue

namespace std {

template <typename Key, typename KeyOfValue, typename PriorityQueue, typename ComparatorTraits, typename Alloc>
struct uses_allocator<multiqueue::GuardedPQ<Key, KeyOfValue, PriorityQueue, ComparatorTraits>, Alloc>
    : uses_allocator<PriorityQueue, Alloc>::type {};

}  // namespace std

#endif  //! GUARDED_PQ
