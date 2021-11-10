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

#include "multiqueue/buffered_pq.hpp"
#include "multiqueue/heap.hpp"

#include <atomic>
#include <type_traits>

namespace multiqueue {

namespace detail {

template <typename Key, bool ImplicitLock /* = false*/>
struct GuardImpl {
    alignas(L1_CACHE_LINESIZE) std::atomic_bool lock;
    alignas(L1_CACHE_LINESIZE) std::atomic<Key> top_key;

    bool try_lock() noexcept {
        bool is_locked = lock.load(std::memory_order_relaxed);
        if (is_locked) {
            return false;
        }
        return lock.exchange(true, std::memory_order_acquire) == false;
    }

    void unlock() noexcept {
        lock.store(false, std::memory_order_release);
    }

    bool is_locked() const noexcept {
        return lock.load(std::memory_order_relaxed);
    }
};

template <typename Key>
struct GuardImpl<Key, /*ImplicitLock = */ true> {
    static_assert(std::is_integral_v<Key> && std::is_unsigned_v<Key>, "Key has to be unsigend integral");

    // LockMask is a 1 in the highest bit position
    static constexpr Key LockMask = ~(~Key(0) >> 1);

    alignas(L1_CACHE_LINESIZE) std::atomic<Key> top_key;

    bool try_lock() noexcept {
        Key key = top_key.load(std::memory_order_relaxed);
        if (key & LockMask) {
            return false;
        }
        // ABA problem is no issue here
        return top_key.compare_exchange_strong(key, key | LockMask, std::memory_order_acquire,
                                               std::memory_order_relaxed);
    }

    void unlock() noexcept {
        Key key = top_key.load(std::memory_order_relaxed);
        assert(key & LockMask);
        [[maybe_unused]] bool result =
            top_key.compare_exchange_strong(key, key & (~LockMask), std::memory_order_release);
        assert(result);
    }

    bool is_locked() const noexcept {
        return top_key.load(std::memory_order_relaxed) & LockMask;
    }
};

}  // namespace detail

template <typename Key, typename T, typename Compare, typename Allocator, typename Configuration>
class GuardedPQ {
   private:
    using pq_type = std::conditional_t<Configuration::UseBuffers, BufferedPQ<Key, T, Compare, Configuration, Allocator>,
                                       Heap<Key, T, Compare, Configuration::HeapDegree, Allocator>>;
    using guard_type = detail::GuardImpl<Key, Configuration::ImplicitLock>;

   public:
    using key_type = typename pq_type::key_type;
    using value_type = typename pq_type::value_type;
    using allocator_type = Allocator;
    using reference = typename pq_type::reference;
    using const_reference = typename pq_type::const_reference;
    using size_type = typename pq_type::size_type;

   private:
    alignas(L1_CACHE_LINESIZE) guard_type guard_;
    alignas(L1_CACHE_LINESIZE) pq_type pq_;
};
}  // namespace multiqueue
#endif  //! GUARDED_PQ
