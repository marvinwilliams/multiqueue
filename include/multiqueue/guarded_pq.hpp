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
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

#ifndef L1_CACHE_LINESIZE
#error Need to define L1_CACHE_LINESIZE
#endif

namespace multiqueue {

namespace detail {

template <typename Key, bool ImplicitLock /* = false*/>
struct Guard {
    alignas(L1_CACHE_LINESIZE) std::atomic_bool lock;
    alignas(L1_CACHE_LINESIZE) std::atomic<Key> top_key;

    explicit Guard(Key const& key) noexcept : lock(false), top_key(key) {
    }

    Key load_top_key() const noexcept {
        return top_key.load(std::memory_order_relaxed);
    }

    bool is_locked() const noexcept {
        return lock.load(std::memory_order_relaxed);
    }

    bool try_lock() noexcept {
        // Maybe do not to test?
        return !is_locked() && !lock.exchange(true, std::memory_order_acquire);
    }

    void unlock(Key const& current_top) noexcept {
        assert(is_locked());
        if (load_top_key() != current_top) {
            top_key.store(current_top, std::memory_order_release);
        }
        lock.store(false, std::memory_order_release);
    }

    bool try_lock_if_key(Key const& key) noexcept {
        if (!try_lock()) {
            return false;
        }

        Key current_key = top_key.load(std::memory_order_relaxed);
        // One could just check for the key not to be the sentinel to allow inaccuracies
        if (current_key == key) {
            return true;
        } else {
            lock.store(false, std::memory_order_release);
            return false;
        }
    }
};

template <typename Key>
struct Guard<Key, /*ImplicitLock = */ true> {
    static_assert(std::is_integral_v<Key> && std::is_unsigned_v<Key>,
                  "Implicit locking is only available with unsigend integrals");

    alignas(L1_CACHE_LINESIZE) std::atomic<Key> top_key;

    explicit Guard(Key const& key) noexcept : top_key(key) {
        assert(key == to_unlocked(key));
    }

    // LockMask is a 1 in the highest bit position
    static constexpr Key LockMask = ~(~Key(0) >> 1);

    static constexpr Key to_locked(Key key) noexcept {
        return key | LockMask;
    }

    static constexpr Key to_unlocked(Key key) noexcept {
        return key & (~LockMask);
    }

    Key load_top_key() const noexcept {
        return to_unlocked(top_key.load(std::memory_order_relaxed));
    }

    bool is_locked() const noexcept {
        return top_key.load(std::memory_order_relaxed) & LockMask;
    }

    bool try_lock() noexcept {
        Key key = top_key.load(std::memory_order_relaxed);
        // ABA problem is no issue here
        return key == to_unlocked(key) &&
            top_key.compare_exchange_strong(key, to_locked(key), std::memory_order_acquire, std::memory_order_relaxed);
    }

    bool try_lock_if_key(Key key) noexcept {
        assert(key == to_unlocked(key));
        // ABA problem is no issue here
        if (top_key.compare_exchange_strong(key, to_locked(key), std::memory_order_acquire,
                                            std::memory_order_relaxed)) {
            return true;
        } else {
            key = to_unlocked(key);
            return false;
        }
    }

    void unlock(Key current_top) noexcept {
        assert(is_locked());
        assert(current_top == to_unlocked(current_top));
        top_key.store(current_top, std::memory_order_release);
    }

    std::pair<Key, bool> load_top_key_and_lock() const noexcept {
        Key key = top_key.load(std::memory_order_relaxed);
        return {to_unlocked(key), is_locked(key)};
    }
};

}  // namespace detail

template <typename Key, typename T, typename ExtractKey, typename Compare, typename Sentinel, bool ImplicitLock,
          typename PriorityQueue>
class alignas(2 * L1_CACHE_LINESIZE) GuardedPQ {
   public:
    using key_type = Key;
    using value_type = T;
    using priority_queue_type = PriorityQueue;
    using key_compare = Compare;
    using value_compare = typename priority_queue_type::value_compare;
    using reference = typename priority_queue_type::reference;
    using const_reference = typename priority_queue_type::const_reference;
    using size_type = typename priority_queue_type::size_type;

   private:
    detail::Guard<key_type, ImplicitLock> guard_;
    alignas(L1_CACHE_LINESIZE) priority_queue_type pq_;

   public:
    explicit GuardedPQ(key_compare const& comp = key_compare())
        : guard_(get_sentinel()), pq_(typename PriorityQueue::value_compare{comp}) {
    }

    explicit GuardedPQ(size_type initial_capacity, key_compare const& comp = key_compare())
        : guard_(get_sentinel()), pq_(typename PriorityQueue::value_compare{comp}) {
        pq_.reserve(initial_capacity);
    }

    template <typename Alloc>
    explicit GuardedPQ(value_compare const& comp, Alloc const& alloc) : guard_(get_sentinel()), pq_(comp, alloc) {
    }

    template <typename Alloc>
    explicit GuardedPQ(size_type initial_capacity, value_compare const& comp, Alloc const& alloc)
        : guard_(get_sentinel()), pq_(comp, alloc) {
        pq_.reserve(initial_capacity);
    }

    static constexpr key_type get_sentinel() noexcept {
        if constexpr (ImplicitLock) {
            return detail::Guard<key_type, ImplicitLock>::to_unlocked(Sentinel()());
        } else {
            return Sentinel()();
        }
    }

    bool try_lock() noexcept {
        return guard_.try_lock();
    }

    bool try_lock_if_key(key_type const& key) noexcept {
        return guard_.try_lock_if_key(key);
    }

    void unlock() noexcept {
        guard_.unlock(pq_.empty() ? get_sentinel() : ExtractKey()(pq_.top()));
    }

    bool is_locked() const noexcept {
        return guard_.is_locked();
    }

    key_type top_key() const noexcept {
        return guard_.load_top_key();
    }

    [[nodiscard]] bool empty() const noexcept {
        return top_key() == get_sentinel();
    }

    void pop() {
        assert(is_locked());
        assert(!unsafe_empty());
        pq_.pop();
    }

    void extract_top(reference retval) {
        assert(is_locked());
        assert(!empty());
        pq_.extract_top(retval);
    };

    void push(const_reference value) {
        assert(is_locked());
        pq_.push(value);
    }

    void push(value_type&& value) {
        assert(is_locked());
        pq_.push(std::move(value));
    }

    void clear() noexcept {
        assert(is_locked());
        pq_.clear();
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

    static std::string description() {
        std::stringstream ss;
        ss << "Locking: " << (ImplicitLock ? "implicit" : "explicit") << "\n\t";
        ss << priority_queue_type::description();
        return ss.str();
    }
};

}  // namespace multiqueue

namespace std {
template <typename Key, typename T, typename ExtractKey, typename Compare, typename Sentinel, bool ImplicitLock,
          typename PriorityQueue, typename Alloc>
struct uses_allocator<multiqueue::GuardedPQ<Key, T, ExtractKey, Compare, Sentinel, ImplicitLock, PriorityQueue>, Alloc>
    : uses_allocator<typename multiqueue::GuardedPQ<Key, T, ExtractKey, Compare, Sentinel, ImplicitLock,
                                                    PriorityQueue>::priority_queue_type,
                     Alloc>::type {};

}  // namespace std

#endif  //! GUARDED_PQ
