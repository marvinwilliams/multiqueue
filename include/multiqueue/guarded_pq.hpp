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
#include <stdexcept>
#include <type_traits>

#ifndef L1_CACHE_LINESIZE
#error Need to define L1_CACHE_LINESIZE
#endif

namespace multiqueue {

namespace detail {

template <typename Key, bool ImplicitLock /* = false*/>
struct LockImpl {
    alignas(L1_CACHE_LINESIZE) std::atomic_bool lock = false;
    LockImpl() noexcept = default;
};

template <typename Key>
struct LockImpl<Key, /*ImplicitLock = */ true> {
    static_assert(std::is_integral_v<Key> && std::is_unsigned_v<Key>, "Key has to be unsigend integral");

    // LockMask is a 1 in the highest bit position
    static constexpr Key LockMask = ~(~Key(0) >> 1);

    static constexpr Key to_locked(Key key) noexcept {
        return key | LockMask;
    }

    static constexpr Key to_unlocked(Key key) noexcept {
        return key & (~LockMask);
    }

    static constexpr bool is_locked(Key key) noexcept {
        return key & LockMask;
    }
};

}  // namespace detail

template <typename Key, typename T, typename Compare, typename Allocator, typename Configuration>
class alignas(2 * L1_CACHE_LINESIZE) GuardedPQ {
   private:
    using pq_type = std::conditional_t<Configuration::UseBuffers, BufferedPQ<Key, T, Compare, Configuration, Allocator>,
                                       Heap<Key, T, Compare, Configuration::HeapDegree, Allocator>>;
    using key_of = detail::key_extractor<Key, T>;

   public:
    using key_type = typename pq_type::key_type;
    using value_type = typename pq_type::value_type;
    using key_compare = typename pq_type::key_compare;
    using value_compare = typename pq_type::value_compare;
    using allocator_type = Allocator;
    using reference = typename pq_type::reference;
    using const_reference = typename pq_type::const_reference;
    using size_type = typename pq_type::size_type;

   private:
    static inline key_type sentinel = Configuration::template sentinel<key_type>::value;

    struct Data : detail::LockImpl<key_type, Configuration::ImplicitLock> {
        alignas(L1_CACHE_LINESIZE) std::atomic<Key> top_key;
        alignas(L1_CACHE_LINESIZE) pq_type pq;
    };

    Data data_;

   public:
    explicit GuardedPQ(key_compare const& comp, allocator_type const& alloc = allocator_type())
        : data_({}, sentinel, {comp, alloc}) {
        if constexpr (Configuration::ImplicitLock) {
            if (!Data::is_unlocked(sentinel)) {
                throw std::invalid_argument("Sentinel must not use the highest bit of the key");
            }
        }
    }

    bool try_lock() noexcept {
        if constexpr (Configuration::ImplicitLock) {
            Key key = data_.top_key.load(std::memory_order_acquire);
            if (Data::is_locked(key)) {
                return false;
            }
            // ABA problem is no issue here
            return data_.top_key.compare_exchange_strong(key, Data::to_locked(key), std::memory_order_release,
                                                         std::memory_order_relaxed);
        } else {
            bool is_locked = data_.lock.load(std::memory_order_acquire);
            if (is_locked) {
                return false;
            }
            return data_.lock.exchange(true, std::memory_order_release) == false;
        }
    }

    bool try_lock_if_key(key_type const& key) noexcept {
        assert(key != sentinel_);
        if constexpr (Configuration::ImplicitLock) {
            assert(!Data::is_locked(key));
            key_type current_key = data_.top_key.load(std::memory_order_acquire);
            // checks if key does not match or lock is set
            if (key != current_key) {
                return false;
            }
            // ABA problem is no issue here
            return data_.top_key.compare_exchange_strong(key, Data::to_locked(key), std::memory_order_release,
                                                         std::memory_order_relaxed);
        } else {
            if (data_.lock.load(std::memory_order_acquire) || data_.lock.exchange(true, std::memory_order_release)) {
                return false;
            }
            if (key_of{}(top()) != key) {
                data_.lock.store(false, std::memory_order_release);
                return false;
            }
            return true;
        }
    }

    void unlock() noexcept {
        assert(is_locked());
        if constexpr (Configuration::ImplicitLock) {
            data_.top_key.store(empty() ? sentinel : key_of{}(top()), std::memory_order_release);
        } else {
            // Also release top_key, since it is read without acquiring the lock
            data_.top_key.store(empty() ? sentinel : key_of{}(top()), std::memory_order_release);
            data_.lock.store(false, std::memory_order_release);
        }
    }

    bool is_locked() const noexcept {
        if constexpr (Configuration::ImplicitLock) {
            return Data::is_locked(data_.top_key.load(std::memory_order_acquire));
        } else {
            return data_.lock.load(std::memory_order_acquire);
        }
    }

    key_type concurrent_top_key() const noexcept {
        return Data::to_unlocked(data_.top_key.load(std::memory_order_acquire));
    }

    [[nodiscard]] bool concurrent_empty() const noexcept {
        return concurrent_top_key() == sentinel;
    }

    [[nodiscard]] bool empty() const noexcept {
        assert(is_locked());
        return data_.pq.empty();
    }

    constexpr size_type size() const noexcept {
        assert(is_locked());
        return data_.pq.size();
    }

    constexpr const_reference top() const {
        assert(is_locked());
        assert(!empty());
        return data_.pq.top();
    }

    void pop() {
        assert(is_locked());
        assert(!empty());
        data_.pq.pop();
    }

    void extract_top(reference retval) {
        assert(is_locked());
        assert(!empty());
        data_.pq.extract_top(retval);
    };

    void push(const_reference value) {
        assert(is_locked());
        assert(!Data::is_locked(key_of{}(value)) && key_of{}(value) != sentinel_);
        data_.pq.push(value);
    }

    void push(value_type&& value) {
        assert(is_locked());
        assert(!Data::is_locked(key_of{}(value)) && key_of{}(value) != sentinel_);
        data_.pq.push(std::move(value));
    }

    // reserve() does not assert that the queue is locked, as reserving is only allowed in sequential phases
    void reserve(size_type cap) {
        data_.pq.reserve(cap);
    }

    constexpr void clear() noexcept {
        assert(guard_.is_locked());
        data_.pq.clear();
    }
};

}  // namespace multiqueue
#endif  //! GUARDED_PQ
