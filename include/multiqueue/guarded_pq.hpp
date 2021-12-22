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

#include "multiqueue/addressable.hpp"

#include <x86intrin.h>
#include <atomic>
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

template <typename T, bool ImplicitLock /* = false*/>
struct LockImpl {
    static inline T const max_key = std::numeric_limits<T>::max();

    alignas(L1_CACHE_LINESIZE) std::atomic_bool lock = false;
};

template <typename T>
struct LockImpl<T, /*ImplicitLock = */ true> {
    static_assert(std::is_integral_v<T> && std::is_unsigned_v<T>,
                  "Implicit locking is only available with unsigend integrals");

    static inline T const max_key = (std::numeric_limits<T>::max() >> 1);

    // LockMask is a 1 in the highest bit position
    static constexpr T LockMask = ~(~T(0) >> 1);

    static constexpr T to_locked(T key) noexcept {
        return key | LockMask;
    }

    static constexpr T to_unlocked(T key) noexcept {
        return key & (~LockMask);
    }

    static constexpr bool is_locked(T key) noexcept {
        return key & LockMask;
    }
};

}  // namespace detail

template <typename Key, typename T, typename Compare, template <typename, typename> typename PriorityQueue,
          bool ImplicitLock>
class alignas(2 * L1_CACHE_LINESIZE) GuardedPQ {
   public:
    using key_type = Key;
    using value_type = typename std::conditional_t<std::is_same_v<T, void>, key_type, std::pair<key_type, T>>;
    using key_compare = Compare;

    struct value_compare {
        key_compare comp;

        constexpr bool operator()(value_type const& lhs, value_type const& rhs) const noexcept {
            if constexpr (std::is_same_v<T, void>) {
                return comp(lhs, rhs);
            } else {
                return comp(lhs.first, rhs.first);
            }
        }
    };

   private:
    using pq_type = PriorityQueue<value_type, value_compare>;

   public:
    using reference = typename pq_type::reference;
    using const_reference = typename pq_type::const_reference;
    using size_type = typename pq_type::size_type;

    struct ElementInfo {
        std::atomic<GuardedPQ*> pq = nullptr;
        Location location;
        std::size_t index;
    };

   private:
    struct Data : detail::LockImpl<key_type, ImplicitLock> {
        alignas(L1_CACHE_LINESIZE) std::atomic<key_type> top_key;
        alignas(L1_CACHE_LINESIZE) pq_type pq;
        ElementInfo* element_info;
        key_type const sentinel;

        explicit Data(ElementInfo* info, key_type const& s, key_compare const& comp)
            : top_key(s), pq(value_compare{comp}), element_info(info), sentinel(s) {
        }

        template <typename Alloc>
        explicit Data(ElementInfo* info, key_type const& s, key_compare const& comp, Alloc const& alloc)
            : top_key(s), pq(value_compare{comp}, alloc), element_info(info), sentinel(s) {
        }
    };

    Data data_;

   public:
    static inline key_type const max_key = Data::max_key;

    explicit GuardedPQ(ElementInfo* element_info, key_type const& sentinel, key_compare const& comp = key_compare())
        : data_(element_info, sentinel, comp) {
        if constexpr (ImplicitLock) {
            assert(!Data::is_locked(sentinel));
        }
    }

    template <typename Alloc>
    explicit GuardedPQ(ElementInfo* element_info, key_type const& sentinel, value_compare const& comp, Alloc const& alloc)
        : data_(element_info, sentinel, comp, alloc) {
        if constexpr (ImplicitLock) {
            assert(!Data::is_locked(sentinel));
        }
    }

    bool try_lock() noexcept {
        if constexpr (ImplicitLock) {
            key_type key = data_.top_key.load(std::memory_order_relaxed);
            // ABA problem is no issue here
            return !Data::is_locked(key) &&
                data_.top_key.compare_exchange_strong(key, Data::to_locked(key), std::memory_order_acquire,
                                                      std::memory_order_relaxed);
        } else {
            // Maybe do not to test?
            return !(data_.lock.load(std::memory_order_relaxed) ||
                     data_.lock.exchange(true, std::memory_order_acquire));
        }
    }

    bool try_lock_assume_key(key_type const& key) noexcept {
        if constexpr (ImplicitLock) {
            assert(!Data::is_locked(key));
            // ABA problem is no issue here
            Key key_cpy = key;
            return data_.top_key.compare_exchange_strong(key_cpy, Data::to_locked(key), std::memory_order_acquire,
                                                         std::memory_order_relaxed);
        } else {
            if (data_.lock.load(std::memory_order_relaxed) || data_.lock.exchange(true, std::memory_order_acquire)) {
                return false;
            }
            key_type current_key = data_.top_key.load(std::memory_order_relaxed);
            // Use this to allow inaccuracies but higher success chance
            /* if (current_key != data_.sentinel) { */
            if (current_key != key) {
                data_.lock.store(false, std::memory_order_release);
                return false;
            }
            return true;
        }
    }

    void unlock() noexcept {
        assert(is_locked());
        if constexpr (ImplicitLock) {
            if constexpr (std::is_same_v<T, void>) {
                data_.top_key.store(unsafe_empty() ? data_.sentinel : unsafe_top(), std::memory_order_release);
            } else {
                data_.top_key.store(unsafe_empty() ? data_.sentinel : unsafe_top().first, std::memory_order_release);
            }
        } else {
            if constexpr (std::is_same_v<T, void>) {
                data_.top_key.store(unsafe_empty() ? data_.sentinel : unsafe_top(), std::memory_order_relaxed);
            } else {
                data_.top_key.store(unsafe_empty() ? data_.sentinel : unsafe_top().first, std::memory_order_relaxed);
            }
            data_.lock.store(false, std::memory_order_release);
        }
    }

    bool is_locked() const noexcept {
        if constexpr (ImplicitLock) {
            return Data::is_locked(data_.top_key.load(std::memory_order_relaxed));
        } else {
            return data_.lock.load(std::memory_order_relaxed);
        }
    }

    key_type top_key() const noexcept {
        if constexpr (ImplicitLock) {
            return Data::to_unlocked(data_.top_key.load(std::memory_order_relaxed));
        } else {
            return data_.top_key.load(std::memory_order_relaxed);
        }
    }

    [[nodiscard]] bool empty() const noexcept {
        return top_key() == data_.sentinel;
    }

    [[nodiscard]] bool unsafe_empty() const noexcept {
        return data_.pq.empty();
    }

    constexpr size_type unsafe_size() const noexcept {
        return data_.pq.size();
    }

    constexpr const_reference unsafe_top() const {
        assert(!unsafe_empty());
        return data_.pq.top();
    }

    void pop() {
        assert(is_locked());
        assert(!empty());
        data_.pq.pop(data_.element_info);
    }

    void extract_top(reference retval) {
        assert(is_locked());
        assert(!empty());
        data_.pq.extract_top(retval, data_.element_info);
    };

    void push(const_reference value) {
        assert(is_locked());
        data_.pq.push(value, data_.element_info);
    }

    void push(value_type&& value) {
        assert(is_locked());
        data_.pq.push(std::move(value), data_.element_info);
    }

    bool try_update(const_reference new_val) {
        while (!try_lock()) {
            _mm_pause();
        }
        if (data_.element_info[static_cast<std::size_t>(new_val.second)].pq.load(std::memory_order_relaxed) == nullptr) {
            // Element already deleted
            unlock();
            return false;
        }
        data_.pq.update(new_val, data_.element_info);
        unlock();
        return true;
    }

    void unsafe_reserve(size_type cap) {
        data_.pq.reserve(cap);
    }

    constexpr void unsafe_clear() noexcept {
        data_.pq.clear();
    }

    static std::string description() {
        std::stringstream ss;
        ss << "Locking: " << (ImplicitLock ? "implicit" : "explicit") << "\n\t";
        ss << pq_type::description();
        return ss.str();
    }
};

}  // namespace multiqueue

namespace std {

template <typename Key, typename T, typename Compare, template <typename, typename> typename PriorityQueue,
          bool ImplicitLock, typename Alloc>
struct uses_allocator<multiqueue::GuardedPQ<Key, T, Compare, PriorityQueue, ImplicitLock>, Alloc>
    : uses_allocator<typename multiqueue::GuardedPQ<Key, T, Compare, PriorityQueue, ImplicitLock>::pq_type,
                     Alloc>::type {};

}  // namespace std

#endif  //! GUARDED_PQ
