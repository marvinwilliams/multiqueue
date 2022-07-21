/**
******************************************************************************
* @file:   multiqueue_impl.hpp
*
* @author: Marvin Williams
* @date:   2021/07/20 17:19
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef MULTIQUEUE_IMPL_HPP_INCLUDED
#define MULTIQUEUE_IMPL_HPP_INCLUDED

#include "multiqueue/stick_policies.hpp"

#include "multiqueue/external/fastrange.h"
#include "multiqueue/external/xoroshiro256starstar.hpp"

#include <cstddef>
#include <mutex>
#include <utility>

namespace multiqueue {

template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits>
struct MultiQueueImplBase {
    static_assert(std::is_same_v<Key, typename ValueTraits::key_type> &&
                      std::is_same_v<T, typename ValueTraits::mapped_type>,
                  "Key and T must be the same in ValueTraits");
    static_assert(std::is_same_v<Key, typename SentinelTraits::type>, "Key must be the same as type in SentinelTraits");

    using key_type = typename ValueTraits::key_type;
    using mapped_type = typename ValueTraits::mapped_type;
    using value_type = typename ValueTraits::value_type;
    using key_compare = KeyCompare;
    class value_compare {
        friend class MultiQueueImplBase;

       protected:
        [[no_unique_address]] key_compare comp;

        explicit value_compare(key_compare const &c) : comp{c} {
        }

       public:
        constexpr bool operator()(value_type const &lhs, value_type const &rhs) const noexcept {
            return comp(ValueTraits::key_of_value(lhs), ValueTraits::key_of_value(rhs));
        }
    };
    using reference = value_type &;
    using const_reference = value_type const &;
    using size_type = std::size_t;
    using inner_pq_type = PriorityQueue<typename ValueTraits::value_type, value_compare>;
    using pq_type = GuardedPQ<ValueTraits, SentinelTraits, inner_pq_type>;

    struct Config {
        std::uint64_t seed = 1;
        std::size_t c = 4;
    };

    pq_type *pq_list;
    size_type num_pqs;
    xoroshiro256starstar rng;
    [[no_unique_address]] key_compare comp;

    value_compare value_comp() const {
        return value_compare{comp};
    }

    static constexpr key_type sentinel() noexcept {
        return SentinelTraits::sentinel();
    }

    bool compare(key_type const &lhs, key_type const &rhs) noexcept {
        if constexpr (SentinelTraits::is_implicit) {
            return comp(lhs, rhs);
        } else {
            if (rhs == SentinelTraits::sentinel()) {
                return false;
            }
            if (lhs == SentinelTraits::sentinel()) {
                return true;
            }
            return comp(lhs, rhs);
        }
    }
};

template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits, StickPolicy policy>
struct MultiQueueImpl;

template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits>
struct MultiQueueImpl<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, StickPolicy::None>
    : public MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits> {
    using base_type = MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits>;
    using Config = typename base_type::Config;

    class Handle {
        friend MultiQueueImpl;

        MultiQueueImpl &impl_;
        xoroshiro256starstar rng_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit Handle(MultiQueueImpl &impl, std::uint64_t seed) noexcept : impl_{impl}, rng_{seed}  {
        }

       public:
        bool try_pop(typename base_type::reference retval) noexcept {
            typename base_type::size_type index[2] = {fastrange64(rng_(), impl_.num_pqs),
                                                      fastrange64(rng_(), impl_.num_pqs)};
            typename base_type::key_type key[2] = {impl_.pq_list[index[0]].concurrent_top_key(),
                                                   impl_.pq_list[index[1]].concurrent_top_key()};
            do {
                bool prefer_second = impl_.compare(key[0], key[1]);
                if (key[prefer_second] == base_type::sentinel()) {
                    return false;
                }
                if (impl_.pq_list[index[prefer_second]].lock_pop(retval)) {
                    return true;
                }
                index[!prefer_second] = fastrange64(rng_(), impl_.num_pqs);
                key[!prefer_second] = impl_.pq_list[index[!prefer_second]].concurrent_top_key();
            } while (true);
        }

        void push(typename base_type::const_reference value) noexcept {
            typename base_type::size_type index;
            do {
                index = fastrange64(rng_(), impl_.num_pqs);
            } while (!impl_.pq_list[index].lock_push(value));
        }

        [[nodiscard]] bool is_empty(typename base_type::size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    MultiQueueImpl(std::size_t /* num_pqs */, Config const &) noexcept {
    }

    Handle get_handle() noexcept {
        static std::mutex m;
        std::scoped_lock l{m};
        return Handle{*this, base_type::rng()};
    }
};

}  // namespace multiqueue
#endif
