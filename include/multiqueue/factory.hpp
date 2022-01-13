/**
******************************************************************************
* @file:   factory.hpp
*
* @author: Marvin Williams
* @date:   2022/01/11 18:11
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef FACTORY_HPP_INCLUDED
#define FACTORY_HPP_INCLUDED

#include "multiqueue/buffered_pq.hpp"
#include "multiqueue/default_configuration.hpp"
#include "multiqueue/multiqueue.hpp"

#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

namespace multiqueue {

namespace detail {

template <typename Key, typename T, typename Compare>
struct Value {
    using type = std::pair<Key, T>;

    struct compare {
        Compare comp;

        constexpr bool operator()(type const& lhs, type const& rhs) const noexcept {
            return comp(lhs.first, rhs.first);
        }
    };

    struct extract_key {
        constexpr Key const& operator()(type const& v) const noexcept {
            return v.first;
        }

        constexpr Key&& operator()(type&& v) const noexcept {
            return std::move(v.first);
        }
    };
};

template <typename Key, typename Compare>
struct Value<Key, void, Compare> {
    using type = Key;

    using compare = Compare;

    struct extract_key {
        constexpr type const& operator()(type const& v) const noexcept {
            return v;
        }

        constexpr type&& operator()(type&& v) const noexcept {
            return std::move(v);
        }
    };
};

}  // namespace detail

template <typename Key, typename T = void, typename Compare = std::less<>>
struct MultiqueueFactory {
    using value_type = typename detail::Value<Key, T, Compare>::type;
    using value_compare = typename detail::Value<Key, T, Compare>::compare;
    using extract_key = typename detail::Value<Key, T, Compare>::extract_key;

    using default_priority_queue = Heap<value_type, value_compare>;

    template <typename Config = DefaultConfiguration, typename Sentinel = DefaultSentinel<Key, Compare>,
              typename PriorityQueue = std::conditional_t<
                  Config::UseBuffers,
                  BufferedPQ<Config::InsertionBufferSize, Config::DeletionBufferSize, default_priority_queue>,
                  default_priority_queue>,
              typename Allocator = std::allocator<Key>>
    using multiqueue_type =
        Multiqueue<Key, value_type, extract_key, Compare, Sentinel, typename Config::SelectionStrategy,
                   Config::ImplicitLock, PriorityQueue, Allocator>;
};
}  // namespace multiqueue

#endif  //! FACTORY_HPP_INCLUDED
