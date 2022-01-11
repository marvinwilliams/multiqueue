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

template <typename Key, typename T = void, typename Compare = std::less<>, typename Config = DefaultConfiguration,
          typename Sentinel = DefaultSentinel<Key, Compare>, typename Allocator = std::allocator<Key>>
struct MultiqueueFactory {
    using Value = detail::Value<Key, T, Compare>;
    using InnerPQ = Heap<typename Value::type, typename Value::compare, Config::HeapDegree,
                         typename Config::template HeapContainer<Value>>;
    using PriorityQueue =
        std::conditional_t<Config::UseBuffers,
                           BufferedPQ<InnerPQ, Config::InsertionBufferSize, Config::DeletionBufferSize>, InnerPQ>;
    using type = Multiqueue<Key, typename Value::type, typename Value::extract_key, Compare, Sentinel,
                            typename Config::selection_strategy_t, Config::ImplicitLock, PriorityQueue, Allocator>;
};

}  // namespace multiqueue

#endif  //! FACTORY_HPP_INCLUDED
