/**
******************************************************************************
* @file:   default_configuration.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 18:41
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef DEFAULT_CONFIGURATION_HPP_INCLUDED
#define DEFAULT_CONFIGURATION_HPP_INCLUDED

#include "multiqueue/buffered_pq.hpp"
#include "multiqueue/multiqueue.hpp"
#include "multiqueue/selection_strategy/random.hpp"
#include "multiqueue/selection_strategy/sticky.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

namespace multiqueue {

struct DefaultConfiguration {
    using selection_strategy_t = selection_strategy::sticky;
    // Use buffers for the sequential pqs
    static constexpr bool UseBuffers = true;
    // Buffer sizes
    static constexpr std::size_t InsertionBufferSize = 8;
    static constexpr std::size_t DeletionBufferSize = 8;
    // Lock the pqs implicitly by marking the highest bit of the top key
    // Thus, this bit is not available to use in keys
    // Only integral and unsigned Key types are allowed
    static constexpr bool ImplicitLock = false;
    // Degree of the heap
    static constexpr unsigned int HeapDegree = 8;
    template <typename T>
    using HeapContainer = std::vector<T>;
};

template <typename Key, typename T = void, typename Compare = std::less<>, typename Config = DefaultConfiguration,
          typename Allocator = std::allocator<Key>>
struct MultiqueueFactory {
    template <typename Value, typename ValueComp>
    using inner_pq_type = Heap<Value, ValueComp, Config::HeapDegree, typename Config::template HeapContainer<Value>>;
    template <typename Value, typename ValueComp>
    using pq_type = std::conditional_t<
        Config::UseBuffers,
        BufferedPQ<inner_pq_type<Value, ValueComp>, Config::InsertionBufferSize, Config::DeletionBufferSize>,
        inner_pq_type<Value, ValueComp>>;
    using type =
        Multiqueue<Key, T, Compare, pq_type, typename Config::selection_strategy_t, Config::ImplicitLock, Allocator>;
};

}  // namespace multiqueue

#endif  //! DEFAULT_CONFIGURATION_HPP_INCLUDED
