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

#include "multiqueue/selection_strategy/random.hpp"
#include "multiqueue/selection_strategy/sticky.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

namespace multiqueue {

template <typename T>
struct Sentinel {
    static inline const T value = T();
};

namespace detail {

struct BaseConfiguration {
    // Use buffers for the sequential pqs
    static constexpr bool UseBuffers = true;
    // Buffer sizes (log_2)
    static constexpr std::size_t InsertionBufferSize = 3;
    static constexpr std::size_t DeletionBufferSize = 3;
    // Lock the pqs implicitly by marking the highest bit of the top key
    // Thus, this bit is not available to use in keys
    // Only integral and unsigned Key types are allowed
    static constexpr bool ImplicitLock = true;
    template <typename T>
    using sentinel = Sentinel<T>;
    // Degree of the heap
    static constexpr unsigned int HeapDegree = 8;
    // The allocator used for the heaps
    using HeapAllocator = std::allocator<char>;

    std::size_t c = 4;
    std::uint64_t seed = 1;
};

}  // namespace detail

// Default Configuration using the sticky selection strategy
struct RandomSelectionConfiguration : detail::BaseConfiguration {
    using selection_strategy = selection_strategy::random;
};

// Default configuration using the sticky selection strategy
struct StickySelectionConfiguration : detail::BaseConfiguration {
    using selection_strategy = selection_strategy::sticky;
    unsigned int stickiness = 4;
};

}  // namespace multiqueue

#endif  //! DEFAULT_CONFIGURATION_HPP_INCLUDED
