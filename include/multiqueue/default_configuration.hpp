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

#include "multiqueue/selection_strategy/sticky.hpp"

#include <cstddef>
#include <limits>
#include <memory>
#include <utility>

namespace multiqueue {

struct DefaultConfiguration {
    // Use buffers for the sequential pqs
    static constexpr bool UseBuffers = true;
    // Buffer sizes (log_2)
    static constexpr std::size_t InsertionBufferSize = 3;
    static constexpr std::size_t DeletionBufferSize = 3;
    // The selection strategy for inserting and deleting elements
    template <typename T>
    using selection_strategy = selection_strategy::sticky<T>;
    // Lock the pqs implicitly by marking the highest bit of the top key
    // Thus, this bit is not available to use in keys
    static constexpr bool ImplicitLock = true;
    // Degree of the heap
    static constexpr unsigned int HeapDegree = 8;
    // The allocator used for the heaps
    using HeapAllocator = std::allocator<char>;
};

}  // namespace multiqueue

#endif  //! DEFAULT_CONFIGURATION_HPP_INCLUDED
