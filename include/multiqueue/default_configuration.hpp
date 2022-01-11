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

#include "multiqueue/heap.hpp"
#include "multiqueue/selection_strategy/sticky.hpp"
#include "multiqueue/sentinel.hpp"

#include <cstddef>

namespace multiqueue {

#ifndef MULTIQUEUE_DEFAULT_INSERTION_BUFFERSIZE
#define MULTIQUEUE_DEFAULT_INSERTION_BUFFERSIZE 8
#endif

#ifndef MULTIQUEUE_DEFAULT_DELETION_BUFFERSIZE
#define MULTIQUEUE_DEFAULT_DELETION_BUFFERSIZE 8
#endif

#ifndef MULTIQUEUE_DEFAULT_HEAP_DEGREE
#define MULTIQUEUE_DEFAULT_HEAP_DEGREE 8
#endif

struct DefaultConfiguration {
    using selection_strategy_t = selection_strategy::sticky;
    // Use buffers for the sequential pqs
    static constexpr bool UseBuffers = true;
    // Buffer sizes
    static constexpr std::size_t InsertionBufferSize = MULTIQUEUE_DEFAULT_INSERTION_BUFFERSIZE;
    static constexpr std::size_t DeletionBufferSize = MULTIQUEUE_DEFAULT_DELETION_BUFFERSIZE;
    // Lock the pqs implicitly by marking the highest bit of the top key
    // Thus, this bit is not available to use in keys
    // Only integral and unsigned Key types are allowed
    static constexpr bool ImplicitLock = false;
    // Degree of the heap
    static constexpr unsigned int HeapDegree = MULTIQUEUE_DEFAULT_HEAP_DEGREE;

    template <typename T, typename Compare>
    using PriorityQueue = Heap<T, Compare>;
};

}  // namespace multiqueue

#endif  //! DEFAULT_CONFIGURATION_HPP_INCLUDED
