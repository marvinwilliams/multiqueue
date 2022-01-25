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
#include "multiqueue/selection_strategy/perm.hpp"
#include "multiqueue/selection_strategy/random.hpp"
#include "multiqueue/selection_strategy/sticky.hpp"
#include "multiqueue/selection_strategy/swapping.hpp"
#include "multiqueue/sentinel.hpp"

#include <cstddef>

#ifndef MULTIQUEUE_DEFAULT_SELECTION_STRATEGY
#define MULTIQUEUE_DEFAULT_SELECTION_STRATEGY selection_strategy::Sticky
#endif

#ifndef MULTIQUEUE_DEFAULT_INSERTION_BUFFERSIZE
#define MULTIQUEUE_DEFAULT_INSERTION_BUFFERSIZE 8
#endif

#ifndef MULTIQUEUE_DEFAULT_DELETION_BUFFERSIZE
#define MULTIQUEUE_DEFAULT_DELETION_BUFFERSIZE 8
#endif

#ifndef MULTIQUEUE_DEFAULT_HEAP_DEGREE
#define MULTIQUEUE_DEFAULT_HEAP_DEGREE 8
#endif

namespace multiqueue {

struct DefaultConfiguration {
    using SelectionStrategy = MULTIQUEUE_DEFAULT_SELECTION_STRATEGY;
    // Use buffers for the sequential pqs
    static constexpr bool UseBuffers =
#ifdef MULTIQUEUE_DEFAULT_DISABLE_BUFFERING
        false;
#else
        true;
#endif
    // Buffer sizes
    static constexpr std::size_t InsertionBufferSize = MULTIQUEUE_DEFAULT_INSERTION_BUFFERSIZE;
    static constexpr std::size_t DeletionBufferSize = MULTIQUEUE_DEFAULT_DELETION_BUFFERSIZE;
    // Lock the pqs implicitly by marking the highest bit of the top key
    // Thus, this bit is not available to use in keys
    // Only integral and unsigned Key types are allowed
    static constexpr bool ImplicitLock =
#ifdef MULTIQUEUE_DEFAULT_IMPLICIT_LOCKING
        true;
#else
        false;
#endif
    // Degree of the heap
    static constexpr unsigned int HeapDegree = MULTIQUEUE_DEFAULT_HEAP_DEGREE;
};

}  // namespace multiqueue

#endif  //! DEFAULT_CONFIGURATION_HPP_INCLUDED
