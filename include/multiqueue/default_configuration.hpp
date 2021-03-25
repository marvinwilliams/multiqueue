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

#include "multiqueue/sequential/heap/full_down_strategy.hpp"

#include <cstddef>
#include <memory>

namespace multiqueue {

struct DefaultConfiguration {
    // Number of queues per thread
    static constexpr unsigned int C = 4;
    // Stickiness of selected queue
    static constexpr unsigned int K = 1;
    // Use a merging heap (implies using buffers with sizes dependent on the node size)
    static constexpr bool UseMergeHeap = false;
    // Activate/Deactivate deletion and insertion buffer (only with merge heap deactivated)
    static constexpr bool WithDeletionBuffer = true;
    static constexpr bool WithInsertionBuffer = true;
    // Buffer sizes (number of elements)
    static constexpr std::size_t DeletionBufferSize = 16;
    static constexpr std::size_t InsertionBufferSize = 16;
    // Node size used only by the merge heap
    static constexpr std::size_t NodeSize = 128;
    // Make multiqueue numa friendly (induces more overhead)
    static constexpr bool NumaFriendly = false;
    // degree of the heap tree (effect only if merge heap deactivated)
    static constexpr unsigned int HeapDegree = 4;
    // Number of elements to preallocate in each queue
    static constexpr std::size_t ReservePerQueue = 1'000'000;
    using HeapAllocator = std::allocator<int>;
    using SiftStrategy = sequential::sift_strategy::FullDown;
};

}  // namespace multiqueue

#endif  //! DEFAULT_CONFIGURATION_HPP_INCLUDED
