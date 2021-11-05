/**
******************************************************************************
* @file:   configurations.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 18:41
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef CONFIGURATIONS_HPP_INCLUDED
#define CONFIGURATIONS_HPP_INCLUDED

#include "multiqueue/heap.hpp"
#include "multiqueue/ring_buffer.hpp"
#include "multiqueue/selection_strategy/sticky.hpp"

#include <cstddef>
#include <memory>
#include <utility>

namespace multiqueue {

struct DefaultConfiguration {
    // Buffer sizes (number of elements)
    static constexpr std::size_t DeletionBufferSize = 8;
    static constexpr std::size_t InsertionBufferSize = 8;
    template <typename T>
    using selection_strategy = selection_strategy::sticky<T>;
    // Degree of the heap tree
    static constexpr unsigned int HeapDegree = 8;
    using HeapAllocator = std::allocator<char>;
};

}  // namespace multiqueue

#endif  //! CONFIGURATIONS_HPP_INCLUDED
