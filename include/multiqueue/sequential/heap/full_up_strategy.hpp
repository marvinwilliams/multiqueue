/**
******************************************************************************
* @file:   full_up_strategy.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 16:35
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef SEQUENTIAL_HEAP_SIFT_STRATEGY_FULL_UP_HPP_INCLUDED
#define SEQUENTIAL_HEAP_SIFT_STRATEGY_FULL_UP_HPP_INCLUDED

#include <cassert>
#include <utility>

namespace multiqueue {
namespace sequential {
namespace sift_strategy {

struct FullUp {
    // Sifts the hole at index `index` up until either the top of the heap is
    // reached or key `key` is not smaller than the parent of the returned
    // hole index.
    template <typename Heap>
    static typename Heap::size_type sift_up_hole(Heap &heap, typename Heap::size_type index,
                                                 typename Heap::key_type const &key) {
        assert(index < heap.data_.size());
        while (index > 0) {
            auto const parent = heap.parent_index(index);
            assert(parent < index);
            heap.data_[index] = std::move(heap.data_[parent]);
            index = parent;
        }
        return sift_down_hole(heap, index, key);
    }

    template <typename Heap>
    static typename Heap::size_type sift_down_hole(Heap &heap, typename Heap::size_type index,
                                                   typename Heap::key_type const &key) {
        assert(heap.data_.size() > 0u && index < heap.data_.size());
        // The node that will be the parent of the next inserted node
        typename Heap::size_type const first_incomplete_parent = heap.parent_index(heap.data_.size());
        assert(first_incomplete_parent < heap.data_.size());
        // This loop exits too early if we descent into a parent node with [1, degree - 1] children
        while (index < first_incomplete_parent) {
            auto const child = heap.min_child_index(index);
            assert(child < heap.data_.size());
            if (!heap.compare(heap.extract_key(heap.data_[child]), key)) {
                return index;
            }
            heap.data_[index] = std::move(heap.data_[child]);
            index = child;
        }
        if (index == first_incomplete_parent) {
            if (typename Heap::size_type const num_children = (heap.data_.size() - 1) % Heap::degree_;
                num_children != 0u) {
                // Loop has exited too early and we need to sift the hole down once
                // more
                auto const child = heap.min_child_index(index, num_children);
                assert(child < heap.data_.size());
                if (heap.compare(heap.extract_key(heap.data_[child]), key)) {
                    heap.data_[index] = std::move(heap.data_[child]);
                    index = child;
                }
            }
        }
        return index;
    }

    // Removes the element at index `index` by sifting the hole down to the appropriate position for the last
    // element and moves it there
    template <typename Heap>
    static typename Heap::size_type remove(Heap &heap, typename Heap::size_type index) {
        return sift_down_hole(heap, index, heap.extract_key(heap.data_.back()));
    }
};

}  // namespace sift_strategy
}  // namespace sequential
}  // namespace multiqueue

#endif  //! SEQUENTIAL_HEAP_SIFT_STRATEGY_FULL_UP_HPP_INCLUDED
