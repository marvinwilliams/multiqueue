/**
******************************************************************************
* @file:   full_down_strategy.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 16:34
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef SEQUENTIAL_HEAP_SIFT_STRATEGY_FULL_DOWN_HPP_INCLUDED
#define SEQUENTIAL_HEAP_SIFT_STRATEGY_FULL_DOWN_HPP_INCLUDED

#include <cassert>
#include <utility>

namespace multiqueue {
namespace sequential {
namespace sift_strategy {

struct FullDown {
    // Sifts the hole at index `index` up until either the top of the heap is
    // reached or key `key` is not smaller than the parent of the returned
    // hole index.
    template <typename Heap>
    static typename Heap::size_type sift_up_hole(Heap &heap, typename Heap::size_type index,
                                                 typename Heap::key_type const &key) {
        assert(index < heap.size());
        typename Heap::size_type parent;
        while (index > 0 &&
               (parent = heap.parent_index(index), heap.compare(key, heap.extract_key(heap.data_[parent])))) {
            heap.data_[index] = std::move(heap.data_[parent]);
            index = parent;
        }
        return index;
    }

    // Removes the element at index `index` by sifting the hole down to the appropriate position for the last
    // element and moves it there
    template <typename Heap>
    static typename Heap::size_type remove(Heap &heap, typename Heap::size_type index) {
        assert(heap.size() > 0 && index < heap.size());
        if (heap.size() == 1) {
          return 0;
        }
        // The node that will be the parent of the next inserted node
        typename Heap::size_type const last_parent = heap.parent_index(heap.size() - 1);
        // This loop exits too early if we descent into the last parent node with [1, degree] children
        while (index < last_parent) {
            auto const child = heap.min_child_index(index);
            assert(child < heap.size());
            heap.data_[index] = std::move(heap.data_[child]);
            index = child;
        }
        if (index == last_parent) {
            // Loop has exited too early and we need to sift the hole down
            // once more
            auto const child = heap.min_child_index(index, heap.size() - heap.first_child_index(last_parent));
            assert(child < heap.size());
            heap.data_[index] = std::move(heap.data_[child]);
            return child;
        }
        return sift_up_hole(heap, index, heap.extract_key(heap.data_.back()));
    }
};

}  // namespace sift_strategy
}  // namespace sequential
}  // namespace multiqueue

#endif  //! SEQUENTIAL_HEAP_SIFT_STRATEGY_FULL_DOWN_HPP_INCLUDED
