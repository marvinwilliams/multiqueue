#ifndef FULL_DOWN_STRATEGY_JHFMCDVI
#define FULL_DOWN_STRATEGY_JHFMCDVI

#include <utility>

namespace multiqueue {
namespace local_nonaddressable {

struct full_down_strategy {
    // Sifts the hole at index `index` up until either the top of the heap is
    // reached or key `key` is not smaller than the parent of the returned
    // hole index.
    template <typename Heap>
    static typename Heap::size_type sift_up_hole(Heap &heap, typename Heap::size_type index,
                                                 typename Heap::key_type const &key) {
        assert(index < heap.data_.container.size());
        while (index > 0) {
            auto const parent = heap.parent_index(index);
            assert(parent < index);
            if (!heap.compare(key, heap.extract_key(heap.data_.container[parent]))) {
                break;
            }
            heap.data_.container[index] = std::move(heap.data_.container[parent]);
            index = parent;
        }
        return index;
    }

    // Removes the element at index `index` by sifting the hole down to the appropriate position for the last
    // element and moves it there
    template <typename Heap>
    static typename Heap::size_type remove(Heap &heap, typename Heap::size_type index) {
        assert(heap.data_.container.size() > 0u && index < heap.data_.container.size());
        // The node that will be the parent of the next inserted node
        typename Heap::size_type const first_incomplete_parent = heap.parent_index(heap.data_.container.size());
        assert(first_incomplete_parent < heap.data_.container.size());
        // This loop exits too early if we descent into a parent node with [1, degree - 1] children
        while (index < first_incomplete_parent) {
            auto const child = heap.min_child_index(index);
            assert(child < heap.data_.container.size());
            heap.data_.container[index] = std::move(heap.data_.container[child]);
            index = child;
        }
        if (index == first_incomplete_parent) {
            if (typename Heap::size_type const num_children = (heap.data_.container.size() - 1) % Heap::degree;
                num_children != 0u) {
                // Loop has exited too early and we need to sift the hole down once
                // more
                auto const child = heap.min_child_index(index, num_children);
                assert(child < heap.data_.container.size());
                heap.data_.container[index] = std::move(heap.data_.container[child]);
                index = child;
            }
        }
        if (index + 1 < heap.data_.container.size()) {
            index = sift_up_hole(heap, index, heap.extract_key(heap.data_.container.back()));
        }
        return index;
    }
};

}  // namespace local_nonaddressable
}  // namespace multiqueue
#endif
