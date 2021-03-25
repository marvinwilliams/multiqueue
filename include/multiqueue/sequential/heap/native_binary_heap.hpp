/******************************************************************************
 * File:             native_binary_heap.hpp
 *
 * Author:           Marvin Williams
 * Created:          12/10/20
 * Description:      This class implements the interface for a local
 *                   nonadressable priority queue as an binary heap.
 *****************************************************************************/

#ifndef NATIVE_BINARY_HEAP_HPP_UDBXUQC4
#define NATIVE_BINARY_HEAP_HPP_UDBXUQC4

#include <cassert>
#include <cstddef>      // size_t
#include <functional>   // std::less
#include <type_traits>  // std::is_same
#include <utility>      // std::swap
#include <vector>

namespace multiqueue {

template <typename T, typename Comparator = std::less<T>, typename Container = std::vector<T>>
class NativeBinaryHeap {
    static_assert(std::is_same_v<T, typename Container::value_type>,
                  "T must be the same as the underlying container value type");

   public:
    using value_type = T;
    using reference = typename Container::reference;
    using const_reference = typename Container::const_reference;
    using iterator = typename Container::iterator;
    using const_iterator = typename Container::const_iterator;
    using difference_type = typename Container::difference_type;
    using size_type = typename Container::size_type;

   private:
    Container data_;
    Comparator comp_;

    // Inserts `value` into the correct position by sifting up the hole
    // accordingly.
    void constexpr sift_up(size_t pos, value_type value) noexcept {
        assert(pos < data_.size());
        while (pos > 0) {
            auto const parent = (pos - 1) / 2;
            if (!comp_(value, data_[parent])) {
                break;
            }
            data_[pos] = std::move(data_[parent]);
            pos = parent;
        }
        data_[pos] = std::move(value);
    }

    // Sift the hole at `pos` down to its correct position. Afterwards, the hole
    // is filled with the last element and the heap property is restored.  It is
    // assumed that the subtrees rooted at the children of `pos` fulfill the
    // heap property.
    void constexpr sift_down(size_t pos) noexcept {
        assert(pos < data_.size());

        auto const len = data_.size();
        // The first node that has less than `degree` children
        size_t const first_incomplete_node = (len - 1) / 2;
        // This loop might exit too early if the tree size is even
        while (pos < first_incomplete_node) {
            auto const min_child = pos * 2 + (comp_(data_[pos * 2 + 1], data_[pos * 2 + 2]) ? 1 : 2);
            data_[pos] = std::move(data_[min_child]);
            pos = min_child;
        }
        // If either the hole is at an incomplete node (i.e. one child) or the hole
        // is the left child of the last complete node, we can just move the last
        // element instead of sifting up.
        if (((len & 1) == 0 && pos == first_incomplete_node) || ((len & 1) != 0 && pos + 2 == len)) {
            data_[pos] = std::move(data_.back());
        } else if (pos + 1 < len) {
            sift_up(pos, std::move(data_.back()));
        }
    }

   public:
    // The special member functions are defaulted for now
    NativeBinaryHeap() = default;

    explicit NativeBinaryHeap(const NativeBinaryHeap& other) = default;
    explicit NativeBinaryHeap(NativeBinaryHeap&& other) = default;
    NativeBinaryHeap& operator=(const NativeBinaryHeap& other) = default;
    NativeBinaryHeap& operator=(NativeBinaryHeap&& other) = default;

    constexpr void swap(NativeBinaryHeap& other) noexcept {
        using std::swap;
        swap(data_, other.data_);
        swap(comp_, other.comp_);
    }

    constexpr iterator begin() noexcept {
        return data_.begin();
    }

    constexpr iterator end() noexcept {
        return data_.end();
    }

    constexpr const_iterator cbegin() noexcept {
        return data_.cbegin();
    }

    constexpr const_iterator cend() noexcept {
        return data_.cend();
    }

    constexpr size_t size() const noexcept {
        return data_.size();
    }

    constexpr size_t max_size() const noexcept {
        return data_.max_size();
    }

    constexpr bool empty() const noexcept {
        return data_.empty();
    }

    constexpr const_reference top() const {
        return data_.front();
    }

    constexpr void reserve() {
        data_.reserve();
    }

    void pop() {
        assert(!data_.empty());
        sift_down(0);
        data_.pop_back();
    }

    void push(const_reference value) {
        if (!data_.empty()) {
            if (auto parent = (data_.size() - 1) / 2; comp_(value, data_[parent])) {
                data_.push_back(data_[parent]);
                sift_up(parent, value);
                return;
            }
        }
        data_.push_back(value);
    }

    void push(value_type&& value) {
        if (!data_.empty()) {
            if (auto parent = (data_.size() - 1) / 2; comp_(value, data_[parent])) {
                data_.push_back(std::move(data_[parent]));
                sift_up(parent, std::move(value));
                return;
            }
        }
        data_.push_back(std::move(value));
    }
};

template <typename T, typename Comparator, typename Container>
inline void swap(NativeBinaryHeap<T, Comparator, Container>& lhs,
                 NativeBinaryHeap<T, Comparator, Container>& rhs) noexcept(noexcept(lhs.swap(rhs))) {
    lhs.swap(rhs);
}

}  // namespace multiqueue

#endif /* end of include guard: NATIVE_BINARY_HEAP_HPP_UDBXUQC4 */
