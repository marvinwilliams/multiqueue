/**
******************************************************************************
* @file:   heap.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 17:26
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef HEAP_HPP_INCLUDED
#define HEAP_HPP_INCLUDED

#include "multiqueue/value.hpp"

#include <cassert>
#include <cstddef>
#include <memory>       // allocator
#include <type_traits>  // is_constructible, enable_if
#include <utility>      // move, forward, pair
#include <vector>

namespace multiqueue {

template <typename Key, typename T, unsigned int Degree, typename Allocator>
class Heap {
   public:
    using value_type = multiqueue::Value<Key, T>;
    using key_type = typename value_type::key_type;
    using mapped_type = typename value_type::mapped_type;
    using reference = value_type &;
    using const_reference = value_type const &;

    using allocator_type = Allocator;
    using container_type = std::vector<value_type, allocator_type>;
    using iterator = typename container_type::const_iterator;
    using const_iterator = typename container_type::const_iterator;
    using difference_type = typename container_type::difference_type;
    using size_type = std::size_t;

    static_assert(Degree >= 2, "Degree must be at least two");

   private:
    container_type data_;

   private:
    static constexpr std::size_t parent_index(std::size_t index) noexcept {
        return (index - 1) / Degree;
    }

    static constexpr std::size_t first_child_index(std::size_t index) noexcept {
        return index * Degree + 1;
    }

    // Find the index of the smallest `num_children` children of the node at
    // index `index`
    constexpr size_type min_child_index(size_type index, size_type num_children = Degree) const noexcept {
        assert(index < size());
        index = first_child_index(index);
        auto const last = index + num_children;
        assert(last <= size());
        auto result = index++;
        for (; index < last; ++index) {
            if (data_[index].key < data_[result].key) {
                result = index;
            }
        }
        return result;
    }

    size_type sift_up_hole(size_type index, key_type key) {
        assert(index < size());
        size_type parent;
        while (index > 0) {
            parent = parent_index(index);
            if (key >= data_[parent].key) {
                break;
            }
            data_[index] = data_[parent];
            index = parent;
        }
        return index;
    }

    size_type remove_min() {
        assert(size() > 0);
        if (size() == 1) {
            return 0;
        }
        size_type const last_parent = parent_index(size() - 1);
        // This loop exits too early if we descent into the last parent node
        size_type index = 0;
        while (index < last_parent) {
            auto const child = min_child_index(index);
            assert(child < size());
            data_[index] = data_[child];
            index = child;
        }
        if (index == last_parent) {
            // Loop has exited too early and we need to sift the hole down
            // once more
            auto const child = min_child_index(index, size() - first_child_index(last_parent));
            assert(child < size());
            data_[index] = data_[child];
            return child;
        }
        return sift_up_hole(index, data_.back().key);
    }

#ifndef NDEBUG
    bool is_heap() const {
        for (size_type i = 0; i < size(); i++) {
            auto const first_child = first_child_index(i);
            for (std::size_t j = 0; j < Degree; ++j) {
                if (first_child + j >= size()) {
                    return true;
                }
                if (data_[first_child + j].key < data_[i].key) {
                    return false;
                }
            }
        }
        return true;
    }
#endif
   public:
    Heap() = default;

    explicit Heap(allocator_type const &alloc = allocator_type()) noexcept : data_(alloc) {
    }

    inline iterator begin() const noexcept {
        return data_.cbegin();
    }

    inline const_iterator cbegin() const noexcept {
        return data_.cbegin();
    }

    inline iterator end() const noexcept {
        return data_.cend();
    }

    inline const_iterator cend() const noexcept {
        return data_.cend();
    }

    [[nodiscard]] inline bool empty() const noexcept {
        return data_.empty();
    }

    inline size_type size() const noexcept {
        return data_.size();
    }

    inline value_type min() const {
        return data_.front();
    }

    void pop() {
        assert(!data_.empty());
        auto const index = remove_min();
        if (index + 1 < size()) {
            data_[index] = data_.back();
        }
        data_.pop_back();
        assert(is_heap());
    }

    void extract_min(value_type &retval) {
        assert(!data_.empty());
        retval = data_.front();
        pop();
    }

    void insert(value_type value) {
        size_type parent;
        if (empty()) {
            data_.push_back(value);
        }
        parent = parent_index(size());
        if (value.key < data_[parent].key) {
            data_.push_back(data_[parent]);
            auto const index = sift_up_hole(parent, value.key);
            data_[index] = value;
        } else {
            data_.push_back(value);
        }
        assert(is_heap());
    }

    inline void reserve(std::size_t cap) {
        data_.reserve(cap);
    }

    inline void clear() noexcept {
        data_.clear();
    }
};

}  // namespace multiqueue

#endif  //! HEAP_HPP_INCLUDED
