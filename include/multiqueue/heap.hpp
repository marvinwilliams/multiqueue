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

template <typename Key, typename T, unsigned int Degree, typename Compare, typename Allocator>
class Heap {
   public:
    using key_type = Key;
    using value_type = detail::value_type<Key, T>;
    using key_compare = Compare;
    template <typename Key, typename T, typename Compare>
    struct value_compare {
        bool operator()(value_type<Key, T> const &lhs, value_type<Key, T> const &rhs) const {
            return Compare{}(key_extractor<Key, T>{}(lhs), key_extractor<Key, T>{}(rhs));
        }
    };
    using allocator_type = Allocator;
    using container_type = std::vector<value_type, allocator_type>;
    using reference = typename container_type::reference;
    using const_reference = typename container_type::const_reference;

    using size_type = typename container_type::size_type;

   private:
    static_assert(Degree >= 2, "Degree must be at least two");
    static constexpr size_type degree_ = Degree;
    static constexpr size_type root = size_type{0};
    using key_extractor = detail::key_extractor<Key, T>;

    container_type data_;
    key_compare comp_;

   private:
    static constexpr size_type parent(size_type index) noexcept {
        assert(index != root);
        return (index - size_type(1)) / degree_;
    }

    static constexpr size_type first_child(size_type index) noexcept {
        return index * degree_ + size_type(1);
    }

    // returns the index of the first node without all children
    constexpr size_type first_partial_parent() const noexcept {
        assert(!empty());
        return parent(size());
    }

    // Find the index of the smallest node
    // If no index is smaller than key, return `last`
    size_type min_node(size_type first, size_type last, key_type const &key) const {
        assert(first <= last);
        assert(last <= size());
        if (first == last) {
            return last;
        }
        auto smallest = last;
        for (; first != last; ++first) {
            if (comp_(key_extractor{}(data_[first]), key)) {
                smallest = first;
                ++first;
                break;
            }
        }
        for (; first != last; ++first) {
            if (comp_(key_extractor{}(data_[first]), key_extractor{}(data_[smallest]))) {
                smallest = first;
            }
        }
        return smallest;
    }

    size_type sift_up(size_type index, key_type key) {
        assert(index < size());
        for (size_type p; index != root && (p = parent(index), comp_(key_extractor{}(data_[p]), key)); index = p) {
            data_[index] = std::move(data_[parent]);
        }
        return index;
    }

    size_type delete_top(key_type key) {
        assert(!empty());
        if (size() == 1) {
            return 0;
        }
        size_type index = 0;
        size_type const last_full = first_partial_parent();
        while (index < last_full) {
            auto next = first_child(index);
            auto const last = next + degree_;
            next = min_node(next, last, key);
            if (next == last) {
                return index;
            }
            data_[index] = std::move(data_[next]);
            index = next;
        }
        if (index == last_full) {
            auto next = first_child(index);
            next = min_node(next, size(), key);
            if (next == size()) {
                return index;
            }
            data_[index] = std::move(data_[next]);
            index = next;
        }
        return index;
    }

#ifndef NDEBUG
    bool is_heap() const {
        for (size_type i = 0; i < size(); i++) {
            auto const first_child = first_child_index(i);
            for (size_type j = 0; j < Degree; ++j) {
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

    [[nodiscard]] bool empty() const noexcept {
        return data_.empty();
    }

    size_type size() const noexcept {
        return data_.size();
    }

    const_reference top() const {
        return data_.front();
    }

    void pop() {
        assert(!empty());
        auto const index = delete_top(key_extractor{}(data_.back()));
        if (index + size_type(1) < size()) {
            data_[index] = std::move(data_.back());
        }
        data_.pop_back();
        assert(is_heap());
    }

    void extract_top(reference retval) {
        assert(!data_.empty());
        retval = std::move(data_.front());
        pop();
        assert(is_heap());
    }

    void push(const_reference value) {
        if (size_t p; empty() || (p = parent(size()), !comp_(key_extractor{}(value)), key_extractor{}(data_[p]))) {
            data_.push_back(value);
        } else {
            data_.push_back(std::move(data_[p]));
            auto const index = sift_up(p, key_extractor{}(value));
            data_[index] = value;
        }
        assert(is_heap());
    }

    void push(value_type &&value) {
        if (size_t p; empty() || (p = parent(size()), !comp_(key_extractor{}(value)), key_extractor{}(data_[p]))) {
            data_.push_back(std::move(value));
        } else {
            data_.push_back(std::move(data_[p]));
            auto const index = sift_up(p, key_extractor{}(value));
            data_[index] = std::move(value);
        }
        assert(is_heap());
    }

    void reserve(size_type cap) {
        data_.reserve(cap);
    }

    void clear() noexcept {
        data_.clear();
    }
};

}  // namespace multiqueue

#endif  //! HEAP_HPP_INCLUDED
