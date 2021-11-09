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

#ifdef HEAP_DEBUG

#define HEAP_ASSERT(x) \
    do {               \
        assert(x);     \
    } while (false)

#else

#define HEAP_ASSERT(x) \
    do {               \
    } while (false)

#endif

template <typename Key, typename T, typename Compare, unsigned int Degree, typename Allocator>
class Heap {
   public:
    using key_type = Key;
    using value_type = detail::value_type<Key, T>;

   private:
    using key_of = detail::key_extractor<Key, T>;

   public:
    using key_compare = Compare;
    struct value_compare {
        bool operator()(value_type const &lhs, value_type const &rhs) const {
            return Compare{}(key_of{}(lhs), key_of{}(rhs));
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

    container_type data_;
    key_compare comp_;

   private:
    static constexpr size_type parent(size_type index) noexcept {
        HEAP_ASSERT(index != root);
        return (index - size_type(1)) / degree_;
    }

    static constexpr size_type first_child(size_type index) noexcept {
        return index * degree_ + size_type(1);
    }

    // returns the index of the first node without all children
    constexpr size_type first_nonfull_parent() const noexcept {
        HEAP_ASSERT(!empty());
        return parent(size());
    }

    // Find the index of the smallest node
    // If no index is smaller than key, return `last`
    size_type min_node(size_type first, size_type last, key_type const &key) const {
        HEAP_ASSERT(first <= last);
        HEAP_ASSERT(last <= size());
        if (first == last) {
            return last;
        }
        auto smallest = last;
        for (; first != last; ++first) {
            if (comp_(key_of{}(data_[first]), key)) {
                smallest = first;
                ++first;
                break;
            }
        }
        for (; first != last; ++first) {
            if (comp_(key_of{}(data_[first]), key_of{}(data_[smallest]))) {
                smallest = first;
            }
        }
        return smallest;
    }

    size_type sift_up(size_type index, key_type key) {
        HEAP_ASSERT(index < size());
        for (size_type p; index != root && (p = parent(index), comp_(key, key_of{}(data_[p]))); index = p) {
            data_[index] = std::move(data_[parent]);
        }
        return index;
    }

    size_type sift_down(size_type index, key_type key) {
        HEAP_ASSERT(index < size());
        size_type const first_nonfull = first_nonfull_parent();
        while (index < first_nonfull) {
            auto next = first_child(index);
            auto const last = next + degree_;
            next = min_node(next, last, key);
            if (next == last) {
                return index;
            }
            data_[index] = std::move(data_[next]);
            index = next;
        }
        if (index == first_nonfull) {
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

   public:
    Heap() = default;

    explicit Heap(allocator_type const &alloc = allocator_type()) noexcept : data_(alloc) {
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return data_.empty();
    }

    constexpr size_type size() const noexcept {
        return data_.size();
    }

    constexpr const_reference top() const {
        return data_.front();
    }

    void pop() {
        HEAP_ASSERT(!empty());
        if (size() > size_type(1)) {
            auto const index = sift_down(0, key_of{}(data_.back()));
            if (index + size_type(1) < size()) {
                data_[index] = std::move(data_.back());
            }
        }
        data_.pop_back();
        HEAP_ASSERT(verify());
    }

    void extract_top(reference retval) {
        HEAP_ASSERT(!data_.empty());
        retval = std::move(data_.front());
        pop();
        HEAP_ASSERT(verify());
    }

    void push(const_reference value) {
        if (empty()) {
            data_.push_back(value);
        } else {
            size_type p = parent(size());
            if (!comp_(key_of{}(value)), key_of{}(data_[p])) {
                data_.push_back(value);
            } else {
                data_.push_back(std::move(data_[p]));
                auto const index = sift_up(p, key_of{}(value));
                data_[index] = value;
            }
        }
        HEAP_ASSERT(verify());
    }

    void push(value_type &&value) {
        if (empty()) {
            data_.push_back(std::move(value));
        } else {
            size_type p = parent(size());
            if (!comp_(key_of{}(value)), key_of{}(data_[p])) {
                data_.push_back(std::move(value));
            } else {
                data_.push_back(std::move(data_[p]));
                auto const index = sift_up(p, key_of{}(value));
                data_[index] = std::move(value);
            }
        }
        HEAP_ASSERT(verify());
    }

    void reserve(size_type cap) {
        data_.reserve(cap);
    }

    constexpr void clear() noexcept {
        data_.clear();
    }

    bool verify() const noexcept{
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
};

#undef HEAP_ASSERT

}  // namespace multiqueue

#endif  //! HEAP_HPP_INCLUDED
