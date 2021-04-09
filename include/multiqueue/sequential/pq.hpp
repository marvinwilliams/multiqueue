/**
******************************************************************************
* @file:   pq.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 16:29
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef SEQUENTIAL_PQ_HPP_INCLUDED
#define SEQUENTIAL_PQ_HPP_INCLUDED

#include "multiqueue/sequential/heap/full_down_strategy.hpp"
#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/util/extractors.hpp"

#include <cassert>
#include <functional>  // std::less
#include <memory>
#include <utility>  // std::swap
#include <vector>

namespace multiqueue {
namespace sequential {

template <typename T, typename Comparator = std::less<T>, unsigned int HeapDegree = 4,
          typename SiftStrategy = sift_strategy::FullDown, typename Allocator = std::allocator<T>>
class pq {
   private:
    using heap_type = value_heap<T, Comparator, HeapDegree, SiftStrategy, Allocator>;

   public:
    using key_type = T;
    using value_type = T;
    using key_comparator = Comparator;
    using value_comparator = Comparator;
    using allocator_type = Allocator;
    using reference = typename heap_type::reference;
    using const_reference = typename heap_type::const_reference;
    using iterator = typename heap_type::iterator;
    using const_iterator = typename heap_type::const_iterator;
    using difference_type = typename heap_type::difference_type;
    using size_type = typename heap_type::size_type;

   private:
    heap_type heap_;

   public:
    pq() = default;

    explicit pq(key_comparator const &c) : heap_(c) {
    }

    explicit pq(allocator_type const &a) : heap_(a) {
    }

    explicit pq(key_comparator const &c, allocator_type const &a) : heap_(c, a) {
    }

    constexpr key_comparator key_comp() const noexcept {
        return heap_.key_comp();
    }

    constexpr value_comparator value_comp() const noexcept {
        return key_comp();
    }

    constexpr iterator begin() noexcept {
        return heap_.begin();
    }

    constexpr const_iterator begin() const noexcept {
        return heap_.cbegin();
    }

    constexpr const_iterator cbegin() const noexcept {
        return heap_.cbegin();
    }

    constexpr iterator end() noexcept {
        return heap_.end();
    }

    constexpr const_iterator end() const noexcept {
        return heap_.cend();
    }

    constexpr const_iterator cend() const noexcept {
        return heap_.cend();
    }

    constexpr size_type size() const noexcept {
        return heap_.size();
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return heap_.empty();
    }

    constexpr const_reference top() const {
        return heap_.top();
    }

    constexpr void clear() noexcept {
        heap_.clear();
    }

    constexpr void reserve(size_type new_cap) {
        heap_.reserve(new_cap);
    }

    constexpr void pop() {
        assert(!empty());
        heap_.pop();
    }

    constexpr bool extract_top(value_type &retval) {
        if (heap_.empty()) {
            return false;
        }
        heap_.extract_top(retval);
        return true;
    }

    constexpr void push(value_type value) {
        heap_.insert(std::move(value));
    }
};

}  // namespace sequential
}  // namespace multiqueue

#endif  //! SEQUENTIAL_PQ_HPP_INCLUDED
