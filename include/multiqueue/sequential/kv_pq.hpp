/**
******************************************************************************
* @file:   kv_pq.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 18:09
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef SEQUENTIAL_KV_PQ_HPP_INCLUDED
#define SEQUENTIAL_KV_PQ_HPP_INCLUDED

#include "multiqueue/sequential/heap/full_down_strategy.hpp"
#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/util/extractors.hpp"

#include <cassert>
#include <functional>  // std::less
#include <utility>     // std::swap, std::pair
#include <vector>

namespace multiqueue {
namespace sequential {

template <typename Key, typename T, typename Comparator = std::less<Key>, unsigned int HeapDegree = 8,
          typename SiftStrategy = sift_strategy::FullDown, typename Allocator = std::allocator<T>>
class kv_pq {
   private:
    using heap_type = key_value_heap<Key, T, Comparator, HeapDegree, SiftStrategy, Allocator>;

   public:
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<key_type, mapped_type>;
    using key_comparator = Comparator;
    class value_comparator : private key_comparator {
        friend kv_pq;
        explicit value_comparator(key_comparator const &comp) : key_comparator{comp} {
        }

       public:
        constexpr bool operator()(value_type const &lhs, value_type const &rhs) const {
            return key_comparator::operator()(util::get_nth<value_type>{}(lhs), util::get_nth<value_type>{}(rhs));
        }
    };
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
    kv_pq() = default;

    explicit kv_pq(Comparator const &c) : heap_(c) {
    }

    explicit kv_pq(allocator_type const &a) : heap_(a) {
    }

    explicit kv_pq(key_comparator const &c, allocator_type const &a) : heap_(c, a) {
    }

    constexpr key_comparator key_comp() const noexcept {
        return heap_.to_comparator();
    }

    constexpr value_comparator value_comp() const noexcept {
        return value_comparator{heap_.key_comp()};
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

    constexpr void push(value_type const &value) {
        heap_.insert(value);
    }

    constexpr void push(value_type &&value) {
        heap_.insert(std::move(value));
    }

    template <typename insert_key_type, typename... Args>
    constexpr void emplace_key(insert_key_type &&key, Args &&...args) {
        heap_.emplace_known(key, std::piecewise_construct, std::forward_as_tuple(std::forward<insert_key_type>(key)),
                            std::forward_as_tuple(std::forward<Args>(args)...));
    }
};

}  // namespace sequential
}  // namespace multiqueue

#endif  //! SEQUENTIAL_KV_PQ_HPP_INCLUDED
