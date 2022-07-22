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
#ifndef SEQUENTIAL_HEAP_HEAP_HPP_INCLUDED
#define SEQUENTIAL_HEAP_HEAP_HPP_INCLUDED

#include "multiqueue/sequential/heap/full_down_strategy.hpp"

#include <cassert>
#include <cstddef>
#include <memory>       // allocator
#include <type_traits>  // is_constructible, enable_if
#include <utility>      // move, forward, pair
#include <vector>

namespace multiqueue {
template <typename T, typename Comparator>
struct heap_base {
    using value_type = T;
    using comp_type = Comparator;
    using reference = value_type &;
    using const_reference = value_type const &;

   protected:
    comp_type comp;
    static constexpr bool is_compare_noexcept =
        noexcept(std::declval<comp_type>()(std::declval<value_type>(), std::declval<value_type>()));

    heap_base() = default;

    explicit heap_base(comp_type const &comp) noexcept : comp(comp) {
    }

    constexpr comp_type const &to_comparator() const noexcept {
        return comp;
    }

    constexpr bool compare(value_type const &lhs, value_type const &rhs) const noexcept(is_compare_noexcept) {
        return comp(lhs, rhs);
    }
};

template <typename T, typename Comparator = std::less<>, unsigned int Degree = 8, typename SiftStrategy = sift_strategy::FullDown,
          typename Allocator = std::allocator<T>>
class Heap : protected heap_base<T, Comparator> {
    friend SiftStrategy;
    using base_type = heap_base<T, Comparator>;
    using base_type::compare;

   public:
    using value_type = typename base_type::value_type;
    using comp_type = typename base_type::comp_type;
    using value_compare = typename base_type::comp_type;
    using reference = typename base_type::reference;
    using const_reference = typename base_type::const_reference;

    using allocator_type = Allocator;
    using container_type = std::vector<value_type, allocator_type>;
    using iterator = typename container_type::const_iterator;
    using const_iterator = typename container_type::const_iterator;
    using difference_type = typename container_type::difference_type;
    using size_type = std::size_t;

    static_assert(Degree >= 1, "Degree must be at least one");

   protected:
    static constexpr auto degree_ = Degree;
    container_type c;

   private:
    static constexpr std::size_t parent_index(std::size_t const index) noexcept {
        return (index - 1) / Degree;
    }

    static constexpr std::size_t first_child_index(std::size_t const index) noexcept {
        return index * Degree + 1;
    }

    // Find the index of the smallest `num_children` children of the node at
    // index `index`
    constexpr size_type min_child_index(size_type index, size_type const num_children = Degree) const
        noexcept(base_type::is_compare_noexcept) {
        assert(index < size());
        index = first_child_index(index);
        if (num_children == 1) {
            return index;
        }
        auto const last = index + num_children;
        assert(last <= size());
        auto result = index++;
        for (; index < last; ++index) {
            if (compare(c[result], c[index])) {
                result = index;
            }
        }
        return result;
    }

#ifndef NDEBUG
    bool is_heap() const {
        for (size_type i = 0; i < size(); i++) {
            auto const first_child = first_child_index(i);
            for (std::size_t j = 0; j < Degree; ++j) {
                if (first_child + j >= size()) {
                    return true;
                }
                if (compare(c[i], c[first_child + j])) {
                    return false;
                }
            }
        }
        return true;
    }
#endif
   public:
    Heap() = default;

    explicit Heap(allocator_type const &alloc) noexcept(std::is_nothrow_default_constructible_v<base_type>)
        : base_type(), c(alloc) {
    }

    explicit Heap(comp_type const &comp, allocator_type const &alloc = allocator_type()) noexcept(
        std::is_nothrow_constructible_v<base_type, comp_type>)
        : base_type(comp), c(alloc) {
    }

    constexpr comp_type const &get_comparator() const noexcept {
        return base_type::to_comparator();
    }

    inline iterator begin() const noexcept {
        return c.cbegin();
    }

    inline const_iterator cbegin() const noexcept {
        return c.cbegin();
    }

    inline iterator end() const noexcept {
        return c.cend();
    }

    inline const_iterator cend() const noexcept {
        return c.cend();
    }

    [[nodiscard]] inline bool empty() const noexcept {
        return c.empty();
    }

    inline size_type size() const noexcept {
        return c.size();
    }

    inline const_reference top() const {
        return c.front();
    }

    void pop() {
        assert(!c.empty());
        auto const index = SiftStrategy::remove(*this, 0);
        if (index + 1 < size()) {
            c[index] = std::move(c.back());
        }
        c.pop_back();
        assert(is_heap());
    }

    void push(value_type const &value) {
        size_type parent;
        if (!empty() && (parent = parent_index(size()), compare(c[parent], value))) {
            c.push_back(std::move(c[parent]));
            auto const index = SiftStrategy::sift_up_hole(*this, parent, value);
            c[index] = value;
            assert(is_heap());
        } else {
            c.push_back(value);
        }
    }

    inline void reserve(std::size_t const cap) {
        c.reserve(cap);
    }

    inline void reserve_and_touch(std::size_t const cap) {
        if (size() < cap) {
            size_type const old_size = size();
            c.resize(cap);
            // this does not free allocated memory
            c.resize(old_size);
        }
    }

    inline void clear() noexcept {
        c.clear();
    }
};

}  // namespace multiqueue

#endif  //! SEQUENTIAL_HEAP_HEAP_HPP_INCLUDED
