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

#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>  // allocator
#include <sstream>
#include <string>
#include <type_traits>  // is_constructible, enable_if
#include <utility>      // move, forward, pair
#include <vector>

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

namespace multiqueue {

template <typename T, typename Compare = std::less<>, unsigned int Degree = 8, typename Container = std::vector<T>>
class Heap {
    static_assert(Degree >= 2, "Degree must be at least two");

   public:
    using value_type = T;
    using value_compare = Compare;
    using container_type = Container;
    using reference = typename container_type::reference;
    using const_reference = typename container_type::const_reference;

    using size_type = typename container_type::size_type;

   protected:
    container_type c;
    [[no_unique_address]] value_compare comp;

   private:
    static constexpr size_type root = size_type{0};

    static constexpr size_type parent(size_type index) noexcept {
        HEAP_ASSERT(index != root);
        return (index - size_type(1)) / Degree;
    }

    static constexpr size_type first_child(size_type index) noexcept {
        return index * Degree + size_type(1);
    }

    // returns the index of the first node without all children
    constexpr size_type current_parent() const noexcept {
        HEAP_ASSERT(!empty());
        return parent(size());
    }

    // Find the index of the node that should become the parent of the others
    // If no index is better than val, return last
    size_type new_parent(size_type first, size_type last, value_type val) const {
        HEAP_ASSERT(first <= last);
        HEAP_ASSERT(last <= size());
        auto best = last;
        for (; first != last; ++first) {
            if (comp(val, c[first])) {
                best = first;
                val = c[first];
            }
        }
        return best;
    }

    void sift_up(size_type index) {
        HEAP_ASSERT(index < size());
        if (index == root) {
            return;
        }
        value_type value = std::move(c[index]);
        size_type p = parent(index);
        while (comp(c[p], value)) {
            c[index] = std::move(c[p]);
            index = p;
            if (index == root) {
                break;
            }
            p = parent(index);
        }
        c[index] = std::move(value);
    }

    void sift_down(size_type index) {
        HEAP_ASSERT(index < size());
        value_type value = std::move(c[index]);
        size_type const end_full = current_parent();
        while (index < end_full) {
            auto const first = first_child(index);
            auto const last = first + Degree;
            auto const next = new_parent(first, last, value);
            if (next == last) {
                c[index] = std::move(value);
                return;
            }
            c[index] = std::move(c[next]);
            index = next;
        }
        if (index == end_full) {
            auto const first = first_child(index);
            auto const last = size();
            auto const next = new_parent(first, last, value);
            if (next == last) {
                c[index] = std::move(value);
            } else {
                c[index] = std::move(c[next]);
                c[next] = std::move(value);
            }
        } else {
            c[index] = std::move(value);
        }
    }

   public:
    explicit Heap(value_compare const &compare = value_compare()) noexcept : c(), comp{compare} {
    }

    template <typename Alloc>
    explicit Heap(value_compare const &compare, Alloc const &alloc) noexcept : c(alloc), comp{compare} {
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return c.empty();
    }

    constexpr size_type size() const noexcept {
        return c.size();
    }

    constexpr const_reference top() const {
        return c.front();
    }

    void pop() {
        HEAP_ASSERT(!empty());
        if (size() > size_type(1)) {
            c.front() = std::move(c.back());
            c.pop_back();
            sift_down(0);
        } else {
            c.pop_back();
        }
        HEAP_ASSERT(verify());
    }

    void push(const_reference value) {
        c.push_back(value);
        sift_up(size() - 1);
        HEAP_ASSERT(verify());
    }

    void push(value_type &&value) {
        c.push_back(std::move(value));
        sift_up(size() - 1);
        HEAP_ASSERT(verify());
    }

    template <typename... Args>
    void emplace(Args &&...args) {
        c.emplace_back(std::forward<Args>(args)...);
        sift_up(size() - 1);
        HEAP_ASSERT(verify());
    }

    constexpr void clear() noexcept {
        c.clear();
    }

    constexpr value_compare value_comp() const {
        return comp;
    }

    bool verify() const noexcept {
        auto const end_full = current_parent();
        for (size_type i = 0; i < end_full; i++) {
            auto const first = first_child(i);
            for (size_type j = first; j < first + Degree; ++j) {
                assert(j < size());
                if (comp(c[i], c[j])) {
                    return false;
                }
            }
        }
        for (size_type j = first_child(end_full); j < size(); ++j) {
            if (comp(c[end_full], c[j])) {
                return false;
            }
        }
    }
};

}  // namespace multiqueue

namespace std {
template <typename T, typename Compare, unsigned int Degree, typename Container, typename Alloc>
struct uses_allocator<multiqueue::Heap<T, Compare, Degree, Container>, Alloc> : uses_allocator<Container, Alloc>::type {
};

}  // namespace std

#undef HEAP_ASSERT

#endif  //! HEAP_HPP_INCLUDED
