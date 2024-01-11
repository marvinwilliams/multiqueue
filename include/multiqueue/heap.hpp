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

#include <cassert>
#include <cstddef>
#include <functional>
#include <utility>
#include <vector>

namespace multiqueue {

template <typename T, typename Compare = std::less<>, unsigned int arity = 8, typename Container = std::vector<T>>
class Heap {
    static_assert(arity >= 2, "Arity must be at least two");

   public:
    using value_type = T;
    using value_compare = Compare;
    using container_type = Container;
    using reference = typename container_type::reference;
    using const_reference = typename container_type::const_reference;

    using size_type = typename container_type::size_type;

   protected:
    // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes): Compatibility to std::priority_queue
    container_type c;
    // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes): Compatibility to std::priority_queue
    [[no_unique_address]] value_compare comp;

   private:
    static constexpr size_type root = size_type{0};

    static constexpr size_type parent(size_type index) {
        assert(index != root);
        return (index - size_type(1)) / arity;
    }

    static constexpr size_type first_child(size_type index) noexcept {
        return index * arity + size_type(1);
    }

    // Find the index of the node that should become the parent of the others
    // If no index is better than the last element, return last
    size_type new_parent(size_type first, size_type last) const {
        assert(first <= last);
        assert(last <= size());
        auto best = size() - 1;
        for (; first != last; ++first) {
            if (comp(c[best], c[first])) {
                best = first;
            }
        }
        return best;
    }

    void sift_up() {
        size_type index = size() - 1;
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

    void sift_down() {
        assert(!empty());
        if (size() == 1) {
            return;
        }
        size_type const end_full = parent(size() - 1);
        size_type index = 0;
        while (index < end_full) {
            auto const first = first_child(index);
            auto const last = first + arity;
            auto const next = new_parent(first, last);
            if (next == size() - 1) {
                c[index] = std::move(c[size() - 1]);
                return;
            }
            c[index] = std::move(c[next]);
            index = next;
        }
        if (index == end_full) {
            auto const first = first_child(index);
            auto const last = size() - 1;
            auto const next = new_parent(first, last);
            if (next == last) {
                c[index] = std::move(c[size() - 1]);
                return;
            }
            c[index] = std::move(c[next]);
            index = next;
        }
        c[index] = std::move(c[size() - 1]);
    }

   public:
    explicit Heap(value_compare const &compare = value_compare()) noexcept(noexcept(Container())) : c(), comp{compare} {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<Container, Alloc>>>
    explicit Heap(value_compare const &compare, Alloc const &alloc) noexcept : c(alloc), comp{compare} {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<Container, Alloc>>>
    explicit Heap(Alloc const &alloc) noexcept : c(alloc), comp() {
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
        assert(!empty());
        sift_down();
        c.pop_back();
    }

    void push(const_reference value) {
        c.push_back(value);
        sift_up();
    }

    void push(value_type &&value) {
        c.push_back(std::move(value));
        sift_up();
    }

    template <typename... Args>
    void emplace(Args &&...args) {
        c.emplace_back(std::forward<Args>(args)...);
        sift_up();
    }

    constexpr void clear() noexcept {
        c.clear();
    }

    constexpr value_compare value_comp() const {
        return comp;
    }
};

}  // namespace multiqueue

namespace std {
template <typename T, typename Compare, unsigned int arity, typename Container, typename Alloc>
struct uses_allocator<multiqueue::Heap<T, Compare, arity, Container>, Alloc> : uses_allocator<Container, Alloc>::type {
};

}  // namespace std
