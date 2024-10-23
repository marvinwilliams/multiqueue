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
#include <cstdint>
#include <functional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace multififo {

template <typename T, typename Container = std::vector<T>>
class RingBuffer {
   public:
    using value_type = T;
    using container_type = Container;
    using reference = typename container_type::reference;
    using const_reference = typename container_type::const_reference;

    using size_type = typename container_type::size_type;

   protected:
    // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes): Compatibility to std::queue
    container_type c;

   private:
    std::uint64_t head_{0};
    std::uint64_t tail_{0};

   public:
    explicit RingBuffer(size_type capacity) noexcept(noexcept(Container(capacity))) : c(capacity) {
        assert(capacity > 0 && (capacity & (capacity - 1)) == 0);
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<Container, Alloc>>>
    explicit RingBuffer(size_type capacity, Alloc const &alloc) noexcept : c(capacity, alloc) {
        assert(capacity > 0 && (capacity & (capacity - 1)) == 0);
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return size() == 0;
    }

    constexpr size_type size() const noexcept {
        return head_ - tail_;
    }

    constexpr size_type capacity() const noexcept {
        return c.size();
    }

    constexpr bool full() const noexcept {
        return size() == capacity();
    }

    constexpr const_reference top() const {
        return c[tail_ & (c.size() - 1)];
    }

    void pop() {
        assert(!empty());
        ++tail_;
    }

    void push(const_reference value) {
        c[head_ & (c.size() - 1)] = value;
        ++head_;
    }

    void push(value_type &&value) {
        c[head_ & (c.size() - 1)] = std::move(value);
        ++head_;
    }

    constexpr void clear() noexcept {
        head_ = 0;
        tail_ = 0;
    }
};

}  // namespace multififo

namespace std {
template <typename T, typename Container, typename Alloc>
struct uses_allocator<multififo::RingBuffer<T, Container>, Alloc>
    : uses_allocator<Container, Alloc>::type {};

}  // namespace std
