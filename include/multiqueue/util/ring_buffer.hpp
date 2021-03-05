/**
******************************************************************************
* @file:   ring_buffer.hpp
*
* @author: Marvin Williams
* @date:   2021/03/05 16:57
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef RING_BUFFER_HPP_INCLUDED
#define RING_BUFFER_HPP_INCLUDED

#include <array>
#include <cassert>
#include <cstdint>

namespace multiqueue {
namespace util {

template <typename T, std::size_t N>
struct ring_buffer {
    static_assert(N > 0u && (N & (N - 1u)) == 0u, "N must be greater than 0 and a power of two");
    static constexpr std::size_t mask = N - 1u;
    std::array<T, N> data;
    std::size_t begin = 0u;
    std::size_t end = 0u;
    bool full = false;

    inline bool empty() const noexcept {
        return begin == end && !full;
    }

    inline std::size_t size() const noexcept {
        return full ? N : ((end - begin) & mask);
    }

    void push_front(T const& t) {
        assert(!full);
        --begin &= mask;
        data[begin] = t;
        full = (begin == end);
    }

    void push_back(T const& t) {
        assert(!full);
        data[end] = t;
        ++end &= mask;
        full = (begin == end);
    }

    // `pos` is relative to begin
    void insert_at(std::size_t pos, T const& t) {
        assert(pos <= size());
        assert(!full);
        if (pos <= size() / 2u) {
            --begin &= mask;
            for (std::size_t i = 0u; i < pos; ++i) {
                data[(begin + i) & mask] = std::move(data[(begin + i + 1u) & mask]);
            }
            data[(begin + pos) & mask] = t;
        } else {
            for (std::size_t i = 0u; i < size() - pos; ++i) {
                data[(end - i) & mask] = std::move(data[(end - (i + 1u)) & mask]);
            }
            data[(end - (size() - pos)) & mask] = t;
            ++end &= mask;
        }
        full = (begin == end);
    }

    inline void pop_front() {
        assert(!empty());
        ++begin &= mask;
        full = false;
    }

    inline void pop_back() {
        assert(!empty());
        --end &= mask;
        full = false;
    }

    inline void clear() {
        begin = end = 0u;
        full = false;
    }

    // Relative to `begin`
    T const& operator[](std::size_t pos) const noexcept {
        assert(pos < size());
        return data[(begin + pos) & mask];
    }

    T& operator[](std::size_t pos) noexcept {
        return data[(begin + pos) & mask];
    }

    T const& front() const noexcept {
        assert(!empty());
        return data[begin];
    }

    T& front() noexcept {
        return data[begin];
    }

    T const& back() const noexcept {
        assert(!empty());
        return data[(end - 1) & mask];
    }

    T& back() noexcept {
        return data[(end - 1) & mask];
    }
};

}  // namespace util
}  // namespace multiqueue

#endif  //! RING_BUFFER_HPP_INCLUDED
