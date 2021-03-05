/**
******************************************************************************
* @file:   buffer.hpp
*
* @author: Marvin Williams
* @date:   2021/03/08 15:56
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef BUFFER_HPP_INCLUDED
#define BUFFER_HPP_INCLUDED

#include <array>
#include <cassert>
#include <cstdint>

namespace multiqueue {
namespace util {

template <typename T, std::size_t N>
struct buffer {
    static_assert(N > 0, "N must be greater than 0");
    std::array<T, N> data;
    std::size_t size = 0;

    inline bool empty() const noexcept {
        return size == 0;
    }

    void push_back(T const& t) {
        assert(size < N);
        data[size] = t;
        ++size;
    }

    void insert_at(std::size_t pos, T const& t) {
        assert(size < N);
        assert(pos <= size);
        for (std::size_t i = 0; i < size - pos; ++i) {
            data[size - i] = std::move(data[size - (i + 1)]);
        }
        data[pos] = t;
        ++size;
    }

    inline void pop_back() {
        assert(!empty());
        --size;
    }

    inline void clear() {
        size = 0u;
    }

    T const& operator[](std::size_t pos) const noexcept {
        assert(pos < size);
        return data[pos];
    }

    T& operator[](std::size_t pos) noexcept {
        return data[pos];
    }

    T const& front() const noexcept {
        assert(!empty());
        return data[0u];
    }

    T& front() noexcept {
        return data[0u];
    }

    T const& back() const noexcept {
        assert(!empty());
        return data[size - 1u];
    }

    T& back() noexcept {
        return data[size - 1u];
    }
};

}  // namespace util
}  // namespace multiqueue

#endif  //! BUFFER_HPP_INCLUDED
