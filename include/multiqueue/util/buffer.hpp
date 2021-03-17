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
    static_assert(N > 0u, "N must be greater than 0");

    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using reference = value_type&;
    using const_reference = value_type const&;
    using pointer = value_type*;
    using const_pointer = value_type const*;
    using iterator = typename std::array<T, N>::iterator;
    using const_iterator = typename std::array<T, N>::const_iterator;
    using reverse_iterator = typename std::array<T, N>::reverse_iterator;
    using const_reverse_iterator = typename std::array<T, N>::const_reverse_iterator;

   private:
    std::array<T, N> data_;
    size_type size_ = 0u;

   public:
    inline size_type size() const noexcept {
        return size_;
    }

    inline bool empty() const noexcept {
        return size_ == 0u;
    }

    inline void set_size(size_type const size) noexcept {
        size_ = size;
    }

    void push_back(T const& t) {
        assert(size_ < N);
        data_[size_] = t;
        ++size_;
    }

    void insert_at(size_type pos, T const& t) {
        assert(size_ < N);
        assert(pos <= size_);
        for (size_type i = 0u; i < size_ - pos; ++i) {
            data_[size_ - i] = std::move(data_[size_ - (i + 1)]);
        }
        data_[pos] = t;
        ++size_;
    }

    inline void pop_back() {
        assert(!empty());
        --size_;
    }

    inline void clear() {
        size_ = 0u;
    }

    T const& operator[](size_type pos) const noexcept {
        assert(pos < size_);
        return data_[pos];
    }

    T& operator[](size_type pos) noexcept {
        return data_[pos];
    }

    T const& front() const noexcept {
        assert(!empty());
        return data_[0u];
    }

    T& front() noexcept {
        return data_[0u];
    }

    T const& back() const noexcept {
        assert(!empty());
        return data_[size_ - 1u];
    }

    T& back() noexcept {
        return data_[size_ - 1u];
    }

    constexpr iterator begin() noexcept {
        return data_.begin();
    }

    constexpr const const_iterator begin() const noexcept {
        return data_.begin();
    }

    constexpr const const_iterator cbegin() const noexcept {
        return data_.cbegin();
    }

    constexpr iterator end() noexcept {
        return data_.begin() + size_;
    }

    constexpr const const_iterator end() const noexcept {
        return data_.begin() + size_;
    }

    constexpr const const_iterator cend() const noexcept {
        return data_.cbegin() + size_;
    }

    constexpr reverse_iterator rbegin() noexcept {
        return data_.rbegin();
    }

    constexpr const const_reverse_iterator rbegin() const noexcept {
        return data_.rbegin();
    }

    constexpr const const_reverse_iterator crbegin() const noexcept {
        return data_.crbegin();
    }

    constexpr reverse_iterator rend() noexcept {
        return data_.rend();
    }

    constexpr const const_reverse_iterator rend() const noexcept {
        return data_.rend();
    }

    constexpr const const_reverse_iterator crend() const noexcept {
        return data_.crend();
    }
};

}  // namespace util
}  // namespace multiqueue

#endif  //! BUFFER_HPP_INCLUDED
