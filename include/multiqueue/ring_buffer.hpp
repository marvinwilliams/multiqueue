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
#ifndef UTIL_RING_BUFFER_HPP_INCLUDED
#define UTIL_RING_BUFFER_HPP_INCLUDED

#include <array>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <type_traits>

namespace multiqueue {

template <typename T, std::size_t N>
struct ring_buffer;

template <typename T, std::size_t N>
class ring_buffer_iterator {
    friend ring_buffer<std::decay_t<T>, N>;

   public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using reference = T&;
    using pointer = T*;

   protected:
    std::size_t pos_;
    T* data_;

    explicit constexpr ring_buffer_iterator(T* data, std::size_t pos) noexcept : data_{data}, pos_{pos} {
    }

   public:
    constexpr reference operator*() const noexcept {
        return data_[pos_ & (N - 1u)];
    }

    constexpr pointer operator->() const noexcept {
        return data_ + (pos_ & (N - 1u));
    }

    constexpr ring_buffer_iterator& operator++() noexcept {
        ++pos_;
        return *this;
    }

    constexpr ring_buffer_iterator operator++(int) noexcept {
        auto tmp = *this;
        ++(*this);
        return tmp;
    }

    constexpr ring_buffer_iterator& operator--() noexcept {
        --pos_;
        return *this;
    }

    constexpr ring_buffer_iterator operator--(int) noexcept {
        auto tmp = *this;
        --(*this);
        return tmp;
    }

    constexpr ring_buffer_iterator& operator+=(difference_type n) noexcept {
        pos_ += n;
        return *this;
    }

    constexpr ring_buffer_iterator operator+(difference_type n) noexcept {
        auto tmp = *this;
        tmp += n;
        return tmp;
    }

    constexpr ring_buffer_iterator& operator-=(difference_type n) noexcept {
        pos_ -= n;
        return *this;
    }

    constexpr ring_buffer_iterator operator-(difference_type n) noexcept {
        auto tmp = *this;
        tmp -= n;
        return tmp;
    }

    constexpr reference operator[](difference_type n) noexcept {
        return *(*this + n);
    }

    constexpr difference_type operator-(ring_buffer_iterator const& other) noexcept {
        return pos_ - other.pos_;
    }

    constexpr difference_type operator<(ring_buffer_iterator const& other) noexcept {
        return pos_ < other.pos_;
    }

    constexpr difference_type operator>(ring_buffer_iterator const& other) noexcept {
        return pos_ > other.pos_;
    }

    constexpr difference_type operator<=(ring_buffer_iterator const& other) noexcept {
        return pos_ <= other.pos_;
    }

    constexpr difference_type operator>=(ring_buffer_iterator const& other) noexcept {
        return pos_ >= other.pos_;
    }

    friend bool operator==(ring_buffer_iterator const& lhs, ring_buffer_iterator const& rhs) noexcept {
        return lhs.pos_ == rhs.pos_;
    }

    friend bool operator!=(ring_buffer_iterator const& lhs, ring_buffer_iterator const& rhs) noexcept {
        return !(lhs == rhs);
    }
};

template <typename T, std::size_t N>
struct ring_buffer {
    static_assert(N > 0u && (N & (N - 1u)) == 0u, "N must be greater than 0 and a power of two");
    static constexpr std::size_t Capacity = N;

    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using reference = value_type&;
    using const_reference = value_type const&;
    using pointer = value_type*;
    using const_pointer = value_type const*;
    using iterator = ring_buffer_iterator<T, N>;
    using const_iterator = ring_buffer_iterator<T const, N>;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

   private:
    static constexpr std::size_t mask = N - 1u;

    std::array<T, N> data_;
    std::size_t begin_ = 0u;
    std::size_t end_ = 0u;

   public:
    bool empty() const noexcept {
        return begin_ == end_;
    }

    bool full() const noexcept {
        return end_ == begin_ + N;
    }

    std::size_t size() const noexcept {
        return (end_ - begin_) & mask;
    }

    void push_front(T const& t) {
        assert(!full());
        --begin_;
        data_[begin_ & mask] = t;
    }

    void push_back(T const& t) {
        assert(!full());
        data_[end_ & mask] = t;
        ++end_;
    }

    void pop_front() {
        assert(!empty());
        ++begin_;
    }

    inline void pop_back() {
        assert(!empty());
        --end_;
    }

    void insert_at(const_iterator it, T const& t) {
        assert(begin_ < end_ ? (begin_ <= it.pos_ && it.pos_ <= end_) : (it.pos_ >= begin_ || it.pos_ <= end_));
        assert(!full());
        std::size_t left_elements = it.pos_ - begin_;
        std::size_t right_elements = end_ - it.pos_;
        if (left_elements < right_elements) {
            for (std::size_t i = 0; i < left_elements; ++i) {
                data_[(begin_ + i - 1) & mask] = std::move(data_[(begin_ + i) & mask]);
            }
            data_[(it.pos_ - 1) & mask] = t;
            --begin_;
        } else {
            for (std::size_t i = 0; i < right_elements; ++i) {
                data_[(end_ - i) & mask] = std::move(data_[(end_ - i - 1) & mask]);
            }
            data_[it.pos_ & mask] = t;
            ++end_;
        }
    }

    void clear() {
        begin_ = end_ = 0u;
    }

    T const& operator[](std::size_t pos) const noexcept {
        assert(pos < size());
        return data_[(begin_ + pos) & mask];
    }

    T& operator[](std::size_t pos) noexcept {
        return data_[(begin_ + pos) & mask];
    }

    T const& front() const noexcept {
        assert(!empty());
        return data_[begin_ & mask];
    }

    T& front() noexcept {
        return data_[begin_ & mask];
    }

    T const& back() const noexcept {
        assert(!empty());
        return data_[(end_ - 1) & mask];
    }

    T& back() noexcept {
        return data_[(end_ - 1) & mask];
    }

    constexpr iterator begin() noexcept {
        return iterator{data_.data(), begin_};
    }

    constexpr const const_iterator begin() const noexcept {
        return const_iterator{data_.data(), begin_};
    }

    constexpr const const_iterator cbegin() const noexcept {
        return const_iterator{data_.data(), begin_};
    }

    constexpr iterator end() noexcept {
        return iterator{data_.data(), end_};
    }

    constexpr const const_iterator end() const noexcept {
        return const_iterator{data_.data(), end_};
    }

    constexpr const const_iterator cend() const noexcept {
        return const_iterator{data_.data(), end_};
    }

    constexpr reverse_iterator rbegin() noexcept {
        return reverse_iterator{end()};
    }

    constexpr const const_reverse_iterator rbegin() const noexcept {
        return const_reverse_iterator{end()};
    }

    constexpr const const_reverse_iterator crbegin() const noexcept {
        return const_reverse_iterator{cend()};
    }

    constexpr reverse_iterator rend() noexcept {
        return reverse_iterator{begin()};
    }

    constexpr const const_reverse_iterator rend() const noexcept {
        return const_reverse_iterator{begin()};
    }

    constexpr const const_reverse_iterator crend() const noexcept {
        return const_reverse_iterator{cbegin()};
    }
};

}  // namespace multiqueue

#endif  //! UTIL_RING_BUFFER_HPP_INCLUDED
