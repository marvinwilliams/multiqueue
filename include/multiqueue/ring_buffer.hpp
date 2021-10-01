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
#include <iterator>
#include <limits>
#include <type_traits>

namespace multiqueue {

template <typename T, std::uint64_t N>
struct ring_buffer;

template <typename T, std::uint64_t N>
class ring_buffer_iterator {
    friend ring_buffer<std::decay_t<T>, N>;

   protected:
    std::uint64_t pos_;
    std::uint64_t begin_;
    T* data_;

    explicit constexpr ring_buffer_iterator(T* data, std::uint64_t begin, std::uint64_t pos = 0) noexcept
        : data_{data}, begin_{begin}, pos_{pos} {
    }

   public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using reference = T&;
    using pointer = T*;

    constexpr reference operator*() const noexcept {
        return *(data_ + ((begin_ + pos_) & (N - 1u)));
    }

    constexpr pointer operator->() const noexcept {
        return data_ + ((begin_ + pos_) & (N - 1u));
    }

    constexpr ring_buffer_iterator& operator++() noexcept {
        assert(pos_ < N);
        ++pos_;
        return *this;
    }

    constexpr ring_buffer_iterator operator++(int) noexcept {
        auto tmp = *this;
        operator++();
        return tmp;
    }

    constexpr ring_buffer_iterator& operator--() noexcept {
        assert(pos_ > 0);
        --pos_;
        return *this;
    }

    constexpr ring_buffer_iterator operator--(int) noexcept {
        auto tmp = *this;
        operator--();
        return tmp;
    }

    constexpr ring_buffer_iterator& operator+=(difference_type n) noexcept {
        assert(pos_ + n <= N);
        pos_ += n;
        return *this;
    }

    constexpr ring_buffer_iterator operator+(difference_type n) noexcept {
        auto tmp = *this;
        tmp += n;
        return tmp;
    }

    constexpr ring_buffer_iterator& operator-=(difference_type n) noexcept {
        assert(pos_ >= n);
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
        return lhs.data_ == rhs.data_ && lhs.begin_ == rhs.begin_ && lhs.pos_ == rhs.pos_;
    }

    friend bool operator!=(ring_buffer_iterator const& lhs, ring_buffer_iterator const& rhs) noexcept {
        return !(lhs == rhs);
    }
};

template <typename T, std::uint64_t N>
struct ring_buffer {
    static_assert(N > 0 && (N & (N - 1)) == 0, "N must be a power of two");
    static constexpr std::uint64_t Capacity = N;

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
    static constexpr std::uint64_t mask = N - 1;

    std::array<T, N> data_;
    std::uint64_t begin_ = 0;
    // The bit representing N signifies if the ring buffer is full
    std::uint64_t end_ = 0;

   public:
    inline bool empty() const noexcept {
        return begin_ == end_;
    }

    inline bool full() const noexcept {
        return end_ & N;
    }

    inline std::size_t size() const noexcept {
        // If full, (end_ - begin_) & mask is zero
        // Otherwise, full() is false
        return !full() ? (end_ - begin_) & mask : N;
    }

    inline void push_front(T value) noexcept {
        assert(!full());
        --begin_ &= mask;
        data_[begin_] = value;
        if (begin_ == end_) {
            end_ |= N;
        }
    }

    inline void push_back(T value) noexcept {
        assert(!full());
        data_[end_] = value;
        ++end_ &= mask;
        if (begin_ == end_) {
            end_ |= N;
        }
    }

    // `pos` is relative to begin_
    void insert(std::uint64_t pos, T value) noexcept {
        assert(!full());
        assert(pos <= size());
        if (pos <= size() / 2) {
            --begin_ &= mask;
            for (std::uint64_t i = 0; i < pos; ++i) {
                data_[(begin_ + i) & mask] = std::move(data_[(begin_ + i + 1) & mask]);
            }
            data_[(begin_ + pos) & mask] = value;
        } else {
            for (std::uint64_t i = 0; i < size() - pos; ++i) {
                data_[(end_ - i) & mask] = std::move(data_[(end_ - (i + 1)) & mask]);
            }
            data_[(end_ - (size() - pos)) & mask] = value;
            ++end_ &= mask;
        }
        if (begin_ == end_) {
            end_ |= N;
        }
    }

    inline void pop_front() noexcept {
        assert(!empty());
        ++begin_ &= mask;
        end_ &= mask;
    }

    inline void pop_back() noexcept {
        assert(!empty());
        --end_ &= mask;
    }

    inline void clear() noexcept {
        begin_ = end_ = 0;
    }

    // Relative to `begin_`
    T operator[](std::uint64_t pos) const noexcept {
        assert(pos < size());
        return data_[(begin_ + pos) & mask];
    }

    T& operator[](std::uint64_t pos) noexcept {
        return data_[(begin_ + pos) & mask];
    }

    T front() const noexcept {
        assert(!empty());
        return data_[begin_];
    }

    T& front() noexcept {
        return data_[begin_];
    }

    T back() const noexcept {
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
        return iterator{data_.data(), begin_, size()};
    }

    constexpr const const_iterator end() const noexcept {
        return const_iterator{data_.data(), begin_, size()};
    }

    constexpr const const_iterator cend() const noexcept {
        return const_iterator{data_.data(), begin_, size()};
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

#endif  //! RING_BUFFER_HPP_INCLUDED
