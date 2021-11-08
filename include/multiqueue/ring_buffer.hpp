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
#include <new>
#include <type_traits>

namespace multiqueue {

template <typename T, std::size_t N>
struct RingBuffer;

template <typename T, std::size_t N>
class RingBufferIterator {
    friend RingBuffer<std::decay_t<T>, N>;

    static constexpr std::size_t mask = N - 1;

   protected:
    T* data_;
    std::size_t pos_;

    explicit constexpr RingBufferIterator(T* data, std::size_t pos) noexcept : data_{data}, pos_{pos} {
    }

   public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using reference = T&;
    using pointer = T*;
    using size_type = std::size_t;

    constexpr reference operator*() const noexcept {
        assert(pos_ < N);
        return *(data_ + pos_);
    }

    constexpr pointer operator->() const noexcept {
        assert(pos_ < N);
        return data_ + pos_;
    }

    constexpr RingBufferIterator& operator++() noexcept {
        assert(pos_ < N);
        ++pos_ &= mask;
        return *this;
    }

    constexpr RingBufferIterator operator++(int) noexcept {
        auto tmp = *this;
        operator++();
        return tmp;
    }

    constexpr RingBufferIterator& operator--() noexcept {
        assert(pos_ > 0);
        --pos_;
        return *this;
    }

    constexpr RingBufferIterator operator--(int) noexcept {
        auto tmp = *this;
        operator--();
        return tmp;
    }

    constexpr RingBufferIterator& operator+=(difference_type n) noexcept {
        pos_ = (pos_ + n) & mask;
        return *this;
    }

    constexpr RingBufferIterator operator+(difference_type n) noexcept {
        auto tmp = *this;
        tmp += n;
        return tmp;
    }

    constexpr RingBufferIterator& operator-=(difference_type n) noexcept {
        pos_ = (pos_ - n) & mask;
        return *this;
    }

    constexpr RingBufferIterator operator-(difference_type n) noexcept {
        auto tmp = *this;
        tmp -= n;
        return tmp;
    }

    constexpr reference operator[](difference_type n) noexcept {
        return *(*this + n);
    }

    constexpr difference_type operator-(RingBufferIterator const& other) noexcept {
        return (pos_ - other.pos_) & mask;
    }

    friend bool operator==(RingBufferIterator const& lhs, RingBufferIterator const& rhs) noexcept {
        return lhs.data_ == rhs.data_ && lhs.pos_ == rhs.pos_;
    }

    friend bool operator!=(RingBufferIterator const& lhs, RingBufferIterator const& rhs) noexcept {
        return !(lhs == rhs);
    }
};

template <typename T, std::size_t N>
struct RingBuffer {
    static_assert(N > 0 && (N & (N - 1)) == 0, "N must be a power of two");
    static constexpr std::size_t Capacity = N;

    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using reference = value_type&;
    using const_reference = value_type const&;
    using pointer = value_type*;
    using const_pointer = value_type const*;
    using iterator = RingBufferIterator<T, N>;
    using const_iterator = RingBufferIterator<T const, N>;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

   private:
    static constexpr std::size_t mask = N - 1;

    std::aligned_storage<sizeof(T), alignof(T)> data_[N];
    size_type begin_ = 0;
    // The bit representing N signifies if the ring buffer is full
    size_type end_ = 0;

    size_type get_index(difference_type pos) const noexcept {
        return ((begin_ + pos) & mask);
    }

    void make_hole(size_type pos) {
        assert(!full());
        assert(pos > 0 && pos < size());
        if (pos <= size() / 2) {
            new (&data_[get_index(-1)]) value_type(std::move(data_[start]));
            size_type start = begin_;
            size_type prev = (start - 1) & mask;
            while (start != ((begin_ + pos) & mask)) {
                *std::launder(reinterpret_cast<T*>(&data_[start])) =
                    std::move(*std::launder(reinterpret_cast<T*>(&data_[prev])));
                start = (start + 1) & mask;
            }
            for (size_type i = 0; i < pos; ++i) {
                data_[(begin_ + i - 1) & mask] = std::move(data_[(begin_ + i) & mask]);
            }
            --begin_ &= mask;
        } else {
            for (std::size_t i = 0; i < size() - pos; ++i) {
                data_[(end_ - i) & mask] = std::move(data_[(end_ - (i + 1)) & mask]);
            }
            data_[(end_ - (size() - pos)) & mask] = value;
            ++end_ &= mask;
        }
        if (begin_ == end_) {
            end_ |= N;
        }
    }

   public:
    bool empty() const noexcept {
        return begin_ == end_;
    }

    bool full() const noexcept {
        return end_ & (~mask);
    }

    std::size_t size() const noexcept {
        return full() ? N : (end_ - begin_) & mask;
    }

    void push_front(const_reference value) {
        assert(!full());
        --begin_ &= mask;
        new (&data_[begin_]) T(value);
        if (begin_ == end_) {
            end_ |= N;
        }
    }

    void push_front(value_type&& value) {
        assert(!full());
        --begin_ &= mask;
        new (&data_[begin_]) T(std::move(value));
        if (begin_ == end_) {
            end_ |= N;
        }
    }

    void push_back(T value) {
        assert(!full());
        data_[end_] = value;
        ++end_ &= mask;
        if (begin_ == end_) {
            end_ |= N;
        }
    }

    // `pos` is relative to begin_
    void insert(std::size_t pos, const_reference value) {
        assert(!full());
        assert(pos <= size());
        if (pos <= size() / 2) {
            for (std::size_t i = 0; i < pos; ++i) {
                data_[(begin_ + i - 1) & mask] = std::move(data_[(begin_ + i) & mask]);
            }
            new (data_[(begin_ + pos) & mask]) value_type(value);
            --begin_ &= mask;
        } else {
            for (std::size_t i = 0; i < size() - pos; ++i) {
                data_[(end_ - i) & mask] = std::move(data_[(end_ - (i + 1)) & mask]);
            }
            data_[(end_ - (size() - pos)) & mask] = value;
            ++end_ &= mask;
        }
        if (begin_ == end_) {
            end_ |= N;
        }
    }

    void pop_front() noexcept {
        assert(!empty());
        ++begin_ &= mask;
        end_ &= mask;
    }

    void pop_back() noexcept {
        assert(!empty());
        --end_ &= mask;
    }

    void clear() noexcept {
        begin_ = end_ = 0;
    }

    // Relative to `begin_`
    T operator[](std::size_t pos) const noexcept {
        assert(pos < size());
        return data_[(begin_ + pos) & mask];
    }

    T& operator[](std::size_t pos) noexcept {
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
