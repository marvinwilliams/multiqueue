/**
******************************************************************************
* @file:   buffer.hpp
*
* @author: Marvin Williams
* @date:   2021/03/05 16:57
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef BUFFER_HPP_INCLUDED
#define BUFFER_HPP_INCLUDED

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <new>
#include <type_traits>

namespace multiqueue {

template <typename T, std::size_t LogSize>
struct Buffer;

template <typename T, std::size_t LogSize>
class BufferIterator {
    using buffer_type = Buffer<std::decay_t<T>, LogSize>;
    friend buffer_type;

   public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using reference = value_type&;
    using const_reference = value_type const&;
    using pointer = value_type*;
    using const_pointer = value_type const*;

   protected:
    pointer p_;

    explicit constexpr BufferIterator(pointer p) noexcept : p_{p} {
    }

   public:
    constexpr BufferIterator() noexcept : p_{nullptr} {
    }

    explicit constexpr BufferIterator(BufferIterator const& other) noexcept : p_{other.p_} {
    }

    BufferIterator& operator=(BufferIterator const& other) noexcept {
        p_ = other.p_;
    }

    reference operator*() const {
        assert(p_);
        return *std::launder(p_);
    }

    pointer operator->() const {
        assert(p_);
        return std::launder(p_);
    }

    constexpr BufferIterator& operator++() noexcept {
        ++p_;
        return *this;
    }

    constexpr BufferIterator operator++(int) noexcept {
        auto tmp = *this;
        operator++();
        return tmp;
    }

    constexpr BufferIterator& operator--() noexcept {
        --p_;
        return *this;
    }

    constexpr BufferIterator operator--(int) noexcept {
        auto tmp = *this;
        operator--();
        return tmp;
    }

    constexpr BufferIterator& operator+=(difference_type n) noexcept {
        p_ += n;
        return *this;
    }

    constexpr BufferIterator operator+(difference_type n) noexcept {
        auto tmp = *this;
        tmp += n;
        return tmp;
    }

    constexpr BufferIterator& operator-=(difference_type n) noexcept {
        p_ -= n;
        return *this;
    }

    constexpr BufferIterator operator-(difference_type n) noexcept {
        auto tmp = *this;
        tmp -= n;
        return tmp;
    }

    reference operator[](difference_type n) {
        return *std::launder(p_ + n);
    }

    const_reference operator[](difference_type n) const {
        return *std::launder(p_ + n);
    }

    constexpr difference_type operator-(BufferIterator const& other) noexcept {
        return p_ - other.p_;
    }

    friend constexpr bool operator==(BufferIterator const& lhs, BufferIterator const& rhs) noexcept {
        return lhs.p_ == rhs.p_;
    }

    friend constexpr bool operator!=(BufferIterator const& lhs, BufferIterator const& rhs) noexcept {
        return !(lhs == rhs);
    }
};

template <typename T, std::size_t LogSize>
struct Buffer {
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using reference = value_type&;
    using const_reference = value_type const&;
    using pointer = value_type*;
    using const_pointer = value_type const*;
    using iterator = BufferIterator<T, LogSize>;
    using const_iterator = BufferIterator<T const, LogSize>;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    static constexpr size_type Capacity = size_type(1) << LogSize;

   private:
    std::aligned_storage_t<sizeof(T), alignof(T)> data_[Capacity];
    pointer end_;

   public:
    Buffer() noexcept : end_{&data_[0]} {
    }

    ~Buffer() noexcept {
        clear();
    }
    [[nodiscard]] constexpr bool empty() const noexcept {
        return end_ == &data_[0];
    }

    constexpr bool full() const noexcept {
        return end_ == &data_[Capacity];
    }

    constexpr std::size_t size() const noexcept {
        return end_ - &data_[0];
    }

    void push_back(const_reference value) {
        assert(!full());
        new (end_) value_type(value);
        ++end_;
    }

    void push_back(value_type&& value) {
        assert(!full());
        new (end_) value_type(std::move(value));
        ++end_;
    }

    void insert(iterator pos, const_reference value) {
        assert(!full());
        if (pos == begin()) {
            begin_ = into_range(begin_ - 1);
            new (&data_[begin_]) value_type(value);
        } else if (pos == end()) {
            new (&data_[into_range(begin_ + size_)]) value_type(value);
        } else {
            // Copy value, as it could exist in the buffer
            auto value_cpy = value;
            // Determine which side of the ring buffer has fewer elements
            if (end() - pos <= pos - begin()) {
                new (&data_[into_range(begin_ + size_)]) value_type(std::move(back()));
                std::move_backward(pos, end() - 1, end());
            } else {
                new (&data_[into_range(begin_ - 1)]) value_type(std::move(front()));
                std::move(begin() + 1, pos, begin());
            }
            *pos = std::move(value_cpy);
        }
        ++size_;
    }

    void pop_back() {
        assert(!empty());
        back().~value_type();
        --end_;
    }

    void clear() noexcept {
        std::for_each(begin(), end(), [](value_type& v) { v.~value_type(); });
        end_ = &data_[0];
    }

    const_reference operator[](size_type pos) const {
        assert(pos < size());
        return *std::launder(reinterpret_cast<pointer>(&data_[pos]));
    }

    reference operator[](size_type pos) {
        assert(pos < size_);
        return *std::launder(reinterpret_cast<pointer>(&data_[pos]));
    }

    const_reference front() const {
        assert(!empty());
        return *std::launder(reinterpret_cast<pointer>(&data_[0]));
    }

    reference front() {
        assert(!empty());
        return *std::launder(reinterpret_cast<pointer>(&data_[0]));
    }

    const_reference back() const {
        assert(!empty());
        return *std::launder(reinterpret_cast<pointer>(end_ - 1));
    }

    reference back() {
        assert(!empty());
        return *std::launder(reinterpret_cast<pointer>(end_ - 1));
    }

    constexpr iterator begin() noexcept {
        return iterator{reinterpret_cast<pointer>(&data_[0])};
    }

    constexpr const_iterator begin() const noexcept {
        return const_iterator{reinterpret_cast<pointer>(&data_[0])};
    }

    constexpr const_iterator cbegin() const noexcept {
        return const_iterator{reinterpret_cast<pointer>(&data_[0])};
    }

    constexpr iterator end() noexcept {
        return iterator{reinterpret_cast<pointer>(&data_[Capacity])};
    }

    constexpr const_iterator end() const noexcept {
        return const_iterator{reinterpret_cast<pointer>(&data_[Capacity])};
    }

    constexpr const const_iterator cend() const noexcept {
        return const_iterator{reinterpret_cast<pointer>(&data_[Capacity])};
    }

    constexpr reverse_iterator rbegin() noexcept {
        return reverse_iterator{end()};
    }

    constexpr const_reverse_iterator rbegin() const noexcept {
        return const_reverse_iterator{cend()};
    }

    constexpr const const_reverse_iterator crbegin() const noexcept {
        return const_reverse_iterator{cend()};
    }

    constexpr reverse_iterator rend() noexcept {
        return reverse_iterator{begin()};
    }

    constexpr const_reverse_iterator rend() const noexcept {
        return const_reverse_iterator{cbegin()};
    }

    constexpr const_reverse_iterator crend() const noexcept {
        return const_reverse_iterator{cbegin()};
    }
};

}  // namespace multiqueue

#endif  //! BUFFER_HPP_INCLUDED
