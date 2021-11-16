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
class Buffer;

template <typename T, std::size_t LogSize, bool IsConst>
class BufferIterator {
    friend class Buffer<T, LogSize>;
    friend class BufferIterator<T, LogSize, true>;  // const iterator is friend for converting constructor

   public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = std::conditional_t<IsConst, T const, T>;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using reference = value_type&;
    using const_reference = T const&;
    using pointer = value_type*;
    using const_pointer = T const*;

   protected:
    pointer p_;

   public:
    constexpr BufferIterator() noexcept : p_{nullptr} {
    }

    explicit constexpr BufferIterator(pointer p) noexcept : p_{p} {
    }

    constexpr BufferIterator(BufferIterator const& other) noexcept : p_{other.p_} {
    }

    template <bool EnableConverting = IsConst, typename = std::enable_if_t<EnableConverting>>
    constexpr BufferIterator(BufferIterator<T, LogSize, false> const& other) noexcept : p_{other.p_} {
    }

    constexpr BufferIterator& operator=(BufferIterator const& other) noexcept {
        p_ = other.p_;
    }

    template <bool EnableConverting = IsConst, typename = std::enable_if_t<EnableConverting>>
    constexpr BufferIterator& operator=(BufferIterator<T, LogSize, false> const& other) noexcept {
        p_ = other.p_;
    }

    const_reference operator*() const {
        assert(p_);
        return *std::launder(p_);
    }

    const_pointer operator->() const {
        assert(p_);
        return std::launder(p_);
    }

    reference operator*() {
        assert(p_);
        return *std::launder(p_);
    }

    pointer operator->() {
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

    constexpr BufferIterator operator+(difference_type n) const noexcept {
        auto tmp = *this;
        tmp += n;
        return tmp;
    }

    constexpr BufferIterator& operator-=(difference_type n) noexcept {
        p_ -= n;
        return *this;
    }

    constexpr BufferIterator operator-(difference_type n) const noexcept {
        auto tmp = *this;
        tmp -= n;
        return tmp;
    }

    const_reference operator[](difference_type n) const {
        return *std::launder(p_ + n);
    }

    reference operator[](difference_type n) {
        return *std::launder(p_ + n);
    }

    friend constexpr difference_type operator-(BufferIterator const& lhs, BufferIterator const& rhs) noexcept {
        return lhs.p_ - rhs.p_;
    }

    friend constexpr bool operator==(BufferIterator const& lhs, BufferIterator const& rhs) noexcept {
        return lhs.p_ == rhs.p_;
    }

    friend constexpr bool operator!=(BufferIterator const& lhs, BufferIterator const& rhs) noexcept {
        return !(lhs == rhs);
    }
};

template <typename T, std::size_t LogSize>
class Buffer {
    static_assert(std::is_same_v<T, std::remove_cv_t<T>>, "value type must be non-const, non-volatile");

   public:
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using reference = value_type&;
    using const_reference = value_type const&;
    using pointer = value_type*;
    using const_pointer = value_type const*;
    using iterator = BufferIterator<T, LogSize, false>;
    using const_iterator = BufferIterator<T, LogSize, true>;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    static constexpr size_type Capacity = size_type(1) << LogSize;

   private:
    std::aligned_storage_t<sizeof(T), alignof(T)> data_[Capacity];
    pointer end_;

   private:
    // Returns a pointer to the data. If this pointer is to dereferenced, it has to be laundered!
    const_pointer to_pointer(size_type pos) const noexcept {
        return reinterpret_cast<const_pointer>(&data_[pos]);
    }

    pointer to_pointer(size_type pos) noexcept {
        return reinterpret_cast<pointer>(&data_[pos]);
    }

   public:
    Buffer() noexcept : end_{to_pointer(0)} {
    }

    ~Buffer() noexcept {
        clear();
    }
    [[nodiscard]] constexpr bool empty() const noexcept {
        return end_ == to_pointer(0);
    }

    constexpr bool full() const noexcept {
        return end_ == to_pointer(Capacity);
    }

    constexpr size_type size() const noexcept {
        return static_cast<size_type>(end_ - to_pointer(0));
    }

    static constexpr size_type capacity() noexcept {
        return Capacity;
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

    iterator insert(const_iterator pos, const_reference value) {
        assert(!full());
        auto it = begin() + (pos - cbegin());
        if (it == end()) {
            new (end_) value_type(value);
        } else {
            // Copy value, as it could exist in the buffer
            auto value_cpy = value;
            // Determine which side of the ring buffer has fewer elements
            new (end_) value_type(std::move(back()));
            std::move_backward(it, end() - 1, end());
            *it = std::move(value_cpy);
        }
        ++end_;
        return it;
    }

    iterator insert(const_iterator pos, value_type&& value) {
        assert(!full());
        auto it = begin() + (pos - cbegin());
        if (it == end()) {
            new (end_) value_type(std::move(value));
        } else {
            // Copy value, as it could exist in the buffer
            auto value_cpy = std::move(value);
            // Determine which side of the ring buffer has fewer elements
            new (end_) value_type(std::move(back()));
            std::move_backward(it, end() - 1, end());
            *it = std::move(value_cpy);
        }
        ++end_;
        return it;
    }

    iterator erase(const_iterator pos) {
        iterator it = begin() + (pos - cbegin());
        if (it + 1 != end()) {
            std::move(it + 1, end(), it);
        }
        pop_back();
        return it;
    }

    void pop_back() {
        assert(!empty());
        --end_;
        std::launder(end_)->~value_type();
    }

    void clear() noexcept {
        std::for_each(begin(), end(), [](value_type& v) { v.~value_type(); });
        end_ = to_pointer(0);
    }

    const_reference operator[](size_type pos) const {
        assert(pos < size());
        return *std::launder(to_pointer(pos));
    }

    reference operator[](size_type pos) {
        assert(pos < size_);
        return *std::launder(to_pointer(pos));
    }

    const_reference front() const {
        assert(!empty());
        return *std::launder(to_pointer(0));
    }

    reference front() {
        assert(!empty());
        return *std::launder(to_pointer(0));
    }

    const_reference back() const {
        assert(!empty());
        return *std::launder(end_ - 1);
    }

    reference back() {
        assert(!empty());
        return *std::launder(end_ - 1);
    }

    constexpr iterator begin() noexcept {
        return iterator{to_pointer(0)};
    }

    constexpr const_iterator begin() const noexcept {
        return const_iterator{to_pointer(0)};
    }

    constexpr const_iterator cbegin() const noexcept {
        return const_iterator{to_pointer(0)};
    }

    constexpr iterator end() noexcept {
        return iterator{end_};
    }

    constexpr const_iterator end() const noexcept {
        return const_iterator{end_};
    }

    constexpr const const_iterator cend() const noexcept {
        return const_iterator{end_};
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
