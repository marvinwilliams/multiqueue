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
#include <array>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <new>
#include <type_traits>

namespace multiqueue {

template <typename T, std::size_t LogSize>
class Buffer;

template <typename T>
class BufferIterator {
    friend std::conditional_t<!std::is_const_v<T>, BufferIterator<T const>,
                              void>;  // const iterator is friend for converting constructor

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
    using underlying_type = std::remove_cv_t<T>;
    using underlying_data_type =
        std::conditional_t<std::is_const_v<T>,
                           std::aligned_storage_t<sizeof(underlying_type), alignof(underlying_type)> const,
                           std::aligned_storage_t<sizeof(underlying_type), alignof(underlying_type)>>;

    underlying_data_type* p_;

   private:
    pointer as_value_pointer(underlying_data_type* p) noexcept {
        return std::launder(reinterpret_cast<pointer>(p));
    }

    const_pointer as_value_pointer(underlying_data_type* p) const noexcept {
        return std::launder(reinterpret_cast<const_pointer>(p));
    }

   public:
    constexpr BufferIterator() noexcept : p_{nullptr} {
    }

    constexpr explicit BufferIterator(underlying_data_type* p) noexcept : p_{p} {
    }

    constexpr BufferIterator(BufferIterator const& other) noexcept : p_{other.p_} {
    }

    template <bool EnableConverting = std::is_const_v<T>, typename = std::enable_if_t<EnableConverting>>
    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr BufferIterator(BufferIterator<std::remove_const_t<T>> const& other) noexcept : p_{other.p_} {
    }

    constexpr BufferIterator& operator=(BufferIterator const& other) noexcept {
        p_ = other.p_;
    }

    template <bool EnableConverting = std::is_const_v<T>, typename = std::enable_if_t<EnableConverting>>
    constexpr BufferIterator& operator=(BufferIterator<std::remove_const_t<T>> const& other) noexcept {
        p_ = other.p_;
    }

    reference operator*() noexcept {
        assert(p_);
        return *as_value_pointer(p_);
    }

    const_reference operator*() const noexcept {
        assert(p_);
        return *as_value_pointer(p_);
    }

    pointer operator->() noexcept {
        assert(p_);
        return as_value_pointer(p_);
    }

    const_pointer operator->() const noexcept {
        assert(p_);
        return as_value_pointer(p_);
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

    reference operator[](difference_type n) {
        return *as_value_pointer(p_ + n);
    }

    const_reference operator[](difference_type n) const {
        return *as_value_pointer(p_ + n);
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
   public:
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using reference = value_type&;
    using const_reference = value_type const&;
    using pointer = value_type*;
    using const_pointer = value_type const*;
    using iterator = BufferIterator<T>;
    using const_iterator = BufferIterator<T const>;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    static constexpr size_type Capacity = size_type(1) << LogSize;

   private:
    using underlying_type = std::remove_cv_t<T>;
    using underlying_data_type = std::aligned_storage_t<sizeof(underlying_type), alignof(underlying_type)>;

   private:
    std::array<underlying_data_type, Capacity> data_;
    underlying_data_type* end_;

   private:
    pointer as_value_pointer(underlying_data_type* p) noexcept {
        return std::launder(reinterpret_cast<pointer>(p));
    }

    const_pointer as_value_pointer(underlying_data_type* p) const noexcept {
        return std::launder(reinterpret_cast<const_pointer>(p));
    }

   public:
    Buffer() noexcept : end_{std::begin(data_)} {
    }

    ~Buffer() noexcept {
        clear();
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return end_ == std::begin(data_);
    }

    constexpr bool full() const noexcept {
        return end_ == std::begin(data_) + Capacity;
    }

    constexpr size_type size() const noexcept {
        return static_cast<size_type>(end_ - std::begin(data_));
    }

    static constexpr size_type capacity() noexcept {
        return Capacity;
    }

    void push_back(const_reference value) noexcept(std::is_nothrow_copy_constructible_v<value_type>) {
        assert(!full());
        ::new (static_cast<void*>(end_)) value_type(value);
        ++end_;
    }

    void push_back(value_type&& value) noexcept(std::is_nothrow_move_constructible_v<value_type>) {
        assert(!full());
        ::new (static_cast<void*>(end_)) value_type(std::move(value));
        ++end_;
    }

    iterator insert(const_iterator pos, const_reference value) noexcept(
        std::is_nothrow_copy_constructible_v<value_type>&& std::is_nothrow_move_constructible_v<value_type>&&
            std::is_nothrow_move_assignable_v<value_type>&& std::is_nothrow_copy_assignable_v<value_type>) {
        assert(!full());
        auto it = begin() + (pos - cbegin());
        if (it == end()) {
            ::new (static_cast<void*>(end_)) value_type(value);
        } else {
            // Copy value, as it could exist in the buffer
            auto value_cpy = value;
            ::new (static_cast<void*>(end_)) value_type(std::move(back()));
            std::move_backward(it, end() - 1, end());
            *it = std::move(value_cpy);
        }
        ++end_;
        return it;
    }

    iterator insert(const_iterator pos, value_type&& value) noexcept(
        std::is_nothrow_move_constructible_v<value_type>&& std::is_nothrow_move_assignable_v<value_type>) {
        assert(!full());
        auto it = begin() + (pos - cbegin());
        if (it == end()) {
            ::new (static_cast<void*>(end_)) value_type(std::move(value));
        } else {
            // Copy value, as it could exist in the buffer
            auto value_cpy = std::move(value);
            ::new (static_cast<void*>(end_)) value_type(std::move(back()));
            std::move_backward(it, end() - 1, end());
            *it = std::move(value_cpy);
        }
        ++end_;
        return it;
    }

    iterator erase(const_iterator pos) noexcept {
        iterator it = begin() + (pos - cbegin());
        if (it + 1 != end()) {
            std::move(it + 1, end(), it);
        }
        pop_back();
        return it;
    }

    void pop_back() noexcept {
        assert(!empty());
        --end_;
        as_value_pointer(end_)->~value_type();
    }

    void clear() noexcept {
        std::for_each(begin(), end(), [](value_type& v) { v.~value_type(); });
        end_ = std::begin(data_);
    }

    reference operator[](size_type pos) {
        assert(pos < size());
        return *as_value_pointer(std::begin(data_) + pos);
    }

    const_reference operator[](size_type pos) const {
        assert(pos < size());
        return *as_value_pointer(std::begin(data_) + pos);
    }

    reference front() noexcept {
        assert(!empty());
        return *as_value_pointer(std::begin(data_));
    }

    const_reference front() const {
        assert(!empty());
        return *as_value_pointer(std::begin(data_));
    }

    reference back() noexcept {
        assert(!empty());
        return *as_value_pointer(end_ - 1);
    }

    const_reference back() const noexcept {
        assert(!empty());
        return *as_value_pointer(end_ - 1);
    }

    constexpr iterator begin() noexcept {
        return iterator{std::begin(data_)};
    }

    constexpr const_iterator begin() const noexcept {
        return const_iterator{std::begin(data_)};
    }

    constexpr const_iterator cbegin() const noexcept {
        return const_iterator{std::begin(data_)};
    }

    constexpr iterator end() noexcept {
        return iterator{end_};
    }

    constexpr const_iterator end() const noexcept {
        return const_iterator{end_};
    }

    constexpr const_iterator cend() const noexcept {
        return const_iterator{end_};
    }

    constexpr reverse_iterator rbegin() noexcept {
        return reverse_iterator{end()};
    }

    constexpr const_reverse_iterator rbegin() const noexcept {
        return const_reverse_iterator{cend()};
    }

    constexpr const_reverse_iterator crbegin() const noexcept {
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
