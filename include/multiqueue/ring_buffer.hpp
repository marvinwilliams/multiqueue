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

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <new>
#include <type_traits>

namespace multiqueue {

template <typename T, std::size_t Size>
class RingBuffer;

template <typename T, std::size_t Size>
class RingBufferIterator {
    friend std::conditional_t<!std::is_const_v<T>, RingBufferIterator<T const, Size>,
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
    static constexpr size_type Capacity = Size;
    static constexpr size_type capacity_mask = Capacity - 1;

    using underlying_type = std::remove_cv_t<T>;
    using underlying_data_type =
        std::conditional_t<std::is_const_v<T>,
                           std::aligned_storage_t<sizeof(underlying_type), alignof(underlying_type)> const,
                           std::aligned_storage_t<sizeof(underlying_type), alignof(underlying_type)>>;

    underlying_data_type* begin_;
    size_type pos_;

   protected:
    static constexpr size_type into_range(size_type pos) noexcept {
        return pos & capacity_mask;
    }

    // Pos is absolute
    pointer as_value_pointer(size_type pos) noexcept {
        return std::launder(reinterpret_cast<pointer>(begin_ + pos));
    }

    const_pointer as_value_pointer(size_type pos) const noexcept {
        return std::launder(reinterpret_cast<const_pointer>(begin_ + pos));
    }

   public:
    constexpr RingBufferIterator() noexcept : begin_{nullptr}, pos_{0} {
    }

    explicit constexpr RingBufferIterator(underlying_data_type* begin, size_type pos) noexcept
        : begin_{begin}, pos_{pos} {
    }

    constexpr RingBufferIterator(RingBufferIterator const& other) noexcept : begin_{other.begin_}, pos_{other.pos_} {
    }

    template <bool EnableConverting = std::is_const_v<T>, typename = std::enable_if_t<EnableConverting>>
    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr RingBufferIterator(RingBufferIterator<std::remove_const_t<T>, Size> const& other) noexcept
        : begin_{other.begin_}, pos_{other.pos_} {
    }

    constexpr RingBufferIterator& operator=(RingBufferIterator const& other) noexcept {
        begin_ = other.begin_;
        pos_ = other.pos_;
    }

    template <bool EnableConverting = std::is_const_v<T>, typename = std::enable_if_t<EnableConverting>>
    constexpr RingBufferIterator& operator=(RingBufferIterator<std::remove_const_t<T>, Size> const& other) noexcept {
        begin_ = other.begin_;
        pos_ = other.pos_;
    }

    reference operator*() noexcept {
        assert(begin_);
        return *as_value_pointer(into_range(pos_));
    }

    const_reference operator*() const noexcept {
        assert(begin_);
        return *as_value_pointer(into_range(pos_));
    }

    pointer operator->() noexcept {
        assert(begin_);
        return as_value_pointer(into_range(pos_));
    }

    const_pointer operator->() const noexcept {
        assert(begin_);
        return as_value_pointer(into_range(pos_));
    }

    constexpr RingBufferIterator& operator++() noexcept {
        ++pos_;
        return *this;
    }

    constexpr RingBufferIterator operator++(int) noexcept {
        auto tmp = *this;
        operator++();
        return tmp;
    }

    constexpr RingBufferIterator& operator--() noexcept {
        --pos_;
        return *this;
    }

    constexpr RingBufferIterator operator--(int) noexcept {
        auto tmp = *this;
        operator--();
        return tmp;
    }

    constexpr RingBufferIterator& operator+=(difference_type n) noexcept {
        pos_ += static_cast<size_type>(n);
        return *this;
    }

    constexpr RingBufferIterator operator+(difference_type n) const noexcept {
        auto tmp = *this;
        tmp += n;
        return tmp;
    }

    constexpr RingBufferIterator& operator-=(difference_type n) noexcept {
        pos_ -= static_cast<size_type>(n);
        return *this;
    }

    constexpr RingBufferIterator operator-(difference_type n) const noexcept {
        auto tmp = *this;
        tmp -= n;
        return tmp;
    }

    reference operator[](size_type n) noexcept {
        assert(begin_);
        return *as_value_pointer(begin_ + into_range(pos_ + n));
    }

    const_reference operator[](size_type n) const noexcept {
        assert(begin_);
        return *as_value_pointer(begin_ + into_range(pos_ + n));
    }

    friend constexpr difference_type operator-(RingBufferIterator const& lhs, RingBufferIterator const& rhs) noexcept {
        return static_cast<difference_type>(lhs.pos_ - rhs.pos_);
    }

    friend constexpr bool operator==(RingBufferIterator const& lhs, RingBufferIterator const& rhs) noexcept {
        return lhs.pos_ == rhs.pos_;
    }

    friend constexpr bool operator!=(RingBufferIterator const& lhs, RingBufferIterator const& rhs) noexcept {
        return !(lhs == rhs);
    }
};

template <typename T, std::size_t Size>
class RingBuffer {
   public:
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using reference = value_type&;
    using const_reference = value_type const&;
    using pointer = value_type*;
    using const_pointer = value_type const*;
    using iterator = RingBufferIterator<T, Size>;
    using const_iterator = RingBufferIterator<T const, Size>;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

    static constexpr size_type Capacity = Size;

   private:
    static constexpr size_type capacity_mask = Capacity - 1;

   private:
    std::aligned_storage_t<sizeof(T), alignof(T)> data_[Capacity];
    size_type begin_;
    size_type end_;

    static constexpr size_type into_range(size_type pos) noexcept {
        return pos & capacity_mask;
    }

    // Pos is absolute
    pointer as_value_pointer(size_type pos) noexcept {
        return std::launder(reinterpret_cast<pointer>(std::begin(data_) + pos));
    }

    const_pointer as_value_pointer(size_type pos) const noexcept {
        return std::launder(reinterpret_cast<const_pointer>(std::begin(data_) + pos));
    }

   public:
    RingBuffer() noexcept : begin_{0}, end_{0} {
    }

    ~RingBuffer() noexcept {
        clear();
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return begin_ == end_;
    }

    constexpr bool full() const noexcept {
        return end_ - begin_ == Capacity;
    }

    constexpr size_type size() const noexcept {
        return end_ - begin_;
    }

    static constexpr size_type capacity() noexcept {
        return Capacity;
    }

    void push_front(const_reference value) noexcept(std::is_nothrow_copy_constructible_v<value_type>) {
        assert(!full());
        --begin_;
        ::new (static_cast<void*>(std::addressof(data_[into_range(begin_)]))) value_type(value);
    }

    void push_front(value_type&& value) noexcept(std::is_nothrow_move_constructible_v<value_type>) {
        assert(!full());
        --begin_;
        ::new (static_cast<void*>(std::addressof(data_[into_range(begin_)]))) value_type(std::move(value));
    }

    void push_back(const_reference value) noexcept(std::is_nothrow_copy_constructible_v<value_type>) {
        assert(!full());
        ::new (static_cast<void*>(std::addressof(data_[into_range(end_)]))) value_type(value);
        ++end_;
    }

    void push_back(value_type&& value) noexcept(std::is_nothrow_move_constructible_v<value_type>) {
        assert(!full());
        ::new (static_cast<void*>(std::addressof(data_[into_range(end_)]))) value_type(std::move(value));
        ++end_;
    }

    iterator insert(const_iterator pos, const_reference value) noexcept(
        std::is_nothrow_copy_constructible_v<value_type>&& std::is_nothrow_move_constructible_v<value_type>&&
            std::is_nothrow_move_assignable_v<value_type>&& std::is_nothrow_copy_assignable_v<value_type>) {
        assert(!full());
        if (pos == cbegin()) {
            --begin_;
            ::new (static_cast<void*>(std::addressof(data_[into_range(begin_)]))) value_type(value);
            return begin();
        }
        if (pos == cend()) {
            ::new (static_cast<void*>(std::addressof(data_[into_range(end_)]))) value_type(value);
            ++end_;
            return end() - 1;
        }
        auto it = begin() + (pos - cbegin());
        // Copy value, as it could exist in the buffer
        auto value_cpy = value;
        ::new (static_cast<void*>(std::addressof(data_[into_range(end_)]))) value_type(std::move(back()));
        std::move_backward(it, end() - 1, end());
        ++end_;
        *it = std::move(value_cpy);
        return it;
    }

    iterator insert(const_iterator pos, value_type&& value) noexcept(
        std::is_nothrow_move_constructible_v<value_type>&& std::is_nothrow_move_assignable_v<value_type>) {
        assert(!full());
        if (pos == cbegin()) {
            --begin_;
            ::new (static_cast<void*>(std::addressof(data_[into_range(begin_)]))) value_type(std::move(value));
            return begin();
        }
        if (pos == cend()) {
            ::new (static_cast<void*>(std::addressof(data_[into_range(end_)]))) value_type(std::move(value));
            ++end_;
            return end() - 1;
        }
        auto it = begin() + (pos - cbegin());
        // Copy value, as it could exist in the buffer
        auto value_cpy = std::move(value);
        // Determine which side of the ring buffer has fewer elements
        if (end() - it <= it - begin()) {
            ::new (static_cast<void*>(std::addressof(data_[into_range(end_)]))) value_type(std::move(back()));
            std::move_backward(it, end() - 1, end());
            ++end_;
        } else {
            ::new (static_cast<void*>(std::addressof(data_[into_range(begin_ - 1)]))) value_type(std::move(front()));
            std::move(begin() + 1, it, begin());
            --begin_;
            --it;
        }
        *it = std::move(value_cpy);
        return it;
    }

    iterator erase(const_iterator pos) noexcept {
        assert(pos != cend());
        if (pos == cbegin()) {
            pop_front();
            return begin();
        }
        if (pos == cend() - 1) {
            pop_back();
            return end();
        }
        iterator it = begin() + (pos - cbegin());
        if (end() - it <= it - begin()) {
            std::move(it + 1, end(), it);
            pop_back();
        } else {
            std::move_backward(begin(), it, it + 1);
            pop_front();
        }
        return it;
    }

    void pop_front() noexcept {
        assert(!empty());
        as_value_pointer(into_range(begin_))->~value_type();
        ++begin_;
    }

    void pop_back() noexcept {
        assert(!empty());
        --end_;
        as_value_pointer(into_range(end_))->~value_type();
    }

    void clear() noexcept {
        std::for_each(begin(), end(), [](value_type& v) { v.~value_type(); });
        end_ = begin_;
    }

    reference operator[](size_type pos) noexcept {
        assert(pos < size());
        return *as_value_pointer(into_range(begin_ + pos));
    }

    const_reference operator[](size_type pos) const noexcept {
        assert(pos < size());
        return *as_value_pointer(into_range(begin_ + pos));
    }

    reference front() noexcept {
        assert(!empty());
        return *as_value_pointer(into_range(begin_));
    }

    const_reference front() const noexcept {
        assert(!empty());
        return *as_value_pointer(into_range(begin_));
    }

    reference back() noexcept {
        assert(!empty());
        return *as_value_pointer(into_range(end_ - 1));
    }

    const_reference back() const noexcept {
        assert(!empty());
        return *as_value_pointer(into_range(end_ - 1));
    }

    constexpr iterator begin() noexcept {
        return iterator{std::begin(data_), begin_};
    }

    constexpr const_iterator begin() const noexcept {
        return const_iterator{std::begin(data_), begin_};
    }

    constexpr const_iterator cbegin() const noexcept {
        return const_iterator{std::begin(data_), begin_};
    }

    constexpr iterator end() noexcept {
        return iterator{std::begin(data_), end_};
    }

    constexpr const_iterator end() const noexcept {
        return const_iterator{std::begin(data_), end_};
    }

    constexpr const_iterator cend() const noexcept {
        return const_iterator{std::begin(data_), end_};
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

#endif  //! RING_BUFFER_HPP_INCLUDED
