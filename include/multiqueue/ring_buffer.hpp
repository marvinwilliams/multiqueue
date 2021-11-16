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

template <typename T, std::size_t LogSize>
struct RingBuffer;

template <typename T, std::size_t LogSize, bool IsConst>
class RingBufferIterator {
    friend class RingBufferIterator<T, LogSize, true>;  // const iterator is friend for converting constructor

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
    using ring_buffer_ptr_t = std::conditional_t<IsConst, RingBuffer<T, LogSize> const*, RingBuffer<T, LogSize>*>;

    ring_buffer_ptr_t ring_buffer_;
    size_type pos_;

   public:
    constexpr RingBufferIterator() noexcept : ring_buffer_{nullptr}, pos_{0} {
    }

    explicit constexpr RingBufferIterator(ring_buffer_ptr_t ring_buffer, size_type pos) noexcept
        : ring_buffer_{ring_buffer}, pos_{pos} {
    }

    constexpr RingBufferIterator(RingBufferIterator const& other) noexcept
        : ring_buffer_{other.ring_buffer_}, pos_{other.pos_} {
    }

    template <bool EnableConverting = IsConst, typename = std::enable_if_t<EnableConverting>>
    constexpr RingBufferIterator(RingBufferIterator<T, LogSize, false> const& other) noexcept
        : ring_buffer_{other.ring_buffer_}, pos_{other.pos_} {
    }

    RingBufferIterator& operator=(RingBufferIterator const& other) noexcept {
        ring_buffer_ = other.ring_buffer_;
        pos_ = other.pos_;
    }

    template <bool EnableConverting = IsConst, typename = std::enable_if_t<EnableConverting>>
    constexpr RingBufferIterator& operator=(RingBufferIterator<T, LogSize, false> const& other) noexcept {
        ring_buffer_ = other.ring_buffer_;
        pos_ = other.pos_;
    }

    const_reference operator*() const {
        assert(ring_buffer_);
        return (*ring_buffer_)[pos_];
    }

    const_pointer operator->() const {
        assert(ring_buffer_);
        return &((*ring_buffer_)[pos_]);
    }

    reference operator*() {
        assert(ring_buffer_);
        return (*ring_buffer_)[pos_];
    }

    pointer operator->() {
        assert(ring_buffer_);
        return &((*ring_buffer_)[pos_]);
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
        pos_ += n;
        return *this;
    }

    constexpr RingBufferIterator operator+(difference_type n) const noexcept {
        auto tmp = *this;
        tmp += n;
        return tmp;
    }

    constexpr RingBufferIterator& operator-=(difference_type n) noexcept {
        assert(pos_ >= n);
        pos_ -= n;
        return *this;
    }

    constexpr RingBufferIterator operator-(difference_type n) const noexcept {
        auto tmp = *this;
        tmp -= n;
        return tmp;
    }

    const_reference operator[](size_type n) const {
        assert(ring_buffer_);
        return (*ring_buffer_)[pos_ + n];
    }

    reference operator[](size_type n) {
        assert(ring_buffer_);
        return (*ring_buffer_)[pos_ + n];
    }

    friend constexpr difference_type operator-(RingBufferIterator const& lhs, RingBufferIterator const& rhs) noexcept {
        assert(lhs.pos_ >= rhs.pos_);
        return lhs.pos_ - rhs.pos_;
    }

    friend constexpr bool operator==(RingBufferIterator const& lhs, RingBufferIterator const& rhs) noexcept {
        return lhs.pos_ == rhs.pos_;
    }

    friend constexpr bool operator!=(RingBufferIterator const& lhs, RingBufferIterator const& rhs) noexcept {
        return !(lhs == rhs);
    }
};

template <typename T, std::size_t LogSize>
class RingBuffer {
    static_assert(std::is_same_v<T, std::remove_cv_t<T>>, "value type must be non-const, non-volatile");

   public:
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using reference = value_type&;
    using const_reference = value_type const&;
    using pointer = value_type*;
    using const_pointer = value_type const*;
    using iterator = RingBufferIterator<T, LogSize, false>;
    using const_iterator = RingBufferIterator<T, LogSize, true>;
    using reverse_iterator = std::reverse_iterator<iterator>;
    using const_reverse_iterator = std::reverse_iterator<const_iterator>;

   private:
    static constexpr size_type Capacity = size_type(1) << LogSize;
    static constexpr size_type capacity_mask = Capacity - 1;

    std::aligned_storage_t<sizeof(T), alignof(T)> data_[Capacity];
    size_type begin_;
    size_type size_;

    static constexpr size_type into_range(size_type pos) noexcept {
        return pos & capacity_mask;
    }

    // Pos is absolute
    pointer as_value_pointer(size_type pos) noexcept {
        return std::launder(reinterpret_cast<pointer>(&data_[pos]));
    }

    const_pointer as_value_pointer(size_type pos) const noexcept {
        return std::launder(reinterpret_cast<pointer>(&data_[pos]));
    }

   public:
    RingBuffer() noexcept : begin_{0}, size_{0} {
    }

    ~RingBuffer() noexcept {
        clear();
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return size_ == 0;
    }

    constexpr bool full() const noexcept {
        return size_ == Capacity;
    }

    constexpr size_type size() const noexcept {
        return size_;
    }

    static constexpr size_type capacity() noexcept {
        return Capacity;
    }

    void push_front(const_reference value) {
        assert(!full());
        begin_ = into_range(begin_ - 1);
        new (&data_[begin_]) value_type(value);
        ++size_;
    }

    void push_front(value_type&& value) {
        assert(!full());
        begin_ = into_range(begin_ - 1);
        new (&data_[begin_]) value_type(std::move(value));
        ++size_;
    }

    void push_back(const_reference value) {
        assert(!full());
        new (&data_[into_range(begin_ + size_)]) value_type(value);
        ++size_;
    }

    void push_back(value_type&& value) {
        assert(!full());
        new (&data_[into_range(begin_ + size_)]) value_type(std::move(value));
        ++size_;
    }

    iterator insert(const_iterator pos, const_reference value) {
        assert(!full());
        auto it = begin() + (pos - cbegin());
        if (it == begin()) {
            begin_ = into_range(begin_ - 1);
            new (&data_[begin_]) value_type(value);
        } else if (it == end()) {
            new (&data_[into_range(begin_ + size_)]) value_type(value);
        } else {
            // Copy value, as it could exist in the buffer
            auto value_cpy = value;
            // Determine which side of the ring buffer has fewer elements
            if (end() - it <= it - begin()) {
                new (&data_[into_range(begin_ + size_)]) value_type(std::move(back()));
                std::move_backward(it, end() - 1, end());
            } else {
                new (&data_[into_range(begin_ - 1)]) value_type(std::move(front()));
                std::move(begin() + 1, it, begin());
            }
            *it = std::move(value_cpy);
        }
        ++size_;
        return it;
    }

    iterator insert(const_iterator pos, value_type&& value) {
        assert(!full());
        auto it = begin() + (pos - cbegin());
        if (it == begin()) {
            begin_ = into_range(begin_ - 1);
            new (&data_[begin_]) value_type(std::move(value));
        } else if (it == end()) {
            new (&data_[into_range(begin_ + size_)]) value_type(std::move(value));
        } else {
            // Copy value, as it could exist in the buffer
            auto value_cpy = std::move(value);
            // Determine which side of the ring buffer has fewer elements
            if (end() - it <= it - begin()) {
                new (&data_[into_range(begin_ + size_)]) value_type(std::move(back()));
                std::move_backward(it, end() - 1, end());
            } else {
                new (&data_[into_range(begin_ - 1)]) value_type(std::move(front()));
                std::move(begin() + 1, it, begin());
            }
            *it = std::move(value_cpy);
        }
        ++size_;
        return it;
    }

    iterator erase(const_iterator pos) {
        if (pos == cbegin()) {
            pop_front();
            return begin();
        } else if (pos == cend()) {
            pop_back();
            return end();
        } else {
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
    }

    void pop_front() {
        assert(!empty());
        as_value_pointer(begin_)->~value_type();
        begin_ = into_range(begin_ + 1);
        --size_;
    }

    void pop_back() {
        assert(!empty());
        as_value_pointer(into_range(begin_ + size_ - 1))->~value_type();
        --size_;
    }

    void clear() noexcept {
        std::for_each(begin(), end(), [](value_type& v) { v.~value_type(); });
        begin_ = size_ = 0;
    }

    const_reference operator[](size_type pos) const {
        assert(pos < size_);
        return *as_value_pointer(into_range(begin_ + pos));
    }

    reference operator[](size_type pos) {
        assert(pos < size_);
        return *as_value_pointer(into_range(begin_ + pos));
    }

    const_reference front() const {
        assert(!empty());
        return *as_value_pointer(begin_);
    }

    reference front() {
        assert(!empty());
        return *as_value_pointer(begin_);
    }

    const_reference back() const {
        assert(!empty());
        return *as_value_pointer(into_range(begin_ + size_ - 1));
    }

    reference back() {
        assert(!empty());
        return *as_value_pointer(into_range(begin_ + size_ - 1));
    }

    constexpr iterator begin() noexcept {
        return iterator{this, size_type(0)};
    }

    constexpr const_iterator begin() const noexcept {
        return const_iterator{this, 0};
    }

    constexpr const_iterator cbegin() const noexcept {
        return const_iterator{this, 0};
    }

    constexpr iterator end() noexcept {
        return iterator{this, size_};
    }

    constexpr const_iterator end() const noexcept {
        return const_iterator{this, size_};
    }

    constexpr const const_iterator cend() const noexcept {
        return const_iterator{this, size_};
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

#endif  //! RING_BUFFER_HPP_INCLUDED
