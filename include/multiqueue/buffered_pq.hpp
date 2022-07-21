/**
******************************************************************************
* @file:   buffered_pq.hpp
*
* @author: Marvin Williams
* @date:   2021/09/14 17:54
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef BUFFERED_PQ_HPP_INCLUDED
#define BUFFERED_PQ_HPP_INCLUDED

#include "multiqueue/ring_buffer.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

namespace multiqueue {

template <std::size_t InsertionBufferSize, std::size_t DeletionBufferSize, typename PriorityQueue>
class BufferedPQ : private PriorityQueue {
   private:
    using base_type = PriorityQueue;

   public:
    using value_type = typename base_type::value_type;
    using value_compare = typename base_type::value_compare;
    using reference = typename base_type::reference;
    using const_reference = typename base_type::const_reference;
    using size_type = std::size_t;

   private:
    using insertion_buffer_type = std::array<value_type, InsertionBufferSize>;
    using deletion_buffer_type = std::array<value_type, DeletionBufferSize>;

    size_type ins_buf_size_ = 0;
    insertion_buffer_type insertion_buffer_;
    ring_buffer<value_type, DeletionBufferSize> deletion_buffer_;

   private:
    void flush_insertion_buffer() {
        for (size_type i = 0; i != ins_buf_size_; ++i) {
            base_type::push(std::move(insertion_buffer_[i]));
        }
        ins_buf_size_ = 0;
    }

    void refill_deletion_buffer() {
        assert(deletion_buffer_.empty());
        // We flush the insertion buffer into the heap, then refill the
        // deletion buffer from the heap. We could also merge the insertion
        // buffer and heap into the deletion buffer
        flush_insertion_buffer();
        size_type num_refill = std::min(DeletionBufferSize, base_type::size());
        while (num_refill != 0) {
            deletion_buffer_.push_back(base_type::c.front());
            base_type::pop();
        }
    }

   public:
    explicit BufferedPQ(value_compare compare = value_compare()) : base_type(compare) {
        base_type::c.reserve(1'000'000);
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<base_type, Alloc>>>
    explicit BufferedPQ(value_compare const& compare, Alloc const& alloc) : base_type(compare, alloc) {
        base_type::c.reserve(1'000'000);
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<base_type, Alloc>>>
    explicit BufferedPQ(Alloc const& alloc) : base_type(alloc) {
        base_type::c.reserve(1'000'000);
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        assert(deletion_buffer_.empty() || (ins_buf_size_ == 0 && base_type::empty()));
        return deletion_buffer_.empty();
    }

    constexpr size_type size() const noexcept {
        return ins_buf_size_ + deletion_buffer_.size() + base_type::size();
    }

    constexpr const_reference top() const {
        assert(!empty());
        return deletion_buffer_.front();
    }

    void pop() {
        assert(!empty());
        deletion_buffer_.pop_front();
        if (deletion_buffer_.empty()) {
            refill_deletion_buffer();
        }
    }

    void push(value_type value) {
        if (empty()) {
            deletion_buffer_.push_back(std::move(value));
            return;
        }
        auto it = deletion_buffer_.crbegin();
        while (base_type::comp(*it, value)) {
            ++it;
            if (it == deletion_buffer_.crend()) {
                break;
            }
        }
        if (it != deletion_buffer_.crbegin()) {
            // value has to go into the deletion buffer
            if (!deletion_buffer_.full()) {
                deletion_buffer_.insert_at(it.base(), std::move(value));
                return;
            } else {
                auto tmp = std::move(deletion_buffer_.back());
                deletion_buffer_.pop_back();
                deletion_buffer_.insert_at(it.base(), std::move(value));
                value = std::move(tmp);
            }
        }
        // Insert `value` into insertion buffer
        if (ins_buf_size_ == InsertionBufferSize) {
            flush_insertion_buffer();
        }
        insertion_buffer_[ins_buf_size_++] = std::move(value);
    }

    constexpr void clear() noexcept {
        ins_buf_size_ = 0;
        deletion_buffer_.clear();
        base_type::clear();
    }

    constexpr value_compare value_comp() const {
        return base_type::comp;
    }
};

}  // namespace multiqueue

namespace std {
template <std::size_t InsertionBufferSize, std::size_t DeletionBufferSize, typename PriorityQueue, typename Alloc>
struct uses_allocator<multiqueue::BufferedPQ<InsertionBufferSize, DeletionBufferSize, PriorityQueue>, Alloc>
    : uses_allocator<PriorityQueue, Alloc>::type {};

}  // namespace std

#endif  //! BUFFERED_PQ_HPP_INCLUDED
