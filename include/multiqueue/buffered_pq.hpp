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

#include "multiqueue/buffer.hpp"
#include "multiqueue/heap.hpp"
#include "multiqueue/ring_buffer.hpp"
#include "multiqueue/value.hpp"

#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

namespace multiqueue {

template <std::size_t InsertionBufferSize, std::size_t DeletionBufferSize, typename PriorityQueue>
class BufferedPQ {
   private:
    using pq_type = PriorityQueue;

   public:
    using value_type = typename pq_type::value_type;
    using value_compare = typename pq_type::value_compare;
    using reference = typename pq_type::reference;
    using const_reference = typename pq_type::const_reference;
    using size_type = std::size_t;

   private:
    using insertion_buffer_type = Buffer<value_type, InsertionBufferSize>;
    using deletion_buffer_type = RingBuffer<value_type, DeletionBufferSize>;

    insertion_buffer_type insertion_buffer_;
    deletion_buffer_type deletion_buffer_;
    pq_type pq_;

   private:
    void flush_insertion_buffer() {
        std::for_each(insertion_buffer_.begin(), insertion_buffer_.end(),
                      [this](value_type& v) { pq_.push(std::move(v)); });
        insertion_buffer_.clear();
    }

    void refill_deletion_buffer() {
        assert(deletion_buffer_.empty());
        // We flush the insertion buffer into the heap, then refill the
        // deletion buffer from the heap. We could also merge the insertion
        // buffer and heap into the deletion buffer
        flush_insertion_buffer();
        size_type num_refill = std::min(deletion_buffer_type::Capacity, pq_.size());
        for (; num_refill != 0; --num_refill) {
            deletion_buffer_.push_back(pq_.top());
            pq_.pop();
        }
    }

   public:
    explicit BufferedPQ(value_compare const& comp = value_compare()) : pq_(comp) {
    }

    template <typename Alloc>
    explicit BufferedPQ(value_compare const& comp, Alloc const& alloc) : pq_(comp, alloc) {
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return deletion_buffer_.empty();
    }

    constexpr size_type size() const noexcept {
        return insertion_buffer_.size() + deletion_buffer_.size() + pq_.size();
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

    void extract_top(reference retval) {
        assert(!empty());
        retval = std::move(deletion_buffer_.front());
        pop();
    };

    void push(const_reference value) {
        if (empty()) {
            deletion_buffer_.push_back(value);
            return;
        }
        auto it = std::find_if(deletion_buffer_.rbegin(), deletion_buffer_.rend(),
                               [&value, this](const_reference entry) { return pq_.value_comp()(entry, value); });
        if (it == deletion_buffer_.rbegin()) {
            // Insert into insertion buffer
            if (!insertion_buffer_.full()) {
                insertion_buffer_.push_back(value);
                return;
            }
            // Could also do a merging refill into the deletion buffer
            flush_insertion_buffer();
            pq_.push(value);
        } else {
            // Insert into deletion buffer
            if (deletion_buffer_.full()) {
                if (!insertion_buffer_.full()) {
                    insertion_buffer_.push_back(std::move(deletion_buffer_.back()));
                } else {
                    flush_insertion_buffer();
                    pq_.push(std::move(deletion_buffer_.back()));
                }
                deletion_buffer_.pop_back();
            }
            deletion_buffer_.insert(it.base(), value);
        }
    }

    void push(value_type&& value) {
        if (empty()) {
            deletion_buffer_.push_back(std::move(value));
            return;
        }
        auto it = std::find_if(deletion_buffer_.rbegin(), deletion_buffer_.rend(),
                               [&value, this](const_reference entry) { return pq_.value_comp()(entry, value); });

        if (it == deletion_buffer_.rbegin()) {
            // Insert into insertion buffer
            if (!insertion_buffer_.full()) {
                insertion_buffer_.push_back(std::move(value));
                return;
            }
            // Could also do a merging refill into the deletion buffer
            flush_insertion_buffer();
            pq_.push(std::move(value));
        } else {
            // Insert into deletion buffer
            if (deletion_buffer_.full()) {
                if (!insertion_buffer_.full()) {
                    insertion_buffer_.push_back(std::move(deletion_buffer_.back()));
                } else {
                    flush_insertion_buffer();
                    pq_.push(std::move(deletion_buffer_.back()));
                }
                deletion_buffer_.pop_back();
            }
            deletion_buffer_.insert(it.base(), std::move(value));
        }
    }

    void reserve(size_type cap) {
        pq_.reserve(cap);
    }

    constexpr void clear() noexcept {
        insertion_buffer_.clear();
        deletion_buffer_.clear();
        pq_.clear();
    }

    constexpr value_compare value_comp() const {
        return pq_.value_comp();
    }

    static std::string description() {
        std::stringstream ss;
        ss << "InsertionBufferSize: " << InsertionBufferSize << "\n\t";
        ss << "DeletionBufferSize: " << DeletionBufferSize << "\n\t";
        ss << pq_type::description();
        return ss.str();
    }
};

}  // namespace multiqueue

namespace std {

template <typename PriorityQueue, std::size_t InsertionBufferSize, std::size_t DeletionBufferSize, typename Alloc>
struct uses_allocator<multiqueue::BufferedPQ<PriorityQueue, InsertionBufferSize, DeletionBufferSize>, Alloc>
    : uses_allocator<PriorityQueue, Alloc>::type {};

}  // namespace std

#endif  //! BUFFERED_PQ_HPP_INCLUDED
