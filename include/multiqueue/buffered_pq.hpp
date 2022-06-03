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

#include "multiqueue/heap.hpp"
#include "multiqueue/value.hpp"

#include <algorithm>
#include <array>
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
    using size_type = typename pq_type::size_type;

   private:
    using insertion_buffer_type = std::array<value_type, InsertionBufferSize>;
    using deletion_buffer_type = std::array<value_type, DeletionBufferSize>;

    std::size_t ins_buf_size_;
    insertion_buffer_type insertion_buffer_;
    std::size_t del_buf_size_;
    deletion_buffer_type deletion_buffer_;
    pq_type pq_;

   private:
    void flush_insertion_buffer() {
        std::for_each(insertion_buffer_.begin(), insertion_buffer_.begin() + ins_buf_size_,
                      [this](value_type& v) { pq_.push(std::move(v)); });
        ins_buf_size_ = 0;
    }

    void refill_deletion_buffer() {
        assert(del_buf_size_ == 0);
        // We flush the insertion buffer into the heap, then refill the
        // deletion buffer from the heap. We could also merge the insertion
        // buffer and heap into the deletion buffer
        flush_insertion_buffer();
        size_type num_refill = std::min(DeletionBufferSize, pq_.size());
        del_buf_size_ = num_refill;
        while (num_refill-- != 0) {
            pq_.extract_top(deletion_buffer_[num_refill]);
        }
    }

   public:
    template <typename... Args>
    explicit BufferedPQ(Args&&... args) : pq_(std::forward<Args>(args)...) {
        ins_buf_size_ = 0;
        del_buf_size_ = 0;
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return del_buf_size_ == 0;
    }

    constexpr size_type size() const noexcept {
        return ins_buf_size_ + del_buf_size_ + pq_.size();
    }

    constexpr const_reference top() const {
        assert(!empty());
        return deletion_buffer_[del_buf_size_ - 1];
    }

    void pop() {
        assert(!empty());
        --del_buf_size_;
        if (del_buf_size_ == 0) {
            refill_deletion_buffer();
        }
    }

    void pop(reference retval) {
        assert(!empty());
        retval = std::move(deletion_buffer_[del_buf_size_ - 1]);
        pop();
    };

    void push(const_reference value) {
    // TODO
        if (del_buf_size_ == 0 || !pq_.value_comp()(deletion_buffer_[0], value)) {
            auto it = std::lower_bound(deletion_buffer_.begin() + 1, deletion_buffer_.begin() + del_buf_size_,
                                       [&value, this](const_reference e) { return pq_.value_comp()(e, value); });
            if (del_buf_size_ == DeletionBufferSize) {
                auto tmp = std::move(deletion_buffer_[0]);
                std::move_backward(deletion_buffer_.begin(), deletion_buffer_.begin() + del_buf_size_,
                                   deletion_buffer_.begin() + del_buf_size_ + 1);
            }
        }
    }

    void push(const_reference value) {
        auto it = std::find_if(deletion_buffer_.begin(), deletion_buffer_.begin() + del_buf_size_,
                               [&value, this](const_reference entry) { return pq_.value_comp()(entry, value); });
        if (it == deletion_buffer_.begin()) {
            // Can the element still be pushed into the deletion buffer?
            if (pq_.empty() && ins_buf_size_ == 0 && del_buf_size_ != DeletionBufferSize) {
                std::move_backward(deletion_buffer_.begin(), deletion_buffer_.begin() + del_buf_size_,
                                   deletion_buffer_.begin() + del_buf_size_ + 1);
                deletion_buffer_.front() = value;
                ++del_buf_size_;
            } else {
                // insert into insertion buffer
                if (ins_buf_size_ != InsertionBufferSize) {
                    insertion_buffer_[ins_buf_size_++] = value;
                } else {
                    flush_insertion_buffer();
                    pq_.push(value);
                }
            }
        } else {
            // Push into deletion buffer
            if (del_buf_size_ == DeletionBufferSize) {
                if (ins_buf_size_ != InsertionBufferSize) {
                    insertion_buffer_[ins_buf_size_++] = std::move(deletion_buffer_.front());
                } else {
                    flush_insertion_buffer();
                    pq_.push(std::move(deletion_buffer_.front()));
                }
                std::move(deletion_buffer_.begin() + 1, it, deletion_buffer_.begin());
                *(it - 1) = value;
            } else {
                std::move_backward(it, deletion_buffer_.begin() + del_buf_size_,
                                   deletion_buffer_.begin() + del_buf_size_ + 1);
                ++del_buf_size_;
                *it = value;
            }
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

template <std::size_t InsertionBufferSize, std::size_t DeletionBufferSize, typename PriorityQueue, typename Alloc>
struct uses_allocator<multiqueue::BufferedPQ<InsertionBufferSize, DeletionBufferSize, PriorityQueue>, Alloc>
    : uses_allocator<PriorityQueue, Alloc>::type {};

}  // namespace std

#endif  //! BUFFERED_PQ_HPP_INCLUDED
