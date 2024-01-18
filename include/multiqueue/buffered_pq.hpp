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

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

namespace multiqueue {

template <typename PriorityQueue, std::size_t insertion_buffer_size = 16, std::size_t deletion_buffer_size = 16>
class BufferedPQ {
    static_assert(insertion_buffer_size > 0 && deletion_buffer_size > 0, "Both buffers must have nonzero size");

   public:
    using priority_queue_type = PriorityQueue;
    using value_type = typename priority_queue_type::value_type;
    using value_compare = typename priority_queue_type::value_compare;
    using reference = typename priority_queue_type::reference;
    using const_reference = typename priority_queue_type::const_reference;
    using size_type = std::size_t;

   private:
    using insertion_buffer_type = std::array<value_type, insertion_buffer_size>;
    using deletion_buffer_type = std::array<value_type, deletion_buffer_size>;

    struct PriorityQueueWrapper : priority_queue_type {
        using priority_queue_type::priority_queue_type;

        bool compare(value_type const& lhs, value_type const& rhs) const {
            return priority_queue_type::comp(lhs, rhs);
        }

        void reserve(size_type new_cap) {
            priority_queue_type::c.reserve(new_cap);
        }
    };

    size_type deletion_end_ = 0;
    size_type insertion_end_ = 0;
    deletion_buffer_type deletion_buffer_;
    insertion_buffer_type insertion_buffer_;
    PriorityQueueWrapper pq_;

    void flush_insertion_buffer() {
        for (; insertion_end_ != 0; --insertion_end_) {
            pq_.push(std::move(insertion_buffer_[insertion_end_ - 1]));
        }
    }

    void refill_deletion_buffer() {
        assert(deletion_end_ == 0);
        // We flush the insertion buffer into the heap, then refill the
        // deletion buffer from the heap. We could also merge the insertion
        // buffer and heap into the deletion buffer
        flush_insertion_buffer();
        size_type front_slot = std::min(deletion_buffer_size, pq_.size());
        deletion_end_ = front_slot;
        while (front_slot != 0) {
            deletion_buffer_[--front_slot] = pq_.top();
            pq_.pop();
        }
    }

   public:
    explicit BufferedPQ(value_compare const& compare = value_compare()) : pq_(compare) {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<priority_queue_type, Alloc>>>
    explicit BufferedPQ(value_compare const& compare, Alloc const& alloc) : pq_(compare, alloc) {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<priority_queue_type, Alloc>>>
    explicit BufferedPQ(Alloc const& alloc) : pq_(alloc) {
    }

    [[nodiscard]] constexpr bool empty() const {
        assert(deletion_end_ != 0 || (insertion_end_ == 0 && pq_.empty()));
        return deletion_end_ == 0;
    }

    [[nodiscard]] constexpr size_type size() const noexcept {
        return insertion_end_ + deletion_end_ + pq_.size();
    }

    constexpr const_reference top() const {
        assert(!empty());
        return deletion_buffer_[deletion_end_ - 1];
    }

    void pop() {
        assert(!empty());
        --deletion_end_;
        if (deletion_end_ == 0) {
            refill_deletion_buffer();
        }
    }

    void push(const_reference value) {
        if (deletion_end_ > 0 && !pq_.compare(value, deletion_buffer_[0])) {
            size_type slot = deletion_end_ - 1;
            while (pq_.compare(value, deletion_buffer_[slot])) {
                --slot;
            }
            if (deletion_end_ == deletion_buffer_size) {
                if (insertion_end_ == insertion_buffer_size) {
                    flush_insertion_buffer();
                    pq_.push(std::move(deletion_buffer_[0]));
                } else {
                    insertion_buffer_[insertion_end_++] = std::move(deletion_buffer_[0]);
                }
                std::move(deletion_buffer_.begin() + 1, deletion_buffer_.begin() + slot + 1, deletion_buffer_.begin());
                deletion_buffer_[slot] = value;
            } else {
                std::move_backward(deletion_buffer_.begin() + slot + 1, deletion_buffer_.begin() + deletion_end_,
                                   deletion_buffer_.begin() + deletion_end_ + 1);
                deletion_buffer_[slot + 1] = value;
                ++deletion_end_;
            }
            return;
        }
        if (deletion_end_ < deletion_buffer_size && pq_.size() == 0 && insertion_end_ == 0) {
            std::move_backward(deletion_buffer_.begin(), deletion_buffer_.begin() + deletion_end_,
                               deletion_buffer_.begin() + deletion_end_ + 1);
            deletion_buffer_[0] = value;
            ++deletion_end_;
            return;
        }
        if (insertion_end_ == insertion_buffer_size) {
            flush_insertion_buffer();
            pq_.push(value);
        } else {
            insertion_buffer_[insertion_end_++] = value;
        }
    }

    void reserve(size_type new_cap) {
        pq_.reserve(new_cap);
    }
};

template <typename PriorityQueue>
class BufferedPQ<PriorityQueue, 0, 0> : public PriorityQueue {
    using priority_queue_type = PriorityQueue;

   public:
    using priority_queue_type::priority_queue_type;

    void reserve(typename priority_queue_type::size_type new_cap) {
        priority_queue_type::c.reserve(new_cap);
    }
};

}  // namespace multiqueue

namespace std {
template <typename PriorityQueue, std::size_t insertion_buffer_size, std::size_t deletion_buffer_size, typename Alloc>
struct uses_allocator<multiqueue::BufferedPQ<PriorityQueue, insertion_buffer_size, deletion_buffer_size>, Alloc>
    : uses_allocator<PriorityQueue, Alloc>::type {};

}  // namespace std
