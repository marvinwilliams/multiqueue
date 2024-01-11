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
class BufferedPQ : private PriorityQueue {
    static_assert(insertion_buffer_size > 0 && deletion_buffer_size > 0, "Both buffers must have nonzero size");
    using base_type = PriorityQueue;

   public:
    using value_type = typename base_type::value_type;
    using value_compare = typename base_type::value_compare;
    using reference = typename base_type::reference;
    using const_reference = typename base_type::const_reference;
    using size_type = std::size_t;
    using priority_queue_type = base_type;

   private:
    using insertion_buffer_type = std::array<value_type, insertion_buffer_size>;
    using deletion_buffer_type = std::array<value_type, deletion_buffer_size>;

    size_type insertion_end_ = 0;
    insertion_buffer_type insertion_buffer_;
    size_type deletion_end_ = 0;
    deletion_buffer_type deletion_buffer_;

    void flush_insertion_buffer() {
        for (; insertion_end_ != 0; --insertion_end_) {
            base_type::push(std::move(insertion_buffer_[insertion_end_ - 1]));
        }
    }

    void refill_deletion_buffer() {
        assert(deletion_end_ == 0);
        // We flush the insertion buffer into the heap, then refill the
        // deletion buffer from the heap. We could also merge the insertion
        // buffer and heap into the deletion buffer
        flush_insertion_buffer();
        size_type front_slot = std::min(deletion_buffer_size, base_type::size());
        deletion_end_ = front_slot;
        while (front_slot != 0) {
            deletion_buffer_[--front_slot] = std::move(base_type::c.front());
            base_type::pop();
        }
    }

   public:
    explicit BufferedPQ(value_compare const& compare = value_compare()) : base_type(compare) {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<base_type, Alloc>>>
    explicit BufferedPQ(value_compare const& compare, Alloc const& alloc) : base_type(compare, alloc) {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<base_type, Alloc>>>
    explicit BufferedPQ(Alloc const& alloc) : base_type(alloc) {
    }

    [[nodiscard]] constexpr bool empty() const {
        assert(deletion_end_ != 0 || (insertion_end_ == 0 && base_type::empty()));
        return deletion_end_ == 0;
    }

    [[nodiscard]] constexpr size_type size() const noexcept {
        return insertion_end_ + deletion_end_ + base_type::size();
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
        if (deletion_end_ > 0 && !base_type::comp(value, deletion_buffer_[0])) {
            size_type slot = deletion_end_ - 1;
            while (base_type::comp(value, deletion_buffer_[slot])) {
                --slot;
            }
            if (deletion_end_ == deletion_buffer_size) {
                if (insertion_end_ == insertion_buffer_size) {
                    flush_insertion_buffer();
                    base_type::push(std::move(deletion_buffer_[0]));
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
        if (deletion_end_ < deletion_buffer_size && base_type::size() == 0 && insertion_end_ == 0) {
            std::move_backward(deletion_buffer_.begin(), deletion_buffer_.begin() + deletion_end_,
                               deletion_buffer_.begin() + deletion_end_ + 1);
            deletion_buffer_[0] = value;
            ++deletion_end_;
            return;
        }
        if (insertion_end_ == insertion_buffer_size) {
            flush_insertion_buffer();
            base_type::push(value);
        } else {
            insertion_buffer_[insertion_end_++] = value;
        }
    }

    void reserve(size_type new_cap) {
        base_type::c.reserve(new_cap);
    }
};

template <typename PriorityQueue>
class BufferedPQ<PriorityQueue, 0, 0> : private PriorityQueue {
   private:
    using base_type = PriorityQueue;

   public:
    using value_type = typename base_type::value_type;
    using value_compare = typename base_type::value_compare;
    using reference = typename base_type::reference;
    using const_reference = typename base_type::const_reference;
    using size_type = std::size_t;

    explicit BufferedPQ(value_compare const& compare = value_compare()) : base_type(compare) {
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<base_type, Alloc>>>
    explicit BufferedPQ(value_compare const& compare, Alloc const& alloc) : base_type(compare, alloc) {
    }

    [[nodiscard]] constexpr bool empty() const {
        return base_type::empty();
    }

    [[nodiscard]] constexpr size_type size() const noexcept {
        return base_type::size();
    }

    constexpr const_reference top() const {
        assert(!empty());
        return base_type::top();
    }

    void pop() {
        assert(!empty());
        base_type::pop();
    }

    void push(const_reference value) {
        base_type::push(value);
    }

    void reserve(size_type new_cap) {
        base_type::c.reserve(new_cap);
    }
};

}  // namespace multiqueue

namespace std {
template <typename PriorityQueue, std::size_t insertion_buffer_size, std::size_t deletion_buffer_size, typename Alloc>
struct uses_allocator<multiqueue::BufferedPQ<PriorityQueue, insertion_buffer_size, deletion_buffer_size>, Alloc>
    : uses_allocator<PriorityQueue, Alloc>::type {};

}  // namespace std
