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
#include <cstddef>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#ifdef NDEBUG_MQ_BUFFERED_PQ

#define BUFFERED_PQ_ASSERT(x) \
    do {                      \
    } while (false)

#else

#include <cassert>
#define BUFFERED_PQ_ASSERT(x) \
    do {                      \
        assert(x);            \
    } while (false)

#endif

namespace multiqueue {

static constexpr std::size_t DefaultInsertionBuffersize = 128;
static constexpr std::size_t DefaultDeletionBuffersize = 128;

template <typename PriorityQueue, std::size_t InsertionBuffersize = DefaultInsertionBuffersize,
          std::size_t DeletionBuffersize = DefaultDeletionBuffersize>
class BufferedPQ : private PriorityQueue {
   private:
    static constexpr std::size_t ReservePerPQ = std::size_t{1} << 20;
    using base_type = PriorityQueue;

   public:
    using value_type = typename base_type::value_type;
    using value_compare = typename base_type::value_compare;
    using reference = typename base_type::reference;
    using const_reference = typename base_type::const_reference;
    using size_type = std::size_t;

   private:
    using insertion_buffer_type = std::array<value_type, InsertionBuffersize>;
    using deletion_buffer_type = std::array<value_type, DeletionBuffersize>;

    size_type ins_buf_size_ = 0;
    insertion_buffer_type insertion_buffer_;
    size_type del_buf_size_ = 0;
    deletion_buffer_type deletion_buffer_;

    void flush_insertion_buffer() {
        for (size_type i = 0; i != ins_buf_size_; ++i) {
            base_type::push(std::move(insertion_buffer_[i]));
        }
        ins_buf_size_ = 0;
    }

    void refill_deletion_buffer() {
        BUFFERED_PQ_ASSERT(del_buf_size_ == 0);
        // We flush the insertion buffer into the heap, then refill the
        // deletion buffer from the heap. We could also merge the insertion
        // buffer and heap into the deletion buffer
        flush_insertion_buffer();
        size_type num_refill = std::min(DeletionBuffersize, base_type::size());
        del_buf_size_ = num_refill;
        while (num_refill != 0) {
            deletion_buffer_[--num_refill] = std::move(base_type::c.front());
            base_type::pop();
        }
    }

   public:
    explicit BufferedPQ(value_compare compare = value_compare()) : base_type(compare) {
        base_type::c.reserve(ReservePerPQ);
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<base_type, Alloc>>>
    explicit BufferedPQ(value_compare const& compare, Alloc const& alloc) : base_type(compare, alloc) {
        base_type::c.reserve(ReservePerPQ);
    }

    template <typename Alloc, typename = std::enable_if_t<std::uses_allocator_v<base_type, Alloc>>>
    explicit BufferedPQ(Alloc const& alloc) : base_type(alloc) {
        base_type::c.reserve(ReservePerPQ);
    }

    [[nodiscard]] constexpr bool empty() const {
        BUFFERED_PQ_ASSERT(del_buf_size_ != 0 || (ins_buf_size_ == 0 && base_type::empty()));
        return del_buf_size_ == 0;
    }

    [[nodiscard]] constexpr size_type size() const noexcept {
        return ins_buf_size_ + del_buf_size_ + base_type::size();
    }

    constexpr const_reference top() const {
        BUFFERED_PQ_ASSERT(!empty());
        return deletion_buffer_[del_buf_size_ - 1];
    }

    void pop() {
        BUFFERED_PQ_ASSERT(!empty());
        if (--del_buf_size_ == 0) {
            refill_deletion_buffer();
        }
    }

    void push(value_type value) {
        if (empty()) {
            deletion_buffer_.front() = std::move(value);
            ++del_buf_size_;
            return;
        }
        if (base_type::comp(deletion_buffer_[0], value)) {
            // value has to go into the deletion buffer
            if (del_buf_size_ != DeletionBuffersize) {
                size_type in_pos = del_buf_size_;
                while (!base_type::comp(deletion_buffer_[in_pos - 1], value)) {
                    deletion_buffer_[in_pos] = std::move(deletion_buffer_[in_pos - 1]);
                    // No need to check for underflow
                    --in_pos;
                    BUFFERED_PQ_ASSERT(in_pos >= 1);
                }
                deletion_buffer_[in_pos] = std::move(value);
                ++del_buf_size_;
                return;
            }
            auto tmp = std::move(deletion_buffer_[0]);
            size_type in_pos = 0;
            while (in_pos + 1 != DeletionBuffersize && base_type::comp(deletion_buffer_[in_pos + 1], value)) {
                deletion_buffer_[in_pos] = std::move(deletion_buffer_[in_pos + 1]);
                ++in_pos;
            }
            BUFFERED_PQ_ASSERT(in_pos < DeletionBuffersize);
            deletion_buffer_[in_pos] = std::move(value);
            // Fallthrough, the last element needs to be inserted into the insertion buffer
            value = std::move(tmp);
        }
        // Insert `value` into insertion buffer
        if (ins_buf_size_ == InsertionBuffersize) {
            flush_insertion_buffer();
        }
        insertion_buffer_[ins_buf_size_++] = std::move(value);
    }
};

}  // namespace multiqueue

namespace std {
template <typename PriorityQueue, std::size_t InsertionBuffersize, std::size_t DeletionBuffersize, typename Alloc>
struct uses_allocator<multiqueue::BufferedPQ<PriorityQueue, InsertionBuffersize, DeletionBuffersize>, Alloc>
    : uses_allocator<PriorityQueue, Alloc>::type {};

}  // namespace std
