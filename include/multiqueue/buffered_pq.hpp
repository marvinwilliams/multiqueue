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
#include "system_config.hpp"

#include <cstddef>
#include <memory>
#include <utility>

namespace multiqueue {

template <typename Key, typename T, typename Compare, typename Allocator, typename Configuration>
class BufferedPQ {
   private:
    using heap_type = Heap<Key, T, Compare, Configuration::HeapDegree, Allocator>;
    using key_of = detail::key_extractor<Key, T>;

   public:
    using key_type = typename heap_type::key_type;
    using value_type = typename heap_type::value_type;
    using key_compare = Compare;
    using allocator_type = Allocator;
    using reference = typename heap_type::reference;
    using const_reference = typename heap_type::const_reference;
    using size_type = std::size_t;

   private:
    Buffer<value_type, Configuration::InsertionBuffersize> insertion_buffer_;
    RingBuffer<value_type, Configuration::DeletionBufferSize> deletion_buffer_;

    heap_type heap_;
    key_compare comp_;

   private:
    void flush_insertion_buffer() {
        for (auto it = insertion_buffer_.begin(); it != insertion_buffer_.end(); ++it) {
            heap_.insert(std::move(*it));
        }
        insertion_buffer_.clear();
    }

    void refill_deletion_buffer() {
        assert(deletion_buffer_.empty());
        flush_insertion_buffer();
        while (!deletion_buffer_.full() && !heap_.empty()) {
            deletion_buffer_.push_back(heap_.top());
            heap_.pop();
        }
    }

   public:
    explicit BufferedPQ(allocator_type const& alloc = allocator_type()) : heap_(alloc) {
    }

    [[nodiscard]] constexpr bool empty() const noexcept {
        return deletion_buffer_.empty();
    }

    constexpr size_type size() const noexcept {
        return insertion_buffer_.size() + deletion_buffer_.size() + heap_.size();
    }

    constexpr const_reference top() const {
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
        auto it = deletion_buffer_.end();
        while (it != deletion_buffer_.begin() && comp_(key_of{}(value), key_of{}(*(it - 1)))) {
            --it;
        }
        if (!deletion_buffer_.empty() && it == deletion_buffer_.end()) {
            // Insert into insertion buffer
            if (insertion_buffer_.full()) {
                flush_insertion_buffer();
                heap_.insert(value);
            } else {
                insertion_buffer_.push_back(value);
            }
        } else {
            // Insert into deletion buffer
            if (deletion_buffer_.full()) {
                if (insertion_buffer_.full()) {
                    flush_insertion_buffer();
                    heap_.insert(std::move(deletion_buffer_.back()));
                } else {
                    insertion_buffer_.push_back(std::move(deletion_buffer_.back()));
                }
                deletion_buffer_.pop_back();
            }
            deletion_buffer_.insert(it, value);
        }
    }

    void push(value_type&& value) {
        auto it = deletion_buffer_.end();
        while (it != deletion_buffer_.begin() && comp_(key_of{}(value), key_of{}(*(it - 1)))) {
            --it;
        }
        if (!deletion_buffer_.empty() && it == deletion_buffer_.end()) {
            // Insert into insertion buffer
            if (insertion_buffer_.full()) {
                flush_insertion_buffer();
                heap_.insert(std::move(value));
            } else {
                insertion_buffer_.push_back(std::move(value));
            }
        } else {
            // Insert into deletion buffer
            if (deletion_buffer_.full()) {
                if (insertion_buffer_.full()) {
                    flush_insertion_buffer();
                    heap_.insert(std::move(deletion_buffer_.back()));
                } else {
                    insertion_buffer_.push_back(std::move(deletion_buffer_.back()));
                }
                deletion_buffer_.pop_back();
            }
            deletion_buffer_.insert(it, std::move(value));
        }
    }

    void heap_reserve(size_type cap) {
        heap_.reserve(cap);
    }

    constexpr void clear() noexcept {
        insertion_buffer_.clear();
        deletion_buffer_.clear();
        heap_.clear();
    }
};

}  // namespace multiqueue
#endif  //! BUFFERED_PQ_HPP_INCLUDED
