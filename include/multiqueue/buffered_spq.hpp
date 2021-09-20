/**
******************************************************************************
* @file:   buffered_spq.hpp
*
* @author: Marvin Williams
* @date:   2021/09/14 17:54
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef BUFFERED_SPQ_HPP_INCLUDED
#define BUFFERED_SPQ_HPP_INCLUDED

#include "multiqueue/heap.hpp"
#include "multiqueue/ring_buffer.hpp"
#include "multiqueue/value.hpp"
#include "system_config.hpp"

#include <limits>
#include <memory>

namespace multiqueue {

template <bool want_numa>
static constexpr bool spq_alignment = use_numa<want_numa> ? PAGESIZE : 2 * L1_CACHE_LINESIZE;

template <typename Key, typename T, typename Configuration>
class alignas(spq_alignment<Configuration::NumaFriendly>) buffered_spq {
   private:
    using allocator_type = typename Configuration::HeapAllocator;
    using heap_type = heap<Key, T, Configuration::HeapDegree, typename Configuration::HeapAllocator>;

   public:
    using value_type = typename heap_type::value_type;
    static constexpr Key max_key = std::numeric_limits<Key>::max() >> 1;

   private:
    static constexpr Key lock_mask = ~max_key;

    // The highest bit of min_key also functions as lock
    std::atomic<Key> min_key;
    alignas(L1_CACHE_LINESIZE) std::array<value_type, Configuration::InsertionBufferSize> insertion_buffer;
    typename decltype(insertion_buffer)::iterator insertion_buffer_end;
    util::ring_buffer<value_type, Configuration::DeletionBufferSize> deletion_buffer;

    heap_type heap;

    inline void flush_insertion_buffer() {
        assert(min_key & lock_mask);
        auto it = insertion_buffer.begin();
        while (it != insertion_buffer_end) {
            heap.insert(*it);
            ++it;
        }
        insertion_buffer_end = insertion_buffer.begin();
    }

    void refresh_min() {
        assert(min_key & lock_mask);
        assert(deletion_buffer.empty());
        flush_insertion_buffer();
        while (!deletion_buffer.full() && !heap.empty()) {
            deletion_buffer.push_back(heap.min());
            heap.pop();
        }
    }

   public:
    explicit buffered_spq(allocator_type const &alloc = allocator_type())
        : min_key(max_key), insertion_buffer_end(insertion_buffer.begin()), heap(alloc) {
    }

    // Lock must be held
    // Deletion buffer must not be empty
    value_type extract_min_and_unlock() {
        assert(min_key & lock_mask);
        assert(!deletion_buffer.empty());
        auto retval = deletion_buffer.front();
        deletion_buffer.pop_front();
        if (deletion_buffer.empty()) {
            refresh_min();
        }
        min_key.store(deletion_buffer.empty() ? max_key : deletion_buffer.front().key, std::memory_order_release);
        return retval;
    };

    void push_and_unlock(value_type value) {
        assert(min_key & lock_mask);
        auto pos = deletion_buffer.size();
        while (pos > 0 && value.first < deletion_buffer[pos - 1].key) {
            --pos;
        }
        if (!deletion_buffer.empty() && pos == deletion_buffer.size()) {
            // Insert into insertion buffer
            if (insertion_buffer_end != insertion_buffer.end()) {
                *insertion_buffer_end++ = value;
            } else {
                flush_insertion_buffer();
                heap.insert(value);
            }
        } else {
            // Insert into deletion buffer
            if (deletion_buffer.full()) {
                if (insertion_buffer_end != insertion_buffer.end()) {
                    *insertion_buffer++ = deletion_buffer.back();
                } else {
                    flush_insertion_buffer();
                    heap.insert(deletion_buffer.back());
                }
                deletion_buffer.pop_back();
            }
            deletion_buffer.insert(pos, value);
        }
        assert(!deletion_buffer.empty());
        // TODO: Maybe only read deletion_buffer if pos == 0?
        min_key.store(deletion_buffer.front().key, std::memory_order_release);
    }

    inline bool empty() const noexcept {
        return deletion_buffer.empty();
    }

    inline std::size_t size() const noexcept {
        return (insertion_buffer_end - insertion_buffer.begin()) + deletion_buffer.size() + heap.size();
    }

    inline Key get_min_key() const noexcept {
        return min_key.load(std::memory_order_relaxed) & (~lock_mask);
    }

    inline bool try_lock() const noexcept {
        Key current = min_key.load(std::memory_order_relaxed);
        if (current & lock_mask) {
            return false;
        }
        return min_key.compare_exchange_strong(current, current | lock_mask, std::memory_order_acquire,
                                               std::memory_order_relaxed);
    }

    inline bool try_lock(Key expected) const noexcept {
        if (expected & lock_mask) {
            return false;
        }
        return min_key.compare_exchange_strong(expected, expected | lock_mask, std::memory_order_acquire,
                                               std::memory_order_relaxed);
    }

    // Use only if min_key was not changed
    inline void unlock() const noexcept {
        Key expected = min_key.load(std::memory_order_acquire);
        assert(expected & lock_mask);
        min_key.store(expected & (~lock_mask), std::memory_order_release);
    }
};

}  // namespace multiqueue
#endif  //! BUFFERED_SPQ_HPP_INCLUDED
