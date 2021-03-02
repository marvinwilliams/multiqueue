/**
******************************************************************************
* @file:   merge_mq.hpp
*
* @author: Marvin Williams
* @date:   2021/03/02 16:18
* @brief:  
*******************************************************************************
**/

#pragma once
#ifndef MERGE_MQ_HPP_INCLUDED
#define MERGE_MQ_HPP_INCLUDED

#include <iostream>

#include "multiqueue/sequential/heap/full_down_strategy.hpp"
#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/util/extractors.hpp"
#include "multiqueue/util/range_iterator.hpp"

#include <cassert>
#include <array>
#include <atomic>
#include <cassert>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <vector>

namespace multiqueue {
namespace rsm {

// TODO: CMake defined
static constexpr unsigned int CACHE_LINESIZE = 64;

template <typename ValueType>
struct MergeConfiguration {
    using key_type = typename ValueType::first_type;
    using mapped_type = typename ValueType::second_type;
    // With `p` threads, use `C*p` queues
    static constexpr unsigned int C = 4;
    // The underlying sequential priority queue to use
    static constexpr unsigned int HeapDegree = 4;
    // The underlying sequential priority queue to use
    static constexpr unsigned int MergeSize = 4;
    // Insertion buffer size
    static constexpr size_t InsertionBufferSize = 16;
    // Deletion buffer size
    static constexpr size_t DeletionBufferSize = 16;
    // The sifting strategy to use
    using SiftStrategy = local_nonaddressable::full_down_strategy;
    // The allocator to use in the underlying sequential priority queue
    using HeapAllocator = std::allocator<ValueType>;
};

template <typename Key, typename T, typename Comparator = std::less<Key>,
          template <typename> typename Configuration = InsDelBufferConfiguration,
          typename Allocator = std::allocator<Key>>
class ins_del_buffer_mq {
   public:
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<key_type, mapped_type>;
    using key_comparator = Comparator;

   private:
    using config_type = Configuration<value_type>;
    static constexpr unsigned int C = config_type::C;
    static constexpr size_t InsertionBufferSize = config_type::InsertionBufferSize;
    static constexpr size_t DeletionBufferSize = config_type::DeletionBufferSize;

    using heap_type = local_nonaddressable::heap<value_type, key_type, util::get_nth<value_type>, key_comparator,
                                                 config_type::HeapDegree, typename config_type::SiftStrategy,
                                                 typename config_type::HeapAllocator>;

    struct alignas(CACHE_LINESIZE) guarded_heap {
        using allocator_type = typename heap_type::allocator_type;
        std::atomic_bool in_use = false;
        heap_type heap;
        std::array<value_type, DeletionBufferSize> deletion_buffer{};
        std::array<value_type, InsertionBufferSize> insertion_buffer{};
        std::uint32_t insertion_buffer_start : 8;
        std::uint32_t insertion_buffer_end : 8;
        std::uint32_t deletion_buffer_start : 8;
        std::uint32_t deletion_buffer_end : 8;

        explicit guarded_heap() = default;
        explicit guarded_heap(allocator_type const &alloc) : heap{alloc} {
        }

        inline bool deletion_buffer_empty() const noexcept {
            return deletion_buffer_end == deletion_buffer_start;
        };

        inline bool deletion_buffer_full() const noexcept {
            return (deletion_buffer_end + 1) % DeletionBufferSize == deletion_buffer_start;
        };

        inline bool insertion_buffer_empty() const noexcept {
            return insertion_buffer_end == 0;
        };

        inline bool insertion_buffer_full() const noexcept {
            return insertion_buffer_end == InsertionBufferSize;
        }

        inline bool try_lock() noexcept {
            // TODO: MEASURE
            bool expect_in_use = false;
            return in_use.compare_exchange_strong(expect_in_use, true, std::memory_order_acquire,
                                                  std::memory_order_relaxed);
        }

        inline void unlock() noexcept {
            in_use.store(false, std::memory_order_release);
        }

        inline void flush_insert_buffer() {
            for (insertion_buffer_start = 0; insertion_buffer_start < insertion_buffer_end; ++insertion_buffer_start) {
                heap.insert(std::move(insertion_buffer[insertion_buffer_start]));
            }
            insertion_buffer_end = 0;
        }

        inline void refill_deletion_buffer() {
            assert(deletion_buffer_empty());
            flush_insert_buffer();
            deletion_buffer_end = 0;
            while (deletion_buffer_end < DeletionBufferSize - 1 && !heap.empty()) {
                heap.extract_top(deletion_buffer[deletion_buffer_end++]);
            }
            deletion_buffer_start = 0;
        }

        // We try to insert the new value into the deletion buffer, if it is smaller than the largest element in the
        // deletion buffer. If the deletion buffer is full, we therefore need to evict the largest element. This element
        // then gets inserted into the insertion buffer to avoid accessing the heap. If the insertion buffer is full, we
        // flush it. If the new value is too large for the deletion buffer, it is inserted into the insertion buffer
        // which might get flushed in the process.
        void push(value_type const &value, key_comparator const &comp) {
            if (!deletion_buffer_empty()) {
                auto insert_start = (deletion_buffer_end + DeletionBufferSize - 1) % DeletionBufferSize;
                if (comp(value.first, deletion_buffer[insert_start].first)) {
                    if (deletion_buffer_full()) {
                        if (insertion_buffer_full()) {
                            flush_insert_buffer();
                            heap.insert(deletion_buffer[insert_start]);
                        } else {
                            insertion_buffer[insertion_buffer_end++] = std::move(deletion_buffer[insert_start]);
                        }
                    } else {
                        deletion_buffer[deletion_buffer_end] = std::move(deletion_buffer[insert_start]);
                        deletion_buffer_end = (deletion_buffer_end + 1) % DeletionBufferSize;
                    }
                    auto next_insert_start = (insert_start + DeletionBufferSize - 1) % DeletionBufferSize;
                    while (insert_start != deletion_buffer_start &&
                           comp(value.first, deletion_buffer[next_insert_start].first)) {
                      /* std::cout << insert_start << " " << deletion_buffer_start << " " << insertion_buffer_start << std::endl; */
                        deletion_buffer[insert_start] = std::move(deletion_buffer[next_insert_start]);
                        insert_start = next_insert_start;
                        next_insert_start = (insert_start + DeletionBufferSize - 1) % DeletionBufferSize;
                    }
                    deletion_buffer[insert_start] = value;
                    return;
                }
            }
            if (insertion_buffer_full()) {
                flush_insert_buffer();
                heap.insert(value);
            } else {
                insertion_buffer[insertion_buffer_end++] = value;
            }
        }
    };

   public:
    using allocator_type = typename std::allocator_traits<Allocator>::template rebind_alloc<guarded_heap>;

   private:
    std::vector<guarded_heap, allocator_type> heap_list_;
    size_t num_queues_;
    key_comparator comp_;

   private:
    inline std::mt19937 &get_rng() const {
        static thread_local std::mt19937 gen;
        return gen;
    }

    inline size_t random_queue_index() const {
        std::uniform_int_distribution<std::size_t> dist{0, num_queues_ - 1};
        return dist(get_rng());
    }

   public:
    explicit ins_del_buffer_mq(unsigned int const num_threads)
        : heap_list_(num_threads * C), num_queues_{static_cast<unsigned int>(heap_list_.size())}, comp_{} {
        assert(num_threads >= 1);
    }

    explicit ins_del_buffer_mq(unsigned int const num_threads, allocator_type const &alloc)
        : heap_list_(num_threads * C, alloc), num_queues_{static_cast<unsigned int>(heap_list_.size())}, comp_{} {
        assert(num_threads >= 1);
    }

    void push(value_type const &value) {
        size_t index;
        do {
            index = random_queue_index();
        } while (!heap_list_[index].try_lock());
        heap_list_[index].push(value, comp_);
        heap_list_[index].unlock();
    }

    /* void push(value_type &&value) { */
    /*     size_t index; */
    /*     do { */
    /*         index = random_queue_index(); */
    /*     } while (!heap_list_[index].try_lock()); */
    /*     heap_list_[index].push(std::move(value)); */
    /*     heap_list_[index].unlock(); */
    /* } */

    bool extract_top(value_type &retval) {
        size_t first_index;
        bool first_empty = false;
        do {
            first_index = random_queue_index();
        } while (!heap_list_[first_index].try_lock());
        if (heap_list_[first_index].deletion_buffer_empty()) {
            heap_list_[first_index].refill_deletion_buffer();
        }
        if (heap_list_[first_index].deletion_buffer_empty()) {
            heap_list_[first_index].unlock();
            first_empty = true;
        }
        // When we get here, we hold the lock for the first heap, which has a nonempty buffer
        size_t second_index;
        do {
            second_index = random_queue_index();
        } while (!heap_list_[second_index].try_lock());
        if (heap_list_[second_index].deletion_buffer_empty()) {
            heap_list_[second_index].refill_deletion_buffer();
        }
        if (heap_list_[second_index].deletion_buffer_empty()) {
            heap_list_[second_index].unlock();
            if (first_empty) {
                return false;
            }
            retval = std::move(heap_list_[first_index].deletion_buffer[heap_list_[first_index].deletion_buffer_start]);
            heap_list_[first_index].deletion_buffer_start =
                (heap_list_[first_index].deletion_buffer_start + 1) % DeletionBufferSize;
            heap_list_[first_index].unlock();
            return true;
        }
        if (first_empty ||
            comp_(heap_list_[second_index].deletion_buffer[heap_list_[second_index].deletion_buffer_start].first,
                  heap_list_[first_index].deletion_buffer[heap_list_[first_index].deletion_buffer_start].first)) {
            heap_list_[first_index].unlock();
            retval =
                std::move(heap_list_[second_index].deletion_buffer[heap_list_[second_index].deletion_buffer_start]);
            heap_list_[second_index].deletion_buffer_start =
                (heap_list_[second_index].deletion_buffer_start + 1) % DeletionBufferSize;
            heap_list_[second_index].unlock();
        } else {
            heap_list_[second_index].unlock();
            retval =
                std::move(heap_list_[first_index].deletion_buffer[heap_list_[first_index].deletion_buffer_start]);
            heap_list_[first_index].deletion_buffer_start =
                (heap_list_[first_index].deletion_buffer_start + 1) % DeletionBufferSize;
            heap_list_[first_index].unlock();
        }
        return true;
    }
};

}  // namespace rsm
}  // namespace multiqueue

#endif  //!MERGE_MQ_HPP_INCLUDED
