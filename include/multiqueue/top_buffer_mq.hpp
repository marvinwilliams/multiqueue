/**
******************************************************************************
* @file:   top_buffer_mq.hpp
*
* @author: Marvin Williams
* @date:   2021/02/19 13:43
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef TOP_BUFFER_MQ_HPP_INCLUDED
#define TOP_BUFFER_MQ_HPP_INCLUDED

#include "multiqueue/sequential/heap/full_down_strategy.hpp"
#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/util/extractors.hpp"
#include "multiqueue/util/range_iterator.hpp"

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
struct TopBufferConfiguration {
    using key_type = typename ValueType::first_type;
    using mapped_type = typename ValueType::second_type;
    // With `p` threads, use `C*p` queues
    static constexpr unsigned int C = 4;
    // The underlying sequential priority queue to use
    static constexpr unsigned int HeapDegree = 4;
    // The sentinel
    static constexpr key_type Sentinel = std::numeric_limits<key_type>::max();
    // The sifting strategy to use
    using SiftStrategy = local_nonaddressable::full_down_strategy;
    // The allocator to use in the underlying sequential priority queue
    using HeapAllocator = std::allocator<ValueType>;
};

template <typename Key, typename T, typename Comparator = std::less<Key>,
          template <typename> typename Configuration = TopBufferConfiguration, typename Allocator = std::allocator<Key>>
class top_buffer_mq {
   public:
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<key_type, mapped_type>;
    using key_comparator = Comparator;

   private:
    using config_type = Configuration<value_type>;
    static constexpr unsigned int C = config_type::C;
    static constexpr key_type Sentinel = config_type::Sentinel;

    using heap_type = local_nonaddressable::heap<value_type, key_type, util::get_nth<value_type>, key_comparator,
                                                 config_type::HeapDegree, typename config_type::SiftStrategy,
                                                 typename config_type::HeapAllocator>;

    struct alignas(CACHE_LINESIZE) guarded_heap {
        using allocator_type = typename heap_type::allocator_type;
        mutable std::atomic_bool in_use = false;
        value_type top = {Sentinel, mapped_type{}};
        heap_type heap;

        explicit guarded_heap() = default;
        explicit guarded_heap(allocator_type const &alloc) : heap{alloc} {
        }
        [[nodiscard]] inline bool empty() const noexcept {
            return top.first == Sentinel;
        };
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

    inline bool try_lock(std::size_t const index) const noexcept {
        // TODO: MEASURE
        bool expect_in_use = false;
        return heap_list_[index].in_use.compare_exchange_strong(expect_in_use, true, std::memory_order_relaxed,
                                                                std::memory_order_acquire);
    }

    inline void unlock(std::size_t const index) const noexcept {
        heap_list_[index].in_use.store(false, std::memory_order_release);
    }

    inline void refill_top(std::size_t const index) {
        if (heap_list_[index].heap.empty()) {
            heap_list_[index].top.first = Sentinel;
        } else {
            heap_list_[index].heap.extract_top(heap_list_[index].top);
        }
    }

   public:
    explicit top_buffer_mq(unsigned int const num_threads)
        : heap_list_(num_threads * C), num_queues_{static_cast<unsigned int>(heap_list_.size())}, comp_{} {
        assert(num_threads >= 1);
    }

    explicit top_buffer_mq(unsigned int const num_threads, allocator_type const &alloc)
        : heap_list_(num_threads * C, alloc), num_queues_{static_cast<unsigned int>(heap_list_.size())}, comp_{} {
        assert(num_threads >= 1);
    }

    bool top(value_type &retval) const {
        size_t index;
        do {
            index = random_queue_index();
        } while (!try_lock(index));
        bool found = false;
        if (heap_list_[index].empty()) {
            retval = heap_list_[index].top;
            found = true;
        }
        unlock(index);
        do {
            index = random_queue_index();
        } while (!try_lock(index));
        if (heap_list_[index].empty() && (!found || comp_(heap_list_[index].top.first, retval.first))) {
            retval = heap_list_[index].top;
            found = true;
        }
        unlock(index);
        return found;
    }

    void push(value_type const &value) {
        size_t index;
        do {
            index = random_queue_index();
        } while (!try_lock(index));
        if (heap_list_[index].empty()) {
            heap_list_[index].top = value;
        } else if (comp_(value.first, heap_list_[index].top.first)) {
            heap_list_[index].heap.insert(std::move(std::exchange(heap_list_[index].top, value)));
        } else {
            heap_list_[index].heap.insert(value);
        }
        unlock(index);
    }

    void push(value_type &&value) {
        size_t index;
        do {
            index = random_queue_index();
        } while (!try_lock(index));
        if (heap_list_[index].empty()) {
            heap_list_[index].top = std::move(value);
        } else if (comp_(value.first, heap_list_[index].top.first)) {
            heap_list_[index].heap.insert(std::move(std::exchange(heap_list_[index].top, std::move(value))));
        } else {
            heap_list_[index].heap.insert(std::move(value));
        }
        unlock(index);
    }

    bool extract_top(value_type &retval) {
        size_t first_index;
        for (unsigned int count = 0; count < 2; ++count) {
            do {
                first_index = random_queue_index();
            } while (!try_lock(first_index));
            if (!heap_list_[first_index].empty()) {
                if (count == 1) {
                    retval = std::move(heap_list_[first_index].top);
                    refill_top(first_index);
                    unlock(first_index);
                    return true;
                }
                // hold the lock for comparison
                break;
            } else {
                unlock(first_index);
            }
            if (count == 1) {
                return false;
            }
        }
        // When we get here, we hold the lock for the first heap, which has a nonempty buffer
        size_t second_index;
        do {
            second_index = random_queue_index();
        } while (!try_lock(second_index));
        if (!heap_list_[second_index].empty() &&
            comp_(heap_list_[second_index].top.first, heap_list_[first_index].top.first)) {
            unlock(first_index);
            retval = std::move(heap_list_[second_index].top);
            refill_top(second_index);
            unlock(second_index);
        } else {
            unlock(second_index);
            retval = std::move(heap_list_[first_index].top);
            refill_top(first_index);
            unlock(first_index);
        }
        return true;
    }
};

}  // namespace rsm
}  // namespace multiqueue

#endif  //! TOP_BUFFER_PQ_HPP_INCLUDED
