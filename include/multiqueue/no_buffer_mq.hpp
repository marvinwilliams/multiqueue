/**
******************************************************************************
* @file:   no_buffer_mq.hpp
*
* @author: Marvin Williams
* @date:   2021/02/17 16:57
* @brief:  This header implements a simple parallel priority queue without
*          buffering.
*******************************************************************************
**/
#pragma once
#ifndef NO_BUFFER_MQ_HPP_INCLUDED
#define NO_BUFFER_MQ_HPP_INCLUDED

#include "multiqueue/sequential/heap/full_down_strategy.hpp"
#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/util/range_iterator.hpp"
#include "multiqueue/util/extractors.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <memory>
#include <random>
#include <vector>

namespace multiqueue {
namespace rsm {

// TODO: CMake defined
static constexpr unsigned int CACHE_LINESIZE = 64;

template <typename T>
struct NoBufferConfiguration {
    // With `p` threads, use `C*p` queues
    static constexpr unsigned int C = 4;
    // The underlying sequential priority queue to use
    static constexpr unsigned int HeapDegree = 4;
    // The sifting strategy to use
    using SiftStrategy = local_nonaddressable::full_down_strategy;
    // The allocator to use in the underlying sequential priority queue
    using HeapAllocator = std::allocator<T>;
};

template <typename Key, typename T, typename Comparator = std::less<Key>,
          template <typename> typename Configuration = NoBufferConfiguration, typename Allocator = std::allocator<Key>>
class no_buffer_mq {
   public:
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<key_type, mapped_type>;
    using key_comparator = Comparator;

    static constexpr auto C = static_cast<unsigned int>(Configuration<value_type>::C);

   private:
    using heap_type = local_nonaddressable::heap<
        value_type, key_type, util::get_nth<value_type>, key_comparator, Configuration<value_type>::HeapDegree,
        typename Configuration<value_type>::SiftStrategy, typename Configuration<value_type>::HeapAllocator>;

    struct alignas(CACHE_LINESIZE) guarded_heap {
        using allocator_type = typename heap_type::allocator_type;
        mutable std::atomic_flag in_use = ATOMIC_FLAG_INIT;
        heap_type heap;

        explicit guarded_heap() = default;
        explicit guarded_heap(allocator_type const &alloc) : heap{alloc} {
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

    inline bool try_lock(std::size_t const index) const noexcept {
        // TODO: MEASURE
        if (!heap_list_[index].in_use.test_and_set(std::memory_order_relaxed)) {
            std::atomic_thread_fence(std::memory_order_acquire);
            return true;
        }
        return false;
    }

    inline void unlock(std::size_t const index) const noexcept {
        heap_list_[index].in_use.clear(std::memory_order_release);
    }

   public:
    explicit no_buffer_mq(unsigned int const num_threads)
        : heap_list_(num_threads * C), num_queues_{static_cast<unsigned int>(heap_list_.size())}, comp_{} {
        assert(num_threads >= 1);
    }

    explicit no_buffer_mq(unsigned int const num_threads, allocator_type const &alloc)
        : heap_list_(num_threads * C, alloc), num_queues_{static_cast<unsigned int>(heap_list_.size())}, comp_{} {
        assert(num_threads >= 1);
    }

    bool top(value_type &retval) const {
        size_t index;
        do {
            index = random_queue_index();
        } while (!try_lock(index));
        bool found = false;
        if (!heap_list_[index].heap.empty()) {
            retval = heap_list_[index].heap.top();
            found = true;
        }
        unlock(index);
        do {
            index = random_queue_index();
        } while (!try_lock(index));
        if (!heap_list_[index].heap.empty() && (!found || comp_(heap_list_[index].heap.top().first, retval.first))) {
            retval = heap_list_[index].heap.top();
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
        heap_list_[index].heap.insert(value);
        unlock(index);
    }

    void push(value_type &&value) {
        size_t index;
        do {
            index = random_queue_index();
        } while (!try_lock(index));
        heap_list_[index].heap.insert(std::move(value));
        unlock(index);
    }

    bool extract_top(value_type &retval) {
        size_t first_index;
        bool found = false;
        do {
            first_index = random_queue_index();
        } while (!try_lock(first_index));
        if (!heap_list_[first_index].heap.empty()) {
            retval.first = heap_list_[first_index].heap.top().first;
            found = true;
        } else {
            unlock(first_index);
        }
        size_t second_index;
        do {
            second_index = random_queue_index();
        } while (!try_lock(second_index));
        if (!heap_list_[second_index].heap.empty() &&
            (!found || comp_(heap_list_[second_index].heap.top().first, retval.first))) {
            if (found) {
                unlock(first_index);
            }
            heap_list_[second_index].heap.extract_top(retval);
            unlock(second_index);
            found = true;
        } else {
            unlock(second_index);
            if (found) {
                heap_list_[first_index].heap.extract_top(retval);
                unlock(first_index);
            }
        }
        return found;
    }
};

}  // namespace rsm
}  // namespace multiqueue

#endif  //!
