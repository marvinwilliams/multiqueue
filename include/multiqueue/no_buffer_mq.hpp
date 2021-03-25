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

#include "multiqueue/default_configuration.hpp"
#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/util/extractors.hpp"
#include "system_config.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <random>
#include <vector>

namespace multiqueue {

template <typename Key, typename T, typename Comparator = std::less<Key>, typename Configuration = DefaultConfiguration,
          typename HeapConfiguration = sequential::DefaultHeapConfiguration, typename Allocator = std::allocator<Key>>
class no_buffer_mq {
   public:
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<key_type, mapped_type>;
    using key_comparator = Comparator;

   private:
    static constexpr unsigned int C = Configuration::C;

    using heap_type = sequential::key_value_heap<key_type, mapped_type, key_comparator, HeapConfiguration,
                                                 typename Configuration::template HeapAllocator<value_type>>;

    struct alignas(L1_CACHE_LINESIZE) guarded_heap {
        using allocator_type = typename heap_type::allocator_type;
        std::atomic_bool in_use = false;
        heap_type heap;

        inline bool try_lock() noexcept {
            bool expect_in_use = false;
            return in_use.compare_exchange_strong(expect_in_use, true, std::memory_order_acquire,
                                                  std::memory_order_relaxed);
        }

        inline void unlock() noexcept {
            in_use.store(false, std::memory_order_release);
        }

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
    inline size_t random_queue_index() const {
        static thread_local std::mt19937 gen;
        std::uniform_int_distribution<std::size_t> dist{0u, num_queues_ - 1u};
        return dist(gen);
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

    void push(value_type const &value) {
        size_t index;
        do {
            index = random_queue_index();
        } while (!heap_list_[index].try_lock());
        heap_list_[index].heap.insert(value);
        heap_list_[index].unlock();
    }

    void push(value_type &&value) {
        size_t index;
        do {
            index = random_queue_index();
        } while (!heap_list_[index].try_lock());
        heap_list_[index].heap.insert(std::move(value));
        heap_list_[index].unlock();
    }

    bool extract_top(value_type &retval) {
        size_t first_index;
        bool found = false;
        do {
            first_index = random_queue_index();
        } while (!heap_list_[first_index].try_lock());
        if (!heap_list_[first_index].heap.empty()) {
            retval.first = heap_list_[first_index].heap.top().first;
            found = true;
        } else {
            heap_list_[first_index].unlock();
        }
        size_t second_index;
        do {
            second_index = random_queue_index();
        } while (!heap_list_[second_index].try_lock());
        if (!heap_list_[second_index].heap.empty() &&
            (!found || comp_(heap_list_[second_index].heap.top().first, retval.first))) {
            if (found) {
                heap_list_[first_index].unlock();
            }
            retval = heap_list_[second_index].heap.top();
            heap_list_[second_index].heap.pop();
            heap_list_[second_index].unlock();
            found = true;
        } else {
            heap_list_[second_index].unlock();
            if (found) {
                retval = heap_list_[first_index].heap.top();
                heap_list_[first_index].heap.pop();
                heap_list_[first_index].unlock();
            }
        }
        return found;
    }
};

}  // namespace multiqueue

#endif  //! NO_BUFFER_MQ_HPP_INCLUDED
