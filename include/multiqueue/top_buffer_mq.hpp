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

#include "multiqueue/default_configuration.hpp"
#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/util/extractors.hpp"
#include "system_config.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <vector>

namespace multiqueue {

template <typename Key, typename T, typename Comparator = std::less<Key>, typename Configuration = DefaultConfiguration,
          typename HeapConfiguration = sequential::DefaultHeapConfiguration, typename Allocator = std::allocator<Key>>
class top_buffer_mq {
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
        std::optional<value_type> top = std::nullopt;

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

    inline void refill_top(std::size_t const index) {
        if (heap_list_[index].heap.empty()) {
            heap_list_[index].top = std::nullopt;
        } else {
            heap_list_[index].top = heap_list_[index].heap.top();
            heap_list_[index].heap.pop();
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

    void push(value_type const &value) {
        size_t index;
        do {
            index = random_queue_index();
        } while (!heap_list_[index].try_lock());
        if (!heap_list_[index].top) {
            heap_list_[index].top = value;
        } else if (comp_(value.first, heap_list_[index].top->first)) {
            heap_list_[index].heap.insert(std::move(std::exchange(*heap_list_[index].top, value)));
        } else {
            heap_list_[index].heap.insert(value);
        }
        heap_list_[index].unlock();
    }

    void push(value_type &&value) {
        size_t index;
        do {
            index = random_queue_index();
        } while (!heap_list_[index].try_lock());
        if (!heap_list_[index].top) {
            heap_list_[index].top = std::move(value);
        } else if (comp_(value.first, heap_list_[index].top->first)) {
            heap_list_[index].heap.insert(std::move(std::exchange(*heap_list_[index].top, std::move(value))));
        } else {
            heap_list_[index].heap.insert(std::move(value));
        }
        heap_list_[index].unlock();
    }

    bool extract_top(value_type &retval) {
        size_t first_index;
        for (unsigned int count = 0; count < 2; ++count) {
            do {
                first_index = random_queue_index();
            } while (!heap_list_[first_index].try_lock());
            if (heap_list_[first_index].top) {
                if (count == 1) {
                    retval = std::move(*heap_list_[first_index].top);
                    refill_top(first_index);
                    heap_list_[first_index].unlock();
                    return true;
                }
                // hold the lock for comparison
                break;
            } else {
                heap_list_[first_index].unlock();
            }
            if (count == 1) {
                return false;
            }
        }
        // When we get here, we hold the lock for the first heap, which has a nonempty buffer
        size_t second_index;
        do {
            second_index = random_queue_index();
        } while (!heap_list_[second_index].try_lock());
        if (heap_list_[second_index].top &&
            comp_(heap_list_[second_index].top->first, heap_list_[first_index].top->first)) {
            heap_list_[first_index].unlock();
            retval = std::move(*heap_list_[second_index].top);
            refill_top(second_index);
            heap_list_[second_index].unlock();
        } else {
            heap_list_[second_index].unlock();
            retval = std::move(*heap_list_[first_index].top);
            refill_top(first_index);
            heap_list_[first_index].unlock();
        }
        return true;
    }
};

}  // namespace multiqueue

#endif  //! TOP_BUFFER_MQ_HPP_INCLUDED
