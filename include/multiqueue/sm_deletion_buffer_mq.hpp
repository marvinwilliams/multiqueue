/**
******************************************************************************
* @file:   sm_deletion_buffer_mq.hpp
*
* @author: Marvin Williams
* @date:   2021/02/22 11:22
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef SM_DELETION_BUFFER_MQ_HPP_INCLUDED
#define SM_DELETION_BUFFER_MQ_HPP_INCLUDED

#include <iostream>

#include "multiqueue/sequential/heap/full_down_strategy.hpp"
#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/util/extractors.hpp"
#include "multiqueue/util/range_iterator.hpp"

#include <x86intrin.h>
#include <array>
#include <atomic>
#include <cassert>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <type_traits>
#include <vector>

namespace multiqueue {
namespace rsm {

// TODO: CMake defined
static constexpr unsigned int CACHE_LINESIZE = 64;

template <typename ValueType>
struct SmDeletionBufferConfiguration {
    using key_type = typename ValueType::first_type;
    using mapped_type = typename ValueType::second_type;
    // With `p` threads, use `C*p` queues
    static constexpr unsigned int C = 4;
    // The underlying sequential priority queue to use
    static constexpr unsigned int HeapDegree = 4;
    // Buffer size
    static constexpr size_t BufferSize = 16;
    // The sentinel
    static constexpr key_type Sentinel = std::numeric_limits<key_type>::max();
    // The sifting strategy to use
    using SiftStrategy = local_nonaddressable::full_down_strategy;
    // The allocator to use in the underlying sequential priority queue
    using HeapAllocator = std::allocator<ValueType>;
};

template <typename Key, typename T, typename Comparator = std::less<Key>,
          template <typename> typename Configuration = SmDeletionBufferConfiguration,
          typename Allocator = std::allocator<Key>>
class sm_deletion_buffer_mq {
   public:
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<key_type, mapped_type>;
    using key_comparator = Comparator;

   private:
    using config_type = Configuration<value_type>;
    static constexpr unsigned int C = config_type::C;
    static constexpr key_type Sentinel = config_type::Sentinel;
    static constexpr size_t BufferSize = config_type::BufferSize;

    using heap_type = local_nonaddressable::heap<value_type, key_type, util::get_nth<value_type>, key_comparator,
                                                 config_type::HeapDegree, typename config_type::SiftStrategy,
                                                 typename config_type::HeapAllocator>;

    struct alignas(CACHE_LINESIZE) guarded_heap {
        using allocator_type = typename heap_type::allocator_type;

        enum class LockFlag : std::uint64_t {
            Queue = static_cast<std::uint64_t>(1) << 63,
            Buffer = static_cast<std::uint64_t>(1) << 62
        };

        std::atomic_uint64_t use_count = 0;
        heap_type heap;
        std::array<value_type, BufferSize> buffer{};
        std::atomic_uint32_t buffer_pos = 0;
        std::uint32_t buffer_end = 0;

        explicit guarded_heap() = default;
        explicit guarded_heap(allocator_type const &alloc) : heap{alloc} {
        }

        inline bool buffer_empty() const noexcept {
            return buffer_pos == buffer_end;
        };

        template <LockFlag... flags>
        constexpr bool is_set(std::uint64_t const c) const noexcept {
            return (((c & static_cast<std::uint64_t>(flags)) == static_cast<std::uint64_t>(flags)) && ...);
        }

        template <LockFlag... flags>
        constexpr std::uint64_t set(std::uint64_t const c) const noexcept {
            return (c | ... | static_cast<std::uint64_t>(flags));
        }

        template <LockFlag... flags>
        constexpr std::uint64_t unset(std::uint64_t const c) const noexcept {
            return c & ~(static_cast<std::uint64_t>(flags) | ...);
        }

        inline int try_lock_buffer_shared() noexcept {
            for (std::uint64_t c = use_count.load(std::memory_order_relaxed); !is_set<LockFlag::Buffer>(c);
                 c = use_count.load(std::memory_order_relaxed)) {
                // Use strong variant because it is expected to succeed
                if (use_count.compare_exchange_strong(c, c + 1, std::memory_order_acquire, std::memory_order_relaxed)) {
                    return static_cast<int>(unset<LockFlag::Buffer, LockFlag::Queue>(c));
                }
            }
            return -1;
        }

        inline void unlock_buffer_shared() noexcept {
            use_count.fetch_sub(1, std::memory_order_release);
        }

        inline bool try_lock_queue_exclusive() noexcept {
            for (std::uint64_t c = use_count.load(std::memory_order_relaxed);
                 !is_set<LockFlag::Queue>(c) && !is_set<LockFlag::Buffer>(c);
                 c = use_count.load(std::memory_order_relaxed)) {
                // Use strong variant because it is expected to succeed
                if (use_count.compare_exchange_strong(c, set<LockFlag::Queue>(c), std::memory_order_acquire,
                                                      std::memory_order_relaxed)) {
                    return true;
                }
            }
            return false;
        }

        inline void unlock_queue_exclusive() noexcept {
            std::uint64_t c;
            do {
                c = use_count.load(std::memory_order_relaxed);
            } while (!use_count.compare_exchange_strong(c, unset<LockFlag::Queue>(c), std::memory_order_release));
        }

        // Needs to hold shared before locking exclusive.
        // Only one thread should try to gain exclusive access at a time. Waits until the queue is unlocked. Also, an
        // ABA problem could occur.
        inline void lock_all_exclusive() noexcept {
            std::uint64_t c;
            do {
                /* std::cerr << "loop 3" << std::endl; */
                c = use_count.load(std::memory_order_relaxed);
                while (is_set<LockFlag::Buffer>(c) || is_set<LockFlag::Queue>(c)) {
                /* std::cerr << "loop 4" << std::endl; */
                    _mm_pause();
                    c = use_count.load(std::memory_order_relaxed);
                }
            } while (!use_count.compare_exchange_strong(c, set<LockFlag::Queue, LockFlag::Buffer>(c),
                                                        std::memory_order_acq_rel, std::memory_order_relaxed));
            c = use_count.load(std::memory_order_relaxed);
            while (unset<LockFlag::Queue, LockFlag::Buffer>(c) != 1u) {
                /* std::cerr << "loop 5" << std::endl; */
                _mm_pause();
                c = use_count.load(std::memory_order_relaxed);
            }
            std::atomic_thread_fence(std::memory_order_acquire);
        }

        inline void unlock_all_exclusive() noexcept {
            // After unlocking, we are the only thread holding a shared lock
            use_count.store(static_cast<std::uint64_t>(1), std::memory_order_release);
        }

        inline void refill_buffer() {
            lock_all_exclusive();
            std::uint32_t count = 0;
            while (count < BufferSize && !heap.empty()) {
                buffer[count] = heap.top();
                heap.pop();
                ++count;
            }
            buffer_end = count;
            buffer_pos.store(0, std::memory_order_relaxed);
            unlock_all_exclusive();
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
    explicit sm_deletion_buffer_mq(unsigned int const num_threads)
        : heap_list_(num_threads * C), num_queues_{static_cast<unsigned int>(heap_list_.size())}, comp_{} {
        assert(num_threads >= 1);
    }

    explicit sm_deletion_buffer_mq(unsigned int const num_threads, allocator_type const &alloc)
        : heap_list_(num_threads * C, alloc), num_queues_{static_cast<unsigned int>(heap_list_.size())}, comp_{} {
        assert(num_threads >= 1);
    }

    void push(value_type const &value) {
        size_t index;
        //  << "start pushing" << std::endl;
        do {
            index = random_queue_index();
        } while (!heap_list_[index].try_lock_queue_exclusive());
        //  << "pushing" << std::endl;
        heap_list_[index].heap.insert(value);
        heap_list_[index].unlock_queue_exclusive(index);
        //  << "end pushing" << std::endl;
    }

    void push(value_type &&value) {
        size_t index;
        //  << "start pushing" << std::endl;
        do {
            index = random_queue_index();
        } while (!heap_list_[index].try_lock_queue_exclusive());
        //  << "pushing" << std::endl;
        heap_list_[index].heap.insert(std::move(value));
        heap_list_[index].unlock_queue_exclusive();
        //  << "end pushing" << std::endl;
    }

    /* bool extract_top(value_type &retval) { */
    /*     std::size_t first_index; */
    /*     std::size_t first_buffer_pos; */
    /*     //  << "Start extracting" << std::endl; */
    /*     for (unsigned int count = 0; count < 2; ++count) { */
    /*         do { */
    /*             first_index = random_queue_index(); */
    /*         } while (!heap_list_[first_index].try_lock_buffer_shared()); */
    /*         //  << "Found first index" << std::endl; */
    /*         first_buffer_pos = heap_list_[first_index].buffer_pos.fetch_add(1, std::memory_order_relaxed); */
    /*         // Refill the buffer if we are the first one who encounter an empty puffer */
    /*         if (first_buffer_pos == heap_list_[first_index].buffer_end) { */
    /*             //  << "Refilling first" << std::endl; */
    /*             heap_list_[first_index].refill_buffer(); */
    /*             //  << "Refilling done" << std::endl; */
    /*             first_buffer_pos = 0; */
    /*         } */
    /*         if (first_buffer_pos < heap_list_[first_index].buffer_end) { */
    /*             //  << "Buffer nonempty" << std::endl; */
    /*             if (count == 1) { */
    /*                 retval = std::move(heap_list_[first_index].buffer[first_buffer_pos]); */
    /*                 //  << "Returning first element" << std::endl; */
    /*                 heap_list_[first_index].unlock_buffer_shared(); */
    /*                 //  << "Returned element" << std::endl; */
    /*                 return true; */
    /*             } */
    /*             // hold the lock for comparison */
    /*             break; */
    /*         } else { */
    /*             // Queue empty */
    /*             //  << "Visited Queue empty" << std::endl; */
    /*             heap_list_[first_index].unlock_buffer_shared(); */
    /*             //  << "Unlocked empty queue" << std::endl; */
    /*         } */
    /*         if (count == 1) { */
    /*             //  << "Seen two empty queues" << std::endl; */
    /*             return false; */
    /*         } */
    /*     } */
    /*     // When we get here, we hold the shared lock for the first heap, which has a nonempty buffer */
    /*     std::size_t second_index; */
    /*     std::size_t second_buffer_pos; */
    /*     do { */
    /*         second_index = random_queue_index(); */
    /*     } while (second_index == first_index || !heap_list_[second_index].try_lock_buffer_shared()); */
    /*     //  << "Found second index" << std::endl; */
    /*     second_buffer_pos = heap_list_[second_index].buffer_pos.fetch_add(1, std::memory_order_relaxed); */
    /*     // Refill the buffer if we are the first one who encounter an empty puffer */
    /*     if (second_buffer_pos == heap_list_[second_index].buffer_end) { */
    /*         //  << "Refilling second" << std::endl; */
    /*         heap_list_[second_index].refill_buffer(); */
    /*         //  << "Refilling done" << std::endl; */
    /*         second_buffer_pos = 0; */
    /*     } */
    /*     if (second_buffer_pos < heap_list_[second_index].buffer_end && */
    /*         //  << "Buffer nonempty" << std::endl; */
    /*         comp_(heap_list_[second_index].buffer[second_buffer_pos].first, */
    /*               heap_list_[first_index].buffer[first_buffer_pos].first)) { */
    /*         //  << "Second is smaller" << std::endl; */
    /*         heap_list_[first_index].unlock_buffer_shared(); */
    /*         retval = std::move(heap_list_[second_index].buffer[second_buffer_pos]); */
    /*         //  << "Unlocking second" << std::endl; */
    /*         heap_list_[second_index].unlock_buffer_shared(); */
    /*     } else { */
    /*         //  << "First is smaller" << std::endl; */
    /*         heap_list_[second_index].unlock_buffer_shared(); */
    /*         retval = std::move(heap_list_[first_index].buffer[first_buffer_pos]); */
    /*         //  << "Unlocking first" << std::endl; */
    /*         heap_list_[first_index].unlock_buffer_shared(); */
    /*     } */
    /*     //  << "Unlocked last, returning" << std::endl; */
    /*     return true; */
    /* } */

    bool extract_top(value_type &retval) {
        //  << "Extracting" << std::endl;
        do {
            std::size_t first_index;
            std::uint32_t first_buffer_pos;
            int id;
            bool first_empty = false;
            do {
                first_index = random_queue_index();
                /* std::cerr << "loop 1" << std::endl; */
            } while ((id = heap_list_[first_index].try_lock_buffer_shared()) < 0);
            //  << "Found first index" << std::endl;
            first_buffer_pos = heap_list_[first_index].buffer_pos.load(std::memory_order_relaxed);
            // Refill the buffer if we are the first one who encounter an empty puffer
            if (id == 0 && first_buffer_pos == heap_list_[first_index].buffer_end) {
                //  << "Refilling first buffer" << std::endl;
                heap_list_[first_index].refill_buffer();
                //  << "Done refilling buffer" << std::endl;
                first_buffer_pos = 0;
            }
            if (first_buffer_pos >= heap_list_[first_index].buffer_end) {
                heap_list_[first_index].unlock_buffer_shared();
                first_empty = true;
            }

            // When we get here, we hold the shared lock for the first heap, which has a nonempty buffer
            std::size_t second_index;
            std::uint32_t second_buffer_pos;
            do {
                second_index = random_queue_index();
                /* std::cerr << "loop 2" << std::endl; */
            } while (second_index == first_index || (id = heap_list_[second_index].try_lock_buffer_shared()) < 0);
            //  << "Found second" << std::endl;
            second_buffer_pos = heap_list_[second_index].buffer_pos.load(std::memory_order_relaxed);
            // Refill the buffer only if we already released the first buffer, otherwise we might deadlock
            if (id == 0 && second_buffer_pos == heap_list_[second_index].buffer_end) {
                //  << "Refilling second buffer" << std::endl;
                heap_list_[second_index].refill_buffer();
                //  << "Done refilling buffer" << std::endl;
                second_buffer_pos = 0;
            }
            if (second_buffer_pos >= heap_list_[second_index].buffer_end) {
                //  << "Second buffer empty" << std::endl;
                heap_list_[second_index].unlock_buffer_shared();
                if (first_empty) {
                    //  << "Both empty, returning" << std::endl;
                    return false;
                }
                //  << "Returning first" << std::endl;
                if (heap_list_[first_index].buffer_pos.compare_exchange_strong(
                        first_buffer_pos, first_buffer_pos + 1, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                    retval = std::move(heap_list_[first_index].buffer[first_buffer_pos]);
                    heap_list_[first_index].unlock_buffer_shared();
                    //  << "Done" << std::endl;
                    return true;
                }
                heap_list_[first_index].unlock_buffer_shared();
                continue;
            }
            if (first_empty ||
                comp_(heap_list_[second_index].buffer[second_buffer_pos].first,
                      heap_list_[first_index].buffer[first_buffer_pos].first)) {
                //  << "Second smaller" << std::endl;
                if (!first_empty) {
                    //  << "...because first is empty" << std::endl;
                    heap_list_[first_index].unlock_buffer_shared();
                }
                //  << "Returning second" << std::endl;
                if (heap_list_[second_index].buffer_pos.compare_exchange_strong(
                        second_buffer_pos, second_buffer_pos + 1, std::memory_order_acq_rel,
                        std::memory_order_relaxed)) {
                    retval = std::move(heap_list_[second_index].buffer[second_buffer_pos]);
                    heap_list_[second_index].unlock_buffer_shared();
                    //  << "Done" << std::endl;
                    return true;
                }
                heap_list_[second_index].unlock_buffer_shared();
                continue;
            }
            heap_list_[second_index].unlock_buffer_shared();
            //  << "First smaller" << std::endl;
            if (heap_list_[first_index].buffer_pos.compare_exchange_strong(
                    first_buffer_pos, first_buffer_pos + 1, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                //  << "Returning first" << std::endl;
                retval = std::move(heap_list_[first_index].buffer[first_buffer_pos]);
                heap_list_[first_index].unlock_buffer_shared();
                //  << "Done" << std::endl;
                return true;
            }
            heap_list_[first_index].unlock_buffer_shared();
        } while (true);
        return false;
    }
};

}  // namespace rsm
}  // namespace multiqueue

#endif  //! SM_DELETION_BUFFER_MQ_HPP_INCLUDED
