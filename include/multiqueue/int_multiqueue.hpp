/**
******************************************************************************
* @file:   multiqueue.hpp
*
* @author: Marvin Williams
* @date:   2021/03/29 17:19
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef MULTIQUEUE_HPP_INCLUDED
#define MULTIQUEUE_HPP_INCLUDED

#include "multiqueue/configurations.hpp"
#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/util/buffer.hpp"
#include "multiqueue/util/extractors.hpp"
#include "multiqueue/util/ring_buffer.hpp"
#include "sequential/heap/heap.hpp"
#include "system_config.hpp"

#ifdef HAVE_NUMA
#include <numa.h>
#endif
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <type_traits>

namespace multiqueue {

template <typename Key, typename T>
struct int_multiqueue_base {
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<key_type, mapped_type>;
    using key_comparator = std::less<Key>;
    using size_type = std::size_t;

    struct value_comparator : private key_comparator {
        explicit value_comparator(key_comparator const &comp) : key_comparator{comp} {
        }

        constexpr bool operator()(value_type const &lhs, value_type const &rhs) const {
            return static_cast<key_comparator const &>(*this)(util::get_nth<value_type>{}(lhs),
                                                              util::get_nth<value_type>{}(rhs));
        }
    };

   protected:
    struct alignas(2 * L1_CACHE_LINESIZE) ThreadData {
        std::mt19937 gen;
        std::uniform_int_distribution<size_type> dist;
        unsigned int insert_count = 0;
        std::array<unsigned int, 2> extract_count = {0, 0};
        size_type insert_index;
        std::array<size_type, 2> extract_index;

        inline size_type get_random_index() {
            return dist(gen);
        }
    };

    ThreadData *thread_data_;

    explicit int_multiqueue_base(unsigned int const num_threads, unsigned int const C, std::uint32_t seed) {
        thread_data_ = new ThreadData[num_threads]();
        auto params = typename std::uniform_int_distribution<size_type>::param_type{0, num_threads * C - 1};
        for (std::size_t i = 0; i < num_threads; ++i) {
            std::seed_seq seq{seed + i};
            thread_data_[i].gen.seed(seq);
            thread_data_[i].dist.param(params);
#ifdef ABORT_ON_MISALIGNMENT
            if (reinterpret_cast<std::uintptr_t>(&thread_data_[i]) % (2 * L1_CACHE_LINESIZE) != 0) {
                std::abort();
            }
#endif
        }
    }

    ~int_multiqueue_base() noexcept {
        delete[] thread_data_;
    }
};

template <typename Key, typename T, typename Configuration, bool UseMergeHeap>
struct alignas(Configuration::NumaFriendly ? PAGESIZE : 2 * L1_CACHE_LINESIZE) LocalPriorityQueue {
    using allocator_type = typename Configuration::HeapAllocator;
    using heap_type =
        sequential::key_value_heap<Key, T, std::less<Key>, Configuration::HeapDegree,
                                   typename Configuration::SiftStrategy, typename Configuration::HeapAllocator>;
    static constexpr uint32_t lock_mask = static_cast<uint32_t>(1) << 31;
    static constexpr uint32_t pheromone_mask = lock_mask - 1;
    static constexpr Key max_key = std::numeric_limits<Key>::max();

    mutable std::atomic_uint32_t guard = Configuration::WithPheromones ? pheromone_mask : 0;
    std::atomic<Key> top_key;
    util::buffer<typename heap_type::value_type, Configuration::InsertionBufferSize> insertion_buffer;
    util::ring_buffer<typename heap_type::value_type, Configuration::DeletionBufferSize> deletion_buffer;

    heap_type heap;

    explicit LocalPriorityQueue(allocator_type const &alloc = allocator_type()) : top_key(max_key), heap(alloc) {
    }

    inline void flush_insertion_buffer() {
        for (auto &v : insertion_buffer) {
            heap.insert(v);
        }
        insertion_buffer.clear();
    }

    void refresh_top() {
        assert(deletion_buffer.empty());
        flush_insertion_buffer();
        typename heap_type::value_type tmp;
        for (std::size_t i = 0; i < Configuration::DeletionBufferSize && !heap.empty(); ++i) {
            heap.extract_top(tmp);
            deletion_buffer.push_back(std::move(tmp));
        }
    }

    bool extract_top(typename heap_type::value_type &retval) {
        if (deletion_buffer.empty()) {
            return false;
        }
        retval = std::move(deletion_buffer.front());
        deletion_buffer.pop_front();
        if (deletion_buffer.empty()) {
            refresh_top();
        }
        if (deletion_buffer.empty()) {
            top_key.store(max_key, std::memory_order_release);
        } else {
            top_key.store(deletion_buffer.front().first, std::memory_order_release);
        }
        return true;
    };

    void push(typename heap_type::value_type const &value) {
        if (deletion_buffer.empty() || value.first < deletion_buffer.back().first) {
            if (deletion_buffer.full()) {
                if (insertion_buffer.full()) {
                    flush_insertion_buffer();
                    heap.insert(std::move(deletion_buffer.back()));
                } else {
                    insertion_buffer.push_back(std::move(deletion_buffer.back()));
                }
                deletion_buffer.pop_back();
            }
            std::size_t pos = deletion_buffer.size();
            for (; pos > 0 && value.first < deletion_buffer[pos - 1].first; --pos) {
            }
            deletion_buffer.insert_at(pos, value);
            if (pos == 0) {
                top_key.store(deletion_buffer.front().first, std::memory_order_release);
            }
            return;
        }
        if (insertion_buffer.full()) {
            flush_insertion_buffer();
            heap.insert(value);
            return;
        } else {
            insertion_buffer.push_back(value);
        }
    }

    inline bool empty() const noexcept {
        return deletion_buffer.empty();
    }

    inline bool try_lock(uint32_t id, bool claiming) const noexcept {
        uint32_t lock_status = guard.load(std::memory_order_relaxed);
        if ((lock_status >> 31) == 1) {
            return false;
        }
        if (Configuration::WithPheromones && !claiming && lock_status != static_cast<uint32_t>(id)) {
            return false;
        }
        uint32_t const locked = Configuration::WithPheromones ? (lock_mask | static_cast<uint32_t>(id)) : lock_mask;
        return guard.compare_exchange_strong(lock_status, locked, std::memory_order_acquire, std::memory_order_relaxed);
    }

    inline void unlock(uint32_t id) const noexcept {
        assert(guard == (Configuration::WithPheromones ? (lock_mask | static_cast<uint32_t>(id)) : lock_mask));
        guard.store(Configuration::WithPheromones ? static_cast<uint32_t>(id) : 0, std::memory_order_release);
    }
};

template <typename Key, typename T, typename Configuration>
struct alignas(Configuration::NumaFriendly ? PAGESIZE
                                           : 2 * L1_CACHE_LINESIZE) LocalPriorityQueue<Key, T, Configuration, true> {
    using heap_type = sequential::key_value_merge_heap<Key, T, std::less<Key>, Configuration::NodeSize,
                                                       typename Configuration::HeapAllocator>;
    using allocator_type = typename Configuration::HeapAllocator;

    static constexpr uint32_t lock_mask = static_cast<uint32_t>(1) << 31;
    static constexpr uint32_t pheromone_mask = lock_mask - 1;
    static constexpr auto max_key = std::numeric_limits<Key>::max();

    mutable std::atomic_uint32_t guard = Configuration::WithPheromones ? pheromone_mask : 0;
    std::atomic<Key> top_key;
    alignas(L1_CACHE_LINESIZE) util::buffer<typename heap_type::value_type, Configuration::NodeSize> insertion_buffer;
    util::ring_buffer<typename heap_type::value_type, Configuration::NodeSize * 2> deletion_buffer;
    heap_type heap;

    explicit LocalPriorityQueue(allocator_type const &alloc = allocator_type()) : heap(alloc) {
    }

    inline void flush_insertion_buffer() {
        assert(insertion_buffer.full());
        std::sort(insertion_buffer.begin(), insertion_buffer.end(),
                  [&](auto const &lhs, auto const &rhs) { return lhs.first < rhs.first; });
        for (std::size_t i = 0; i < insertion_buffer.size(); i += Configuration::NodeSize) {
            heap.insert(insertion_buffer.begin() + (i * Configuration::NodeSize),
                        insertion_buffer.begin() + ((i + 1) * Configuration::NodeSize));
        }
        insertion_buffer.clear();
    }

    void refresh_top() {
        assert(deletion_buffer.empty());
        if (insertion_buffer.full()) {
            flush_insertion_buffer();
        }
        if (!heap.empty()) {
            if (!insertion_buffer.empty()) {
                auto insert_it = std::partition(insertion_buffer.begin(), insertion_buffer.end(),
                                                [&](auto const &v) { return heap.top_node().back().first < v.first; });
                std::sort(insert_it, insertion_buffer.end(),
                          [&](auto const &lhs, auto const &rhs) { return rhs.first < lhs.first; });
                auto heap_it = heap.top_node().begin();
                for (auto current = insert_it; current != insertion_buffer.end(); ++current) {
                    while (heap_it->first < current->first) {
                        deletion_buffer.push_back(std::move(*heap_it++));
                    }
                    deletion_buffer.push_back(std::move(*current));
                }
                insertion_buffer.set_size(static_cast<std::size_t>(insert_it - insertion_buffer.begin()));
                std::move(heap_it, heap.top_node().end(), std::back_inserter(deletion_buffer));
            } else {
                std::move(heap.top_node().begin(), heap.top_node().end(), std::back_inserter(deletion_buffer));
            }
            heap.pop_node();
        } else if (!insertion_buffer.empty()) {
            std::sort(insertion_buffer.begin(), insertion_buffer.end(),
                      [&](auto const &lhs, auto const &rhs) { return lhs.first < rhs.first; });
            std::move(insertion_buffer.begin(), insertion_buffer.end(), std::back_inserter(deletion_buffer));
            insertion_buffer.clear();
        }
    }

    bool extract_top(typename heap_type::value_type &retval) {
        if (deletion_buffer.empty()) {
            return false;
        }
        retval = std::move(deletion_buffer.front());
        deletion_buffer.pop_front();
        if (deletion_buffer.empty()) {
            refresh_top();
        }
        if (deletion_buffer.empty()) {
            top_key.store(max_key, std::memory_order_release);
        } else {
            top_key.store(deletion_buffer.front().first, std::memory_order_release);
        }
        return true;
    };

    void push(typename heap_type::value_type const &value) {
        if (deletion_buffer.empty() || value.first < deletion_buffer.back().first) {
            if (deletion_buffer.full()) {
                if (insertion_buffer.full()) {
                    flush_insertion_buffer();
                }
                insertion_buffer.push_back(std::move(deletion_buffer.back()));
                deletion_buffer.pop_back();
            }
            std::size_t pos = deletion_buffer.size();
            for (; pos > 0 && heap.get_comparator()(value.first, deletion_buffer[pos - 1].first); --pos) {
            }
            deletion_buffer.insert_at(pos, value);
            if (pos == 0) {
                top_key.store(deletion_buffer.front().first, std::memory_order_release);
            }
            return;
        }
        if (insertion_buffer.full()) {
            flush_insertion_buffer();
        }
        insertion_buffer.push_back(value);
    }

    void pop() {
        assert(!deletion_buffer.empty());
        deletion_buffer.pop_front();
    }

    inline bool empty() const noexcept {
        return deletion_buffer.empty();
    }

    inline bool try_lock(uint32_t id, bool claiming) const noexcept {
        uint32_t lock_status = guard.load(std::memory_order_relaxed);
        if ((lock_status >> 31) == 1) {
            return false;
        }
        if (Configuration::WithPheromones && !claiming && lock_status != static_cast<uint32_t>(id)) {
            return false;
        }
        uint32_t const locked = Configuration::WithPheromones ? (lock_mask | static_cast<uint32_t>(id)) : lock_mask;
        return guard.compare_exchange_strong(lock_status, locked, std::memory_order_acquire, std::memory_order_relaxed);
    }

    inline void unlock(uint32_t id) const noexcept {
        assert(guard == (Configuration::WithPheromones ? (lock_mask | static_cast<uint32_t>(id)) : lock_mask));
        guard.store(Configuration::WithPheromones ? static_cast<uint32_t>(id) : 0, std::memory_order_release);
    }
};

template <typename Key, typename T, typename Configuration = configuration::Default,
          typename Allocator = std::allocator<Key>>
class int_multiqueue : private int_multiqueue_base<Key, T> {
    static_assert(std::is_unsigned_v<Key>, "Key must be unsigned integer");

   private:
    using base_type = int_multiqueue_base<Key, T>;
    using local_queue_type = LocalPriorityQueue<Key, T, Configuration, Configuration::UseMergeHeap>;
    static constexpr auto max_key = local_queue_type::max_key;

   public:
    using allocator_type = Allocator;
    using key_type = typename base_type::key_type;
    using mapped_type = typename base_type::mapped_type;
    using value_type = typename base_type::value_type;
    using key_comparator = typename base_type::key_comparator;
    using size_type = typename base_type::size_type;
    struct Handle {
        friend class int_multiqueue;

       private:
        uint32_t id_;

       private:
        explicit Handle(unsigned int id) : id_{static_cast<uint32_t>(id)} {
        }
    };

   private:
    static_assert(std::is_same_v<value_type, typename local_queue_type::heap_type::value_type>);

    using queue_alloc_type = typename allocator_type::template rebind<local_queue_type>::other;
    using alloc_traits = std::allocator_traits<queue_alloc_type>;
    using base_type::thread_data_;

   private:
    local_queue_type *pq_list_;
    size_type pq_list_size_;
    queue_alloc_type alloc_;

   public:
    explicit int_multiqueue(unsigned int const num_threads, std::uint32_t seed = 0,
                            allocator_type const &alloc = allocator_type())
        : base_type{num_threads, Configuration::C, seed}, pq_list_size_{num_threads * Configuration::C}, alloc_(alloc) {
        assert(num_threads >= 1);
#ifdef HAVE_NUMA
        if (Configuration::NumaFriendly) {
            numa_set_interleave_mask(numa_all_nodes_ptr);
        }
#endif
        pq_list_ = alloc_traits::allocate(alloc_, pq_list_size_);
        for (std::size_t i = 0; i < pq_list_size_; ++i) {
            alloc_traits::construct(alloc_, pq_list_ + i);
#ifdef ABORT_ON_MISALIGNMENT
            if (reinterpret_cast<std::uintptr_t>(&pq_list_[i]) % (2 * L1_CACHE_LINESIZE) != 0) {
                std::abort();
            }
#endif
        }
#ifdef HAVE_NUMA
        if (Configuration::NumaFriendly) {
            numa_set_interleave_mask(numa_no_nodes_ptr);
        }
#endif
        for (std::size_t i = 0; i < pq_list_size_; ++i) {
#ifdef HAVE_NUMA
            if (Configuration::NumaFriendly) {
                numa_set_preferred(
                    static_cast<int>(i / (pq_list_size_ / (static_cast<std::size_t>(numa_max_node()) + 1))));
                pq_list_[i].heap.reserve_and_touch(Configuration::ReservePerQueue);
                numa_set_preferred(-1);
            }
#else
            pq_list_[i].heap.reserve(Configuration::ReservePerQueue);
#endif
        }
    }

    ~int_multiqueue() noexcept {
        for (size_type i = 0; i < pq_list_size_; ++i) {
            alloc_traits::destroy(alloc_, pq_list_ + i);
        }
        alloc_traits::deallocate(alloc_, pq_list_, pq_list_size_);
    }

    Handle get_handle(unsigned int id) const noexcept {
        return Handle{id};
    }

    template <unsigned int K = Configuration::K, std::enable_if_t<(K == 1), int> = 0>
    void push(Handle handle, value_type const &value) {
        size_type index = thread_data_[handle.id_].get_random_index();
        while (!pq_list_[index].try_lock(handle.id_, true)) {
            index = thread_data_[handle.id_].get_random_index();
        }
        pq_list_[index].push(value);
        pq_list_[index].unlock(handle.id_);
    }

    template <unsigned int K = Configuration::K, std::enable_if_t<(K > 1), int> = 0>
    void push(Handle handle, value_type const &value) {
        auto& index = thread_data_[handle.id_].insert_index;
        if (thread_data_[handle.id_].insert_count == 0 ||
            !pq_list_[index].try_lock(handle.id_, false)) {
            do {
                index = thread_data_[handle.id_].get_random_index();
            } while (!pq_list_[index].try_lock(handle.id_, true));
            thread_data_[handle.id_].insert_count = Configuration::K;
        }
        pq_list_[index].push(value);
        pq_list_[index].unlock(handle.id_);
        --thread_data_[handle.id_].insert_count;
    }

    template <unsigned int K = Configuration::K, std::enable_if_t<(K == 1), int> = 0>
    bool extract_top(Handle handle, value_type &retval) {
        size_type first_index;
        size_type second_index;
        Key first_key;
        Key second_key;

        do {
            first_index = thread_data_[handle.id_].get_random_index();
            second_index = thread_data_[handle.id_].get_random_index();
            first_key = pq_list_[first_index].top_key.load(std::memory_order_relaxed);
            second_key = pq_list_[second_index].top_key.load(std::memory_order_relaxed);
            if (second_key < first_key) {
                first_index = second_index;
                first_key = second_key;
            }
            if (first_key == max_key) {
                return false;
            }
        } while (!pq_list_[first_index].try_lock(handle.id_, true));
        bool success = pq_list_[first_index].extract_top(retval);
        pq_list_[first_index].unlock(handle.id_);
        return success;
    }

    template <unsigned int K = Configuration::K, std::enable_if_t<(K > 1), int> = 0>
    bool extract_top(Handle handle, value_type &retval) {
        if (thread_data_[handle.id_].extract_count[0] == 0) {
            thread_data_[handle.id_].extract_index[0] = thread_data_[handle.id_].get_random_index();
            thread_data_[handle.id_].extract_count[0] = Configuration::K;
        }
        if (thread_data_[handle.id_].extract_count[1] == 0) {
            thread_data_[handle.id_].extract_index[1] = thread_data_[handle.id_].get_random_index();
            thread_data_[handle.id_].extract_count[0] = Configuration::K;
        }
        auto &first_index = thread_data_[handle.id_].extract_index[0];
        auto &second_index = thread_data_[handle.id_].extract_index[1];
        Key first_key = pq_list_[first_index].top_key.load(std::memory_order_relaxed);
        Key second_key = pq_list_[second_index].top_key.load(std::memory_order_relaxed);

        if (first_key == max_key && second_key == max_key) {
            thread_data_[handle.id_].extract_count[0] = 0;
            thread_data_[handle.id_].extract_count[1] = 0;
            return false;
        }

        if (second_key < first_key) {
            std::swap(first_index, second_index);
            std::swap(first_key, second_key);
            std::swap(thread_data_[handle.id_].extract_count[0], thread_data_[handle.id_].extract_count[1]);
        }

        if (!pq_list_[first_index].try_lock(handle.id_, thread_data_[handle.id_].extract_count[0] == Configuration::K)) {
            do {
                first_index = thread_data_[handle.id_].get_random_index();
                second_index = thread_data_[handle.id_].get_random_index();
                first_key = pq_list_[first_index].top_key.load(std::memory_order_relaxed);
                second_key = pq_list_[second_index].top_key.load(std::memory_order_relaxed);
                if (first_key == max_key && second_key == max_key) {
                    thread_data_[handle.id_].extract_count[0] = 0;
                    thread_data_[handle.id_].extract_count[1] = 0;
                    return false;
                }
                if (second_key < first_key) {
                  std::swap(first_index, second_index);
                  std::swap(first_key, second_key);
                  std::swap(thread_data_[handle.id_].extract_count[0], thread_data_[handle.id_].extract_count[1]);
                }
            } while (!pq_list_[first_index].try_lock(handle.id_, true));
            thread_data_[handle.id_].extract_count[0] = Configuration::K;
            thread_data_[handle.id_].extract_count[1] = Configuration::K;
        }

        bool success = pq_list_[first_index].extract_top(retval);
        pq_list_[first_index].unlock(handle.id_);
        if (success) {
            --thread_data_[handle.id_].extract_count[0];
        } else {
            thread_data_[handle.id_].extract_count[0] = 0;
        }
        if (second_key != max_key) {
            --thread_data_[handle.id_].extract_count[1];
        } else {
            thread_data_[handle.id_].extract_count[1] = 0;
        }
        return success;
    }

    bool extract_from_partition(Handle handle, value_type &retval) {
        for (size_type i = Configuration::C * handle.id_; i < Configuration::C * (handle.id_ + 1); ++i) {
            if (pq_list_[i].top_key.load(std::memory_order_acquire) == max_key ||
                !pq_list_[i].try_lock(handle.id_, true)) {
                continue;
            }
            bool success = pq_list_[i].extract_top(retval);
            pq_list_[i].unlock(handle.id_);
            if (success) {
                return true;
            }
        }
        return false;
    }

    static std::string description() {
        std::stringstream ss;
        ss << "int multiqueue\n\t";
        ss << "C: " << Configuration::C << "\n\t";
        ss << "K: " << Configuration::K << "\n\t";
        if (Configuration::UseMergeHeap) {
            ss << "Using merge heap, node size: " << Configuration::NodeSize << "\n\t";
        } else {
            if (Configuration::WithDeletionBuffer) {
                ss << "Using deletion buffer with size: " << Configuration::DeletionBufferSize << "\n\t";
            }
            if (Configuration::WithInsertionBuffer) {
                ss << "Using insertion buffer with size: " << Configuration::InsertionBufferSize << "\n\t";
            }
            ss << "Heap degree: " << Configuration::HeapDegree << "\n\t";
        }
        if (Configuration::NumaFriendly) {
            ss << "Numa friendly\n\t";
#ifndef HAVE_NUMA
            ss << "But numasupport disabled!\n\t";
#endif
        }
#ifdef ABORT_ON_MISALIGNMENT
        ss << "Abort on misalignment\n\t";
#endif
        if (Configuration::WithPheromones) {
            ss << "Using pheromones\n\t";
        }
        ss << "Preallocation for " << Configuration::ReservePerQueue << " elements per internal pq";
        return ss.str();
    }
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
