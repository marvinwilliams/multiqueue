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
#ifndef INT_MULTIQUEUE_ASSIGNED_HPP_INCLUDED
#define INT_MULTIQUEUE_ASSIGNED_HPP_INCLUDED

#include "multiqueue/configurations.hpp"
#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/util/buffer.hpp"
#include "multiqueue/util/extractors.hpp"
#include "multiqueue/util/ring_buffer.hpp"
#include "sequential/heap/heap.hpp"
#include "system_config.hpp"

#ifdef MULTIQUEUE_HAVE_NUMA
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
struct int_multiqueue_assigned_base {
    static_assert(std::is_unsigned_v<Key>, "Key must be unsigned integer");
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
        unsigned int insert_count = 0;
        unsigned int extract_count = 0;
    };

    static inline bool is_reserved(std::uint32_t i) {
        return (i >> 31) & 1u;
    }

    std::uint32_t reserve(unsigned int id, unsigned int num) {
        assert(num < 3);
        auto i = queue_index_[3 * id + num].index.load(std::memory_order_relaxed);
        while (!queue_index_[3 * id + num].index.compare_exchange_weak(i, i | (1u << 31), std::memory_order_acquire,
                                                                       std::memory_order_relaxed)) {
        }
        return i;
    }

    template <typename RNG>
    void swap_assignment(unsigned int id, unsigned int num, RNG &&g) {
        auto assignment = reserve(id, num);
        std::uniform_int_distribution<std::uint32_t> dist(0, pq_list_size_ - 1);
        do {
            auto swap_index = dist(g);
            auto other_assignment = queue_index_[swap_index].index.load(std::memory_order_relaxed);
            if (is_reserved(other_assignment)) {
                continue;
            }
            if (queue_index_[swap_index].index.compare_exchange_strong(
                    other_assignment, assignment, std::memory_order_acq_rel, std::memory_order_relaxed)) {
                queue_index_[3 * id + num].index.store(other_assignment, std::memory_order_acq_rel);
                break;
            }
        } while (true);
    }

    struct QueueIndex {
        alignas(2 * L1_CACHE_LINESIZE) std::atomic<std::uint32_t> index;
    };

    size_type pq_list_size_;
    ThreadData *thread_data_;
    QueueIndex *queue_index_;

    explicit int_multiqueue_assigned_base(unsigned int const num_threads, unsigned int const C, std::uint32_t seed)
        : pq_list_size_{num_threads * C} {
        thread_data_ = new ThreadData[num_threads]();
        for (std::size_t i = 0; i < num_threads; ++i) {
            std::seed_seq seq{seed + i};
            thread_data_[i].gen.seed(seq);
#ifdef MULTIQUEUE_ABORT_MISALIGNED
            if (reinterpret_cast<std::uintptr_t>(&thread_data_[i]) % (2 * L1_CACHE_LINESIZE) != 0) {
                std::abort();
            }
#endif
        }
        queue_index_ = new QueueIndex[pq_list_size_]();
        std::vector<unsigned int> indices(pq_list_size_);
        std::iota(indices.begin(), indices.end(), 0);
        std::seed_seq seq{seed + num_threads};
        std::mt19937 gen(seq);
        std::shuffle(indices.begin(), indices.end(), gen);
        for (std::size_t i = 0; i < indices.size(); ++i) {
            queue_index_[i].index = indices[i];
        }
    }

    ~int_multiqueue_assigned_base() noexcept {
        delete[] thread_data_;
        delete[] queue_index_;
    }
};

template <typename Key, typename T, typename Configuration>
struct alignas(Configuration::NumaFriendly ? PAGESIZE : 2 * L1_CACHE_LINESIZE) LocalPriorityQueueAssigned {
    using allocator_type = typename Configuration::HeapAllocator;
    using heap_type =
        sequential::key_value_heap<Key, T, std::less<Key>, Configuration::HeapDegree,
                                   typename Configuration::SiftStrategy, typename Configuration::HeapAllocator>;
    static constexpr uint32_t lock_mask = static_cast<uint32_t>(1) << 31;
    static constexpr Key max_key = std::numeric_limits<Key>::max();

    mutable std::atomic_uint32_t guard;
    std::atomic<Key> top_key;
    util::buffer<typename heap_type::value_type, Configuration::InsertionBufferSize> insertion_buffer;
    util::ring_buffer<typename heap_type::value_type, Configuration::DeletionBufferSize> deletion_buffer;

    heap_type heap;

    explicit LocalPriorityQueueAssigned(allocator_type const &alloc = allocator_type())
        : guard(0), top_key(max_key), heap(alloc) {
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

    inline std::size_t size() const noexcept {
        return insertion_buffer.size() + deletion_buffer.size() + heap.size();
    }

    inline bool try_lock() const noexcept {
        uint32_t lock_status = guard.load(std::memory_order_relaxed);
        if ((lock_status >> 31) == 1) {
            return false;
        }
        uint32_t const locked = lock_mask;
        return guard.compare_exchange_strong(lock_status, locked, std::memory_order_acquire, std::memory_order_relaxed);
    }

    inline void unlock() const noexcept {
        assert(guard == lock_mask);
        guard.store(0, std::memory_order_release);
    }
};

template <typename Key, typename T, typename Configuration = configuration::Default,
          typename Allocator = std::allocator<Key>>
class int_multiqueue_assigned : private int_multiqueue_assigned_base<Key, T> {
    static_assert(Configuration::WithDeletionBuffer == Configuration::WithInsertionBuffer,
                  "Must use either both or no buffers");

   private:
    using base_type = int_multiqueue_assigned_base<Key, T>;
    using local_queue_type = LocalPriorityQueueAssigned<Key, T, Configuration>;
    static constexpr auto max_key = local_queue_type::max_key;

   public:
    using allocator_type = Allocator;
    using key_type = typename base_type::key_type;
    using mapped_type = typename base_type::mapped_type;
    using value_type = typename base_type::value_type;
    using key_comparator = typename base_type::key_comparator;
    using size_type = typename base_type::size_type;
    struct Handle {
        friend class int_multiqueue_assigned;

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
    using base_type::pq_list_size_;
    using base_type::queue_index_;
    using base_type::thread_data_;

   private:
    local_queue_type *pq_list_;
    queue_alloc_type alloc_;

   public:
    explicit int_multiqueue_assigned(unsigned int const num_threads, std::uint32_t seed = 0,
                                     allocator_type const &alloc = allocator_type())
        : base_type{num_threads, Configuration::C, seed}, alloc_(alloc) {
        assert(num_threads >= 1);
#ifdef MULTIQUEUE_HAVE_NUMA
        if (Configuration::NumaFriendly) {
            numa_set_interleave_mask(numa_all_nodes_ptr);
        }
#endif
        pq_list_ = alloc_traits::allocate(alloc_, pq_list_size_);
        for (std::size_t i = 0; i < pq_list_size_; ++i) {
            alloc_traits::construct(alloc_, pq_list_ + i);
#ifdef MULTIQUEUE_ABORT_MISALIGNED
            if (reinterpret_cast<std::uintptr_t>(&pq_list_[i]) % (2 * L1_CACHE_LINESIZE) != 0) {
                std::abort();
            }
#endif
        }
#ifdef MULTIQUEUE_HAVE_NUMA
        if (Configuration::NumaFriendly) {
            numa_set_interleave_mask(numa_no_nodes_ptr);
        }
#endif
        for (std::size_t i = 0; i < pq_list_size_; ++i) {
#ifdef MULTIQUEUE_HAVE_NUMA
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

    ~int_multiqueue_assigned() noexcept {
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
        std::uniform_int_distribution<std::uint32_t> dist(0, pq_list_size_ - 1);
        size_type index;
        do {
            index = dist(thread_data_[handle.id_].gen);
        } while (!pq_list_[index].try_lock());
        pq_list_[index].push(value);
        pq_list_[index].unlock(handle.id_);
    }

    template <unsigned int K = Configuration::K, std::enable_if_t<(K > 1), int> = 0>
    void push(Handle handle, value_type const &value) {
        if (thread_data_[handle.id_].insert_count == 0) {
            this->swap_assignment(handle.id_, 0, thread_data_[handle.id_].gen);
            thread_data_[handle.id_].insert_count = Configuration::K;
        }
        auto index = queue_index_[3 * handle.id_].index.load(std::memory_order_relaxed);
        if (!pq_list_[index].try_lock()) {
            std::uniform_int_distribution<std::uint32_t> dist(0, pq_list_size_ - 1);
            do {
                index = dist(thread_data_[handle.id_].gen);
            } while (!pq_list_[index].try_lock());
        }
        pq_list_[index].push(value);
        pq_list_[index].unlock();
        --thread_data_[handle.id_].insert_count;
    }

    template <unsigned int K = Configuration::K, std::enable_if_t<(K == 1), int> = 0>
    bool extract_top(Handle handle, value_type &retval) {
        size_type first_index;
        size_type second_index;
        Key first_key;
        Key second_key;

        std::uniform_int_distribution<std::uint32_t> dist(0, pq_list_size_ - 1);
        do {
            first_index = dist(thread_data_[handle.id_].gen);
            second_index = dist(thread_data_[handle.id_].gen);
            first_key = pq_list_[first_index].top_key.load(std::memory_order_relaxed);
            second_key = pq_list_[second_index].top_key.load(std::memory_order_relaxed);
            if (second_key < first_key) {
                first_index = second_index;
                first_key = second_key;
            }
            if (first_key == max_key) {
                return false;
            }
        } while (!pq_list_[first_index].try_lock());
        bool success = pq_list_[first_index].extract_top(retval);
        pq_list_[first_index].unlock();
        return success;
    }

    template <unsigned int K = Configuration::K, std::enable_if_t<(K > 1), int> = 0>
    bool extract_top(Handle handle, value_type &retval) {
        if (thread_data_[handle.id_].extract_count == 0) {
            this->swap_assignment(handle.id_, 1, thread_data_[handle.id_].gen);
            this->swap_assignment(handle.id_, 2, thread_data_[handle.id_].gen);
            thread_data_[handle.id_].extract_count = Configuration::K;
        }
        auto first_index = queue_index_[3 * handle.id_ + 1].index.load(std::memory_order_relaxed);
        auto second_index = queue_index_[3 * handle.id_ + 2].index.load(std::memory_order_relaxed);
        Key first_key = pq_list_[first_index].top_key.load(std::memory_order_relaxed);
        Key second_key = pq_list_[second_index].top_key.load(std::memory_order_relaxed);

        if (first_key == max_key && second_key == max_key) {
            thread_data_[handle.id_].extract_count = 0;
            return false;
        }

        if (second_key < first_key) {
            std::swap(first_index, second_index);
            std::swap(first_key, second_key);
        }

        if (!pq_list_[first_index].try_lock()) {
            std::uniform_int_distribution<std::uint32_t> dist(0, pq_list_size_ - 1);
            do {
                first_index = dist(thread_data_[handle.id_].gen);
                second_index = dist(thread_data_[handle.id_].gen);
                first_key = pq_list_[first_index].top_key.load(std::memory_order_relaxed);
                second_key = pq_list_[second_index].top_key.load(std::memory_order_relaxed);
                if (first_key == max_key && second_key == max_key) {
                    thread_data_[handle.id_].extract_count = 0;
                    return false;
                }
                if (second_key < first_key) {
                    std::swap(first_index, second_index);
                    std::swap(first_key, second_key);
                }
            } while (!pq_list_[first_index].try_lock());
        }

        bool success = pq_list_[first_index].extract_top(retval);
        pq_list_[first_index].unlock();
        if (success) {
            --thread_data_[handle.id_].extract_count;
        } else {
            thread_data_[handle.id_].extract_count = 0;
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

    std::vector<std::size_t> get_distribution() const {
        std::vector<std::size_t> distribution(pq_list_size_);
        std::transform(pq_list_, pq_list_ + pq_list_size_, distribution.begin(),
                       [](auto const &pq_wrapper) { return pq_wrapper.size(); });
        return distribution;
    }

    std::vector<std::size_t> get_top_distribution(std::size_t k) {
        std::vector<std::pair<value_type, std::size_t>> removed_elements;
        removed_elements.reserve(k);
        std::vector<std::size_t> distribution(pq_list_size_, 0);
        for (std::size_t i = 0; i < k; ++i) {
            auto min = std::min_element(
                pq_list_, pq_list_ + pq_list_size_, [](local_queue_type const &lhs, local_queue_type const &rhs) {
                    return lhs.top_key.load(std::memory_order_relaxed) < rhs.top_key.load(std::memory_order_relaxed);
                });
            if (min->top_key.load(std::memory_order_relaxed) == max_key) {
                break;
            }
            assert(!min->empty());
            std::pair<value_type, std::size_t> result;
            [[maybe_unused]] bool success = min->extract_top(result.first);
            assert(success);
            result.second = static_cast<std::size_t>(std::distance(pq_list_, min));
            removed_elements.push_back(result);
            ++distribution[result.second];
        }
        for (auto [val, index] : removed_elements) {
            pq_list_[index].push(val);
        }
        return distribution;
    }

    void push_in_queue(value_type const &value, std::size_t index) {
    }

    static std::string description() {
        std::stringstream ss;
        ss << "int multiqueue assignment\n\t";
        ss << "C: " << Configuration::C << "\n\t";
        ss << "K: " << Configuration::K << "\n\t";
        if (Configuration::WithDeletionBuffer) {
            ss << "Using deletion buffer with size: " << Configuration::DeletionBufferSize << "\n\t";
        }
        if (Configuration::WithInsertionBuffer) {
            ss << "Using insertion buffer with size: " << Configuration::InsertionBufferSize << "\n\t";
        }
        ss << "Heap degree: " << Configuration::HeapDegree << "\n\t";
        if (Configuration::NumaFriendly) {
            ss << "Numa friendly\n\t";
#ifndef MULTIQUEUE_HAVE_NUMA
            ss << "But numasupport disabled!\n\t";
#endif
        }
#ifdef MULTIQUEUE_ABORT_MISALIGNED
        ss << "Abort on misalignment\n\t";
#endif
        ss << "Preallocation for " << Configuration::ReservePerQueue << " elements per internal pq";
        return ss.str();
    }
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
