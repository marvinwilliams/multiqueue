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
#include "system_config.hpp"

#ifdef HAVE_NUMA
#include <numa.h>
#endif
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <type_traits>

namespace multiqueue {

template <typename Key, typename T, typename Comparator>
struct multiqueue_base {
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<key_type, mapped_type>;
    using key_comparator = Comparator;
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
    struct alignas(L1_CACHE_LINESIZE) ThreadLocalData {
        std::mt19937 gen;
        std::uniform_int_distribution<size_type> dist;
        unsigned int insert_count = 0;
        unsigned int extract_count = 0;
        size_type insert_index;
        std::array<size_type, 2> extract_index;

        inline size_type get_random_index() {
            return dist(gen);
        }
    };

    ThreadLocalData *thread_data_;
    key_comparator comp_;

    explicit multiqueue_base(unsigned int const num_threads) : comp_() {
        thread_data_ = new ThreadLocalData[num_threads]();
    }

    explicit multiqueue_base(unsigned int const num_threads, key_comparator const &c) : comp_(c) {
        thread_data_ = new ThreadLocalData[num_threads]();
    }

    ~multiqueue_base() noexcept {
        delete[] thread_data_;
    }
};

template <typename Key, typename T, typename Comparator = std::less<Key>,
          typename Configuration = configuration::Default, typename Allocator = std::allocator<Key>>
class multiqueue : private multiqueue_base<Key, T, Comparator> {
   private:
    using base_type = multiqueue_base<Key, T, Comparator>;

   public:
    using allocator_type = Allocator;
    using key_type = typename base_type::key_type;
    using mapped_type = typename base_type::mapped_type;
    using value_type = typename base_type::value_type;
    using key_comparator = typename base_type::key_comparator;
    using size_type = typename base_type::size_type;
    struct Handle {
        friend class multiqueue;

       private:
        uint32_t id_;

       private:
        explicit Handle(unsigned int id) : id_{static_cast<uint32_t>(id)} {
        }
    };

   private:
    struct alignas(Configuration::NumaFriendly ? PAGESIZE : L1_CACHE_LINESIZE) InternalPriorityQueueWrapper {
        using pq_type = internal_priority_queue_t<key_type, mapped_type, key_comparator, Configuration>;
        using allocator_type = typename Configuration::HeapAllocator;
        static constexpr uint32_t lock_mask = static_cast<uint32_t>(1) << 31;
        static constexpr uint32_t pheromone_mask = lock_mask - 1;
        mutable std::atomic_uint32_t guard = Configuration::WithPheromones ? pheromone_mask : 0;
        pq_type pq;

        InternalPriorityQueueWrapper() = default;

        explicit InternalPriorityQueueWrapper(allocator_type const &alloc) : pq(alloc) {
        }

        explicit InternalPriorityQueueWrapper(Comparator const &comp, allocator_type const &alloc = allocator_type())
            : pq(comp, alloc) {
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
            return guard.compare_exchange_strong(lock_status, locked, std::memory_order_acquire,
                                                 std::memory_order_relaxed);
        }

        inline void unlock(uint32_t id) const noexcept {
            assert(guard == (Configuration::WithPheromones ? (lock_mask | static_cast<uint32_t>(id)) : lock_mask));
            guard.store(Configuration::WithPheromones ? static_cast<uint32_t>(id) : 0, std::memory_order_release);
        }
    };

    static_assert(std::is_same_v<value_type, typename InternalPriorityQueueWrapper::pq_type::heap_type::value_type>);

    using queue_alloc_type = typename allocator_type::template rebind<InternalPriorityQueueWrapper>::other;
    using alloc_traits = std::allocator_traits<queue_alloc_type>;
    using base_type::comp_;
    using base_type::thread_data_;

   private:
    InternalPriorityQueueWrapper *pq_list_;
    size_type pq_list_size_;
    queue_alloc_type alloc_;

   private:
    inline void refresh_insert_index(Handle handle) {
        thread_data_[handle.id_].insert_index = thread_data_[handle.id_].get_random_index();
        thread_data_[handle.id_].insert_count = Configuration::K;
    }

    inline void refresh_extract_indices(Handle handle) {
        thread_data_[handle.id_].extract_index[0] = thread_data_[handle.id_].get_random_index();
        do {
            thread_data_[handle.id_].extract_index[1] = thread_data_[handle.id_].get_random_index();
        } while (thread_data_[handle.id_].extract_index[1] == thread_data_[handle.id_].extract_index[0]);
        thread_data_[handle.id_].extract_count = Configuration::K;
    }

   public:
    explicit multiqueue(unsigned int const num_threads, allocator_type const &alloc = allocator_type())
        : base_type{num_threads}, pq_list_size_{num_threads * Configuration::C}, alloc_(alloc) {
        assert(num_threads >= 1);
#ifdef HAVE_NUMA
        if (Configuration::NumaFriendly) {
            numa_set_interleave_mask(numa_all_nodes_ptr);
        }
#endif
        pq_list_ = alloc_traits::allocate(alloc_, pq_list_size_);
        for (std::size_t i = 0; i < pq_list_size_; ++i) {
            alloc_traits::construct(alloc_, pq_list_ + i);
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
                pq_list_[i].pq.heap.reserve_and_touch(Configuration::ReservePerQueue);
                numa_set_preferred(-1);
            }
#else
            pq_list_[i].pq.heap.reserve(Configuration::ReservePerQueue);
#endif
        }
        typename std::uniform_int_distribution<size_type>::param_type params{0, pq_list_size_ - 1};
        for (std::size_t i = 0; i < num_threads; ++i) {
            thread_data_[i].dist.param(params);
        }
    }

    explicit multiqueue(unsigned int const num_threads, key_comparator const &comp,
                        allocator_type const &alloc = allocator_type())
        : base_type{num_threads, comp}, pq_list_size_{num_threads * Configuration::C}, alloc_(alloc) {
        assert(num_threads >= 1);
#ifdef HAVE_NUMA
        if (Configuration::NumaFriendly) {
            numa_set_interleave_mask(numa_all_nodes_ptr);
        }
#endif
        pq_list_ = std::allocator_traits<allocator_type>::allocate(alloc_, pq_list_size_);
        for (std::size_t i = 0; i < pq_list_size_; ++i) {
            std::allocator_traits<allocator_type>::construct(alloc_, pq_list_ + i, comp);
        }
#ifdef HAVE_NUMA
        if (Configuration::NumaFriendly) {
            numa_set_interleave_mask(numa_no_nodes_ptr);
        }
#endif
        for (std::size_t i = 0; i < pq_list_.size(); ++i) {
#ifdef HAVE_NUMA
            if (Configuration::NumaFriendly) {
                numa_set_preferred(
                    static_cast<int>(i / (pq_list_.size() / (static_cast<std::size_t>(numa_max_node()) + 1))));
                pq_list_[i].pq.heap.reserve_and_touch(Configuration::ReservePerQueue);
                numa_set_preferred(-1);
            }
#else
            pq_list_[i].pq.heap.reserve(Configuration::ReservePerQueue);
#endif
        }
        typename std::uniform_int_distribution<size_type>::param_type params{0, pq_list_size_ - 1};
        for (std::size_t i = 0; i < num_threads; ++i) {
            thread_data_[i].dist.param(params);
        }
    }

    ~multiqueue() noexcept {
        for (size_type i = 0; i < pq_list_size_; ++i) {
            alloc_traits::destroy(alloc_, pq_list_ + i);
        }
        alloc_traits::deallocate(alloc_, pq_list_, pq_list_size_);
    }

    Handle get_handle(unsigned int id) const noexcept {
        return Handle{id};
    }

    void push(Handle handle, value_type const &value) {
        bool claiming = false;
        if (thread_data_[handle.id_].insert_count == 0) {
            refresh_insert_index(handle);
            claiming = true;
        }
        size_type index = thread_data_[handle.id_].insert_index;
        while (!pq_list_[index].try_lock(handle.id_, claiming)) {
            refresh_insert_index(handle);
            index = thread_data_[handle.id_].insert_index;
            claiming = true;
        }
        /* std::cout << "lock " << index << '\n'; */
        pq_list_[index].pq.push(value);
        pq_list_[index].unlock(handle.id_);
        /* std::cout << "unlock " << index << '\n'; */
        --thread_data_[handle.id_].insert_count;
    }

    bool extract_top(Handle handle, value_type &retval) {
        bool claiming = false;
        if (thread_data_[handle.id_].extract_count == 0) {
            refresh_extract_indices(handle);
            claiming = true;
        }
        size_type first_index = thread_data_[handle.id_].extract_index[0];
        size_type second_index = thread_data_[handle.id_].extract_index[1];
        do {
            if (pq_list_[first_index].try_lock(handle.id_, claiming)) {
                /* std::cout << "1 lock " << first_index << '\n'; */
                if (pq_list_[second_index].try_lock(handle.id_, claiming)) {
                    /* std::cout << "2 lock " << second_index << '\n'; */
                    break;
                }
                pq_list_[first_index].unlock(handle.id_);
                /* std::cout << "unlock " << first_index << '\n'; */
            }
            refresh_extract_indices(handle);
            first_index = thread_data_[handle.id_].extract_index[0];
            second_index = thread_data_[handle.id_].extract_index[1];
            claiming = true;
            /* std::cout <<first_index << " " << second_index << '\n'; */
        } while (true);
        /* std::cout << "1 lock " << first_index << '\n'; */
        /* std::cout << "2 lock " << second_index << '\n'; */
        // When we get here, we hold the lock for both queues
        bool first_empty = !pq_list_[first_index].pq.refresh_top();
        if (first_empty) {
            pq_list_[first_index].unlock(handle.id_);
            /* std::cout << "1 unlock " << first_index << '\n'; */
        }
        bool second_empty = !pq_list_[second_index].pq.refresh_top();
        if (second_empty) {
            pq_list_[second_index].unlock(handle.id_);
            /* std::cout << "2 unlock " << second_index << '\n'; */
        }
        if (first_empty && second_empty) {
            refresh_extract_indices(handle);
            return false;
        }
        if (!first_empty && !second_empty) {
            if (comp_(pq_list_[second_index].pq.top().first, pq_list_[first_index].pq.top().first)) {
                std::swap(first_index, second_index);
            }
            pq_list_[second_index].unlock(handle.id_);
            /* std::cout << "unlock other " << first_index << '\n'; */
        } else if (first_empty) {
            first_index = second_index;
        }
        pq_list_[first_index].pq.extract_top(retval);
        pq_list_[first_index].unlock(handle.id_);
        /* std::cout << "unlock best " << first_index << '\n'; */
        if (first_empty || second_empty) {
            refresh_extract_indices(handle);
        }
        --thread_data_[handle.id_].extract_count;
        return true;
    }

    // threadsafe, but can be inaccurate if multiqueue is accessed
    bool extract_from_partition(Handle handle, value_type &retval) {
        for (size_type i = Configuration::C * handle.id_; i < Configuration::C * (handle.id_ + 1); ++i) {
            if (!pq_list_[i].try_lock(handle.id_, true)) {
                continue;
            }
            if (pq_list_[i].pq.refresh_top()) {
                pq_list_[i].pq.extract_top(retval);
                pq_list_[i].unlock(handle.id_);
                return true;
            }
            pq_list_[i].unlock(handle.id_);
        }
        return false;
    }

    static std::string description() {
        std::stringstream ss;
        ss << "multiqueue\n\t";
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
        if (Configuration::WithPheromones) {
            ss << "Using pheromones\n\t";
        }
        ss << "Preallocation for " << Configuration::ReservePerQueue << " elements per internal pq";
        return ss.str();
    }
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
