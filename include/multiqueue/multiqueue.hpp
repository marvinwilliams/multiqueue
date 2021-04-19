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
#include "multiqueue/sequential/heap/merge_heap.hpp"
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
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <type_traits>

namespace multiqueue {

template <bool isMerging, bool WithInsertionBuffer, bool WithDeletionBuffer, typename Key, typename T,
          typename Comparator, typename Configuration>
struct PriorityQueueConfiguration;

template <typename Key, typename T, typename Comparator, typename Configuration>
struct PriorityQueueConfiguration<false, false, false, Key, T, Comparator, Configuration> {
    using heap_type =
        sequential::key_value_heap<Key, T, Comparator, Configuration::HeapDegree, typename Configuration::SiftStrategy,
                                   typename Configuration::HeapAllocator>;
    using allocator_type = typename Configuration::HeapAllocator;

    heap_type heap;

    PriorityQueueConfiguration() = default;

    explicit PriorityQueueConfiguration(allocator_type const &alloc) : heap(alloc) {
    }

    explicit PriorityQueueConfiguration(Comparator const &comp, allocator_type const &alloc = allocator_type())
        : heap(comp, alloc) {
    }
    inline typename heap_type::value_type const &top() {
        return heap.top();
    }

    inline void extract_top(typename heap_type::value_type &retval) {
        heap.extract_top(retval);
    }

    inline bool refresh_top() noexcept {
        return !heap.empty();
    }

    inline void push(typename heap_type::value_type const &value) {
        heap.insert(value);
    }

    inline void pop() {
        heap.pop();
    }

    inline bool empty() const noexcept {
      return heap.empty();
    }
};

template <typename Key, typename T, typename Comparator, typename Configuration>
struct PriorityQueueConfiguration<false, true, false, Key, T, Comparator, Configuration> {
    using heap_type =
        sequential::key_value_heap<Key, T, Comparator, Configuration::HeapDegree, typename Configuration::SiftStrategy,
                                   typename Configuration::HeapAllocator>;
    using allocator_type = typename Configuration::HeapAllocator;

    util::buffer<typename heap_type::value_type, Configuration::InsertionBufferSize> insertion_buffer;
    heap_type heap;

    PriorityQueueConfiguration() = default;

    explicit PriorityQueueConfiguration(allocator_type const &alloc) : heap(alloc) {
    }

    explicit PriorityQueueConfiguration(Comparator const &comp, allocator_type const &alloc = allocator_type())
        : heap(comp, alloc) {
    }

    inline typename heap_type::value_type const &top() {
        assert(insertion_buffer.empty());
        return heap.top();
    }

    inline void extract_top(typename heap_type::value_type &retval) {
        assert(insertion_buffer.empty());
        heap.extract_top(retval);
    }

    inline bool refresh_top() {
        for (auto &v : insertion_buffer) {
            heap.insert(std::move(v));
        }
        insertion_buffer.clear();
        return !heap.empty();
    }

    inline void push(typename heap_type::value_type const &value) {
        if (insertion_buffer.size() != Configuration::InsertionBufferSize) {
            insertion_buffer.push_back(value);
        } else {
            refresh_top();
            heap.insert(value);
        }
    }

    inline void pop() {
        assert(insertion_buffer.empty());
        heap.pop();
    }

    inline bool empty() const noexcept {
      return insertion_buffer.empty() && heap.empty();
    }
};

template <typename Key, typename T, typename Comparator, typename Configuration>
struct PriorityQueueConfiguration<false, false, true, Key, T, Comparator, Configuration> {
    using heap_type =
        sequential::key_value_heap<Key, T, Comparator, Configuration::HeapDegree, typename Configuration::SiftStrategy,
                                   typename Configuration::HeapAllocator>;
    using allocator_type = typename Configuration::HeapAllocator;

    util::ring_buffer<typename heap_type::value_type, Configuration::DeletionBufferSize> deletion_buffer;
    heap_type heap;

    PriorityQueueConfiguration() = default;

    explicit PriorityQueueConfiguration(allocator_type const &alloc) : heap(alloc) {
    }

    explicit PriorityQueueConfiguration(Comparator const &comp, allocator_type const &alloc = allocator_type())
        : heap(comp, alloc) {
    }

    inline typename heap_type::value_type const &top() {
        assert(!deletion_buffer.empty());
        return deletion_buffer.front();
    }

    inline bool refresh_top() {
        if (!deletion_buffer.empty()) {
            return true;
        }
        typename heap_type::value_type tmp;
        for (std::size_t i = 0; i < Configuration::DeletionBufferSize && !heap.empty(); ++i) {
            heap.extract_top(tmp);
            deletion_buffer.push_back(std::move(tmp));
        }
        return !deletion_buffer.empty();
    }

    inline void extract_top(typename heap_type::value_type &retval) {
        assert(!deletion_buffer.empty());
        retval = std::move(deletion_buffer.front());
        deletion_buffer.pop_front();
    };

    inline void push(typename heap_type::value_type const &value) {
        if (deletion_buffer.empty() || !heap.get_comparator()(value.first, deletion_buffer.back().first)) {
            heap.insert(value);
        } else {
            if (deletion_buffer.full()) {
                heap.insert(std::move(deletion_buffer.back()));
                deletion_buffer.pop_back();
            }
            std::size_t pos = deletion_buffer.size();
            for (; pos > 0 && heap.get_comparator()(value.first, deletion_buffer[pos - 1].first); --pos) {
            }
            deletion_buffer.insert_at(pos, value);
        }
    }

    inline void pop() {
        assert(!deletion_buffer.empty());
        deletion_buffer.pop_front();
    }

    inline bool empty() const noexcept {
      return deletion_buffer.empty() && heap.empty();
    }
};

template <typename Key, typename T, typename Comparator, typename Configuration>
struct PriorityQueueConfiguration<false, true, true, Key, T, Comparator, Configuration> {
    using heap_type =
        sequential::key_value_heap<Key, T, Comparator, Configuration::HeapDegree, typename Configuration::SiftStrategy,
                                   typename Configuration::HeapAllocator>;
    using allocator_type = typename Configuration::HeapAllocator;
    util::buffer<typename heap_type::value_type, Configuration::InsertionBufferSize> insertion_buffer;
    util::ring_buffer<typename heap_type::value_type, Configuration::DeletionBufferSize> deletion_buffer;

    heap_type heap;

    inline typename heap_type::value_type const &top() {
        assert(!deletion_buffer.empty());
        return deletion_buffer.front();
    }

    inline void flush_insertion_buffer() {
        for (auto &v : insertion_buffer) {
            heap.insert(std::move(v));
        }
        insertion_buffer.clear();
    }

    bool refresh_top() {
        if (!deletion_buffer.empty()) {
            return true;
        }
        flush_insertion_buffer();
        typename heap_type::value_type tmp;
        for (std::size_t i = 0; i < Configuration::DeletionBufferSize && !heap.empty(); ++i) {
            heap.extract_top(tmp);
            deletion_buffer.push_back(std::move(tmp));
        }
        return !deletion_buffer.empty();
    }

    void extract_top(typename heap_type::value_type &retval) {
        assert(!deletion_buffer.empty());
        retval = std::move(deletion_buffer.front());
        deletion_buffer.pop_front();
    };

    void push(typename heap_type::value_type const &value) {
        if (!deletion_buffer.empty() && heap.get_comparator()(value.first, deletion_buffer.back().first)) {
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
            for (; pos > 0 && heap.get_comparator()(value.first, deletion_buffer[pos - 1].first); --pos) {
            }
            deletion_buffer.insert_at(pos, value);
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

    void pop() {
        assert(!deletion_buffer.empty());
        deletion_buffer.pop_front();
    }

    inline bool empty() const noexcept {
      return insertion_buffer.empty() && deletion_buffer.empty() && heap.empty();
    }
};

template <typename Key, typename T, typename Comparator, typename Configuration>
struct PriorityQueueConfiguration<true, true, true, Key, T, Comparator, Configuration> {
    using heap_type = sequential::key_value_merge_heap<Key, T, Comparator, Configuration::NodeSize,
                                                       typename Configuration::HeapAllocator>;
    using allocator_type = typename Configuration::HeapAllocator;

    util::buffer<typename heap_type::value_type, Configuration::NodeSize> insertion_buffer;
    util::ring_buffer<typename heap_type::value_type, Configuration::NodeSize * 2> deletion_buffer;
    heap_type heap;

    PriorityQueueConfiguration() = default;

    explicit PriorityQueueConfiguration(allocator_type const &alloc) : heap(alloc) {
    }

    explicit PriorityQueueConfiguration(Comparator const &comp, allocator_type const &alloc = allocator_type())
        : heap(comp, alloc) {
    }

    inline typename heap_type::value_type const &top() {
        assert(!deletion_buffer.empty());
        return deletion_buffer.front();
    }

    inline void flush_insertion_buffer() {
        assert(insertion_buffer.full());
        std::sort(insertion_buffer.begin(), insertion_buffer.end(),
                  [&](auto const &lhs, auto const &rhs) { return heap.get_comparator()(lhs.first, rhs.first); });
        for (std::size_t i = 0; i < insertion_buffer.size(); i += Configuration::NodeSize) {
            heap.insert(insertion_buffer.begin() + (i * Configuration::NodeSize),
                        insertion_buffer.begin() + ((i + 1) * Configuration::NodeSize));
        }
        insertion_buffer.clear();
    }

    bool refresh_top() {
        if (!deletion_buffer.empty()) {
            return true;
        }
        if (insertion_buffer.full()) {
            flush_insertion_buffer();
        }
        if (!heap.empty()) {
            if (!insertion_buffer.empty()) {
                auto insert_it = std::partition(insertion_buffer.begin(), insertion_buffer.end(), [&](auto const &v) {
                    return heap.get_comparator()(heap.top_node().back().first, v.first);
                });
                std::sort(insert_it, insertion_buffer.end(), [&](auto const &lhs, auto const &rhs) {
                    return heap.get_comparator()(rhs.first, lhs.first);
                });
                auto heap_it = heap.top_node().begin();
                for (auto current = insert_it; current != insertion_buffer.end(); ++current) {
                    while (heap.get_comparator()(heap_it->first, current->first)) {
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
                      [&](auto const &lhs, auto const &rhs) { return heap.get_comparator()(lhs.first, rhs.first); });
            std::move(insertion_buffer.begin(), insertion_buffer.end(), std::back_inserter(deletion_buffer));
            insertion_buffer.clear();
        }
        return !deletion_buffer.empty();
    }

    void extract_top(typename heap_type::value_type &retval) {
        assert(!deletion_buffer.empty());
        retval = std::move(deletion_buffer.front());
        deletion_buffer.pop_front();
    };

    void push(typename heap_type::value_type const &value) {
        if (!deletion_buffer.empty() && heap.get_comparator()(value.first, deletion_buffer.back().first)) {
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
      return insertion_buffer.empty() && deletion_buffer.empty() && heap.empty();
    }
};

template <typename Key, typename T, typename Comparator, typename Configuration>
using internal_priority_queue_t = PriorityQueueConfiguration<
    Configuration::UseMergeHeap, Configuration::UseMergeHeap ? true : Configuration::WithInsertionBuffer,
    Configuration::UseMergeHeap ? true : Configuration::WithDeletionBuffer, Key, T, Comparator, Configuration>;

}  // namespace

struct Handle {
    template <typename Key, typename T, typename Comparator, typename Configuration, typename Allocator>
    friend class multiqueue;

   private:
    unsigned int id_;

   private:
    explicit Handle(unsigned int id) : id_{id} {
    }
};

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
        std::default_random_engine gen;
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
        mutable std::atomic_bool guard = false;
        pq_type pq;

        InternalPriorityQueueWrapper() = default;

        explicit InternalPriorityQueueWrapper(allocator_type const &alloc) : pq(alloc) {
        }

        explicit InternalPriorityQueueWrapper(Comparator const &comp, allocator_type const &alloc = allocator_type())
            : pq(comp, alloc) {
        }

        inline bool try_lock() const noexcept {
            bool expect = false;
            return guard.compare_exchange_strong(expect, true, std::memory_order_acquire, std::memory_order_relaxed);
        }

        inline void unlock() const noexcept {
            assert(guard == true);
            guard.store(false, std::memory_order_release);
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
    inline void refresh_insert_index(size_type index) {
        thread_data_[index].insert_index = thread_data_[index].get_random_index();
        thread_data_[index].insert_count = Configuration::K;
    }

    inline void refresh_extract_indices(size_type index) {
        thread_data_[index].extract_index[0] = thread_data_[index].get_random_index();
        do {
            thread_data_[index].extract_index[1] = thread_data_[index].get_random_index();
        } while (thread_data_[index].extract_index[1] == thread_data_[index].extract_index[0]);
        thread_data_[index].extract_count = Configuration::K;
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

    void push(Handle const &handle, value_type const &value) {
        if (thread_data_[handle.id_].insert_count == 0) {
            refresh_insert_index(handle.id_);
        }
        size_type index = thread_data_[handle.id_].insert_index;
        while (!pq_list_[index].try_lock()) {
            refresh_insert_index(handle.id_);
            index = thread_data_[handle.id_].insert_index;
        }
        /* std::cout << "lock " << index << '\n'; */
        pq_list_[index].pq.push(value);
        pq_list_[index].unlock();
        /* std::cout << "unlock " << index << '\n'; */
        --thread_data_[handle.id_].insert_count;
    }

    bool extract_top(Handle const &handle, value_type &retval) {
        if (thread_data_[handle.id_].extract_count == 0) {
            refresh_extract_indices(handle.id_);
        }
        size_type first_index = thread_data_[handle.id_].extract_index[0];
        size_type second_index = thread_data_[handle.id_].extract_index[1];
        do {
            if (pq_list_[first_index].try_lock()) {
                /* std::cout << "1 lock " << first_index << '\n'; */
                if (pq_list_[second_index].try_lock()) {
                    /* std::cout << "2 lock " << second_index << '\n'; */
                    break;
                }
                pq_list_[first_index].unlock();
                /* std::cout << "unlock " << first_index << '\n'; */
            }
            refresh_extract_indices(handle.id_);
            first_index = thread_data_[handle.id_].extract_index[0];
            second_index = thread_data_[handle.id_].extract_index[1];
            /* std::cout <<first_index << " " << second_index << '\n'; */
        } while (true);
        /* std::cout << "1 lock " << first_index << '\n'; */
        /* std::cout << "2 lock " << second_index << '\n'; */
        // When we get here, we hold the lock for both queues
        bool first_empty = !pq_list_[first_index].pq.refresh_top();
        if (first_empty) {
            pq_list_[first_index].unlock();
            /* std::cout << "1 unlock " << first_index << '\n'; */
        }
        bool second_empty = !pq_list_[second_index].pq.refresh_top();
        if (second_empty) {
            pq_list_[second_index].unlock();
            /* std::cout << "2 unlock " << second_index << '\n'; */
        }
        if (first_empty && second_empty) {
            refresh_extract_indices(handle.id_);
            return false;
        }
        if (!first_empty && !second_empty) {
            if (comp_(pq_list_[second_index].pq.top().first, pq_list_[first_index].pq.top().first)) {
                std::swap(first_index, second_index);
            }
            pq_list_[second_index].unlock();
            /* std::cout << "unlock other " << first_index << '\n'; */
        } else if (first_empty) {
            first_index = second_index;
        }
        pq_list_[first_index].pq.extract_top(retval);
        pq_list_[first_index].unlock();
        /* std::cout << "unlock best " << first_index << '\n'; */
        if (first_empty || second_empty) {
            refresh_extract_indices(handle.id_);
        }
        --thread_data_[handle.id_].extract_count;
        return true;
    }

    // threadsafe, but can be inaccurate if multiqueue is accessed
    bool weak_empty() const {
      for (size_type i = 0; i < pq_list_size_; ++i) {
        if (!pq_list_[i].try_lock()) {
          return false;
        }
        if (!pq_list_[i].pq.empty()) {
          return false;
        }
        pq_list_[i].unlock();
      }
      return true;
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
        ss << "Preallocation for " << Configuration::ReservePerQueue << " elements per internal pq";
        return ss.str();
    }
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
