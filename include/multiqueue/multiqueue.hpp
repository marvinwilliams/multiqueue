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

#include "multiqueue/default_configuration.hpp"
#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/sequential/heap/merge_heap.hpp"
#include "multiqueue/util/buffer.hpp"
#include "multiqueue/util/extractors.hpp"
#include "multiqueue/util/ring_buffer.hpp"
#include "sequential/heap/heap.hpp"
#include "system_config.hpp"

#ifdef NUMA_SUPPORT
#include <numa.h>
#endif
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <random>
#include <sstream>
#include <type_traits>

namespace multiqueue {

namespace {

template <typename Key, typename T, typename Comparator, typename Configuration>
struct InternalPriorityQueueFactory {
   private:
    template <bool isMerging>
    struct MergeSelector;

    template <>
    struct MergeSelector<false> {
        using internal_heap_type =
            sequential::key_value_heap<Key, T, Comparator, Configuration::HeapDegree,
                                       typename Configuration::SiftStrategy, typename Configuration::HeapAllocator>;
        template <bool WithInsertionBuffer, bool WithDeletionBuffer>
        struct BufferSelector;
        template <>
        struct BufferSelector<false, false> {
            struct PriorityQueueConfiguration {
                using heap_type = internal_heap_type;
                heap_type heap;

                template <typename... Args>
                PriorityQueueConfiguration(Args &&...args) : heap(std::forward<Args>(args)...) {
                }

                inline typename heap_type::value_type const &top() {
                    return heap.top();
                }

                inline void extract_top(typename heap_type::value_type &retval) {
                    heap.extract_top(retval);
                }

                inline bool refresh_top() noexcept {
                    return heap.empty();
                }

                inline void push(typename heap_type::value_type const &value) {
                    heap.insert(value);
                }

                inline void pop() {
                    heap.pop();
                }
            };
        };

        template <>
        struct BufferSelector<true, false> {
            struct PriorityQueueConfiguration {
                using heap_type = internal_heap_type;
                util::buffer<typename heap_type::value_type, Configuration::InsertionBufferSize> insertion_buffer;
                heap_type heap;

                template <typename... Args>
                PriorityQueueConfiguration(Args &&...args) : heap(std::forward<Args>(args)...) {
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
                    return heap.empty();
                }

                inline void push(typename heap_type::value_type const &value) {
                    if (insertion_buffer.size() != InsertionBufferSize) {
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
            };
        };

        template <>
        struct BufferSelector<false, true> {
            struct PriorityQueueConfiguration {
                using heap_type = internal_heap_type<Key, T, Comparator>;
                util::ring_buffer<typename heap_type::value_type, Configuration::DeletionBufferSize> deletion_buffer;
                heap_type heap;

                template <typename... Args>
                PriorityQueueConfiguration(Args &&...args) : heap(std::forward<Args>(args)...) {
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
                    for (std::size_t i = 0; i < DeletionBufferSize && !heap.empty(); ++i) {
                        heap.extract_top(tmp);
                        deletion_buffer.push_back(std::move(tmp));
                    }
                    return deletion_buffer.empty();
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
                        if (deletion_buffer.size() == DeletionBufferSize) {
                            heap.insert(std::move(deletion_buffer.back()));
                            deletion_buffer.pop_back();
                        }
                        std::size_t pos = deletion_buffer.size();
                        for (; pos > 0 && heap.get_comparator()(value.first, deletion_buffer[pos - 1].first; --pos)) {
                        }
                        deletion_buffer.insert_at(pos, value);
                    }
                }

                inline void pop() {
                    assert(!deletion_buffer.empty());
                    deletion_buffer.pop_front();
                }
            };
        };

        template <>
        struct BufferSelector<true, true> {
            struct PriorityQueueConfiguration {
                using heap_type = internal_heap_type<Key, T, Comparator>;
                util::buffer<typename heap_type::value_type, Configuration::InsertionBufferSize> insertion_buffer;
                util::ring_buffer<typename heap_type::value_type, Configuration::DeletionBufferSize> deletion_buffer;
                heap_type heap;

                template <typename... Args>
                PriorityQueueConfiguration(Args &&...args) : heap(std::forward<Args>(args)...) {
                }

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
                    for (std::size_t i = 0; i < DeletionBufferSize && !heap.empty(); ++i) {
                        heap.extract_top(tmp);
                        deletion_buffer.push_back(std::move(tmp));
                    }
                    return deletion_buffer.empty();
                }

                void extract_top(typename heap_type::value_type &retval) {
                    assert(!deletion_buffer.empty());
                    retval = std::move(deletion_buffer.front());
                    deletion_buffer.pop_front();
                };

                void push(typename heap_type::value_type const &value) {
                    if (!deletion_buffer.empty() && heap.get_comparator()(value.first, deletion_buffer.back().first)) {
                        if (deletion_buffer.size() == DeletionBufferSize) {
                            if (insertion_buffer.size() == InsertionBufferSize) {
                                flush_insertion_buffer();
                                heap.insert(std::move(deletion_buffer.back()));
                            } else {
                                insertion_buffer.push_back(std::move(deletion_buffer.back()));
                            }
                            deletion_buffer.pop_back();
                        }
                        std::size_t pos = deletion_buffer.size();
                        for (; pos > 0 && heap.get_comparator()(value.first, deletion_buffer[pos - 1].first; --pos)) {
                        }
                        deletion_buffer.insert_at(pos, value);
                        return;
                    }
                    if (insertion_buffer.size() == InsertionBufferSize) {
                        flush_insertion_buffer();
                        heap.insert(std::move(deletion_buffer.back()));
                        return;
                    } else {
                        insertion_buffer.push_back(std::move(deletion_buffer.back()));
                    }
                }

                void pop() {
                    assert(!deletion_buffer.empty());
                    deletion_buffer.pop_front();
                }
            };
        };
        using PriorityQueueConfiguration =
            typename BufferSelector<Configuration::WithInsertionBuffer,
                                    Configuration::WithDeletionBuffer>::PriorityQueueConfiguration;
    };

    template <>
    struct MergeSelector<true> {
        struct PriorityQueueConfiguration {
            using heap_type = sequential::key_value_merge_heap<Key, T, Comparator, Configuration::NodeSize,
                                                               typename Configuration::HeapAllocator>;
            util::buffer<typename heap_type::value_type, Configuration::NodeSize> insertion_buffer;
            util::ring_buffer<typename heap_type::value_type, Configuration::NodeSize * 2> deletion_buffer;
            heap_type heap;

            template <typename... Args>
            PriorityQueueConfiguration(Args &&...args) : heap(std::forward<Args>(args)...) {
            }

            inline typename heap_type::value_type const &top() {
                assert(!deletion_buffer.empty());
                return deletion_buffer.front();
            }

            inline void flush_insertion_buffer() {
                assert(insertion_buffer.size() == InsertionBufferSize);
                std::sort(insertion_buffer.begin(), insertion_buffer.end(), heap.get_comparator());
                for (std::size_t i = 0; i < insertion_buffer.size(); i += NodeSize) {
                    heap.insert(insertion_buffer.begin() + (i * NodeSize),
                                insertion_buffer.begin() + ((i + 1) * NodeSize));
                }
                insertion_buffer.clear();
            }

            bool refresh_top() {
                if (!deletion_buffer.empty()) {
                    return true;
                }
                if (insertion_buffer.size() == InsertionBufferSize) {
                    flush_insertion_buffer();
                }
                if (!heap.empty()) {
                    if (!insertion_buffer.empty()) {
                        auto insert_it = std::partition(
                            insertion_buffer.begin(), insertion_buffer.end(),
                            [&](auto const &v) { return heap.get_comparator()(heap.top_node().back(), v); });
                        std::sort(insert_it, insertion_buffer.end(), heap.get_comparator());
                        auto heap_it = heap.top_node().begin();
                        for (auto current = insert_it; current != insertion_buffer.end(); ++current) {
                            while (heap.get_comparator()(*heap_it, *current)) {
                                deletion_buffer.push_back(std::move(*heap_it++));
                            }
                            deletion_buffer.push_back(std::move(*current));
                        }
                        insertion_buffer.set_size(insert_it - insertion_buffer.begin());
                        std::move(heap_it, heap.top_node().end(), std::back_inserter(deletion_buffer));
                    } else {
                        std::move(heap.top_node.begin(), heap.top_node().end(), std::back_inserter(deletion_buffer));
                    }
                    heap.pop_node();
                } else if (!insertion_buffer.empty()) {
                    std::sort(insertion_buffer.begin(), insertion_buffer.end(), heap.get_comparator());
                    std::move(insertion_buffer.begin(), insertion_buffer.end(), std::back_inserter(deletion_buffer));
                    insertion_buffer.clear();
                }
                return deletion_buffer.empty();
            }

            void extract_top(typename heap_type::value_type &retval) {
                assert(!deletion_buffer.empty());
                retval = std::move(deletion_buffer.front());
                deletion_buffer.pop_front();
            };

            void push(typename heap_type::value_type const &value) {
                if (!deletion_buffer.empty() && heap.get_comparator()(value.first, deletion_buffer.back().first)) {
                    if (deletion_buffer.size() == DeletionBufferSize) {
                        if (insertion_buffer.size() == InsertionBufferSize) {
                            flush_insertion_buffer();
                            heap.insert(std::move(deletion_buffer.back()));
                        } else {
                            insertion_buffer.push_back(std::move(deletion_buffer.back()));
                        }
                        deletion_buffer.pop_back();
                    }
                    std::size_t pos = deletion_buffer.size();
                    for (; pos > 0 && heap.get_comparator()(value.first, deletion_buffer[pos - 1].first; --pos)) {
                    }
                    deletion_buffer.insert_at(pos, value);
                    return;
                }
                if (insertion_buffer.size() == InsertionBufferSize) {
                    flush_insertion_buffer();
                    heap.insert(std::move(deletion_buffer.back()));
                    return;
                } else {
                    insertion_buffer.push_back(std::move(deletion_buffer.back()));
                }
            }

            void pop() {
                assert(!deletion_buffer.empty());
                deletion_buffer.pop_front();
            }
        };
    };
    using PriorityQueueConfiguration = typename MergeSelector<Configuration::UseMergeHeap>::PriorityQueueConfiguration;

   public:
    using type = PriorityQueueConfiguration;
};

template <typename Key, typename T, typename Comparator, typename Configuration>
using internal_priority_queue_type_factory_t =
    typename InternalPriorityQueueFactory<Key, T, Comparator, Configuration>::type;

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
};

template <typename Key, typename T, typename Comparator = std::less<Key>, typename Configuration = DefaultConfiguration,
          typename Allocator = std::allocator<Key>>
class multiqueue : multiqueue_base<Key, T, Comparator> {
   private:
    static constexpr unsigned int C = Configuration::C;
    using internal_priority_queue_type = internal_priority_queue_type_factory_t<Key, T, Comparator, Configuration>;
    struct alignas(Configuration::NumaFriendly ? PAGE_SIZE : L1_CACHE_LINESIZE) InternalPriorityQueueWrapper {
        using allocator_type = heap_configuration::heap_type::allocator_type;
        std::atomic_bool guard = false;
        internal_priority_queue_type pq;

        template <typename... Args>
        InternalPriorityQueueWrapper(Args &&...args) : pq{std::forward<Args>(args)...} {
        }

        inline bool try_lock() noexcept {
            bool expect = false;
            return guard.compare_exchange_strong(expect, true, std::memory_order_acquire, std::memory_order_relaxed);
        }

        inline void unlock() noexcept {
            assert(guard == true);
            guard.store(false, std::memory_order_release);
        }
    };

    struct ThreadLocalData {
        std::default_random_engine gen;
        unsigned int insert_count = 0;
        unsigned int extract_count = 0;
        size_type insert_index;
        std::array<size_type, 2> extract_index;
    };

   public:
    using allocator_type = Allocator;

   private:
    std::vector<InternalPriorityQueueWrapper, allocator_type> pq_list_;
    std::vector<ThreadLocalData, allocator_type> thread_data_;
    typename multiqueue::key_comparator comp_;

   private:
    inline void refresh_insert_index(size_type index) {
        std::uniform_int_distribution<size_type> dist{0, num_queues_ - 1};
        thread_data_[index].insert_index = dist(thread_data_[index].gen);
        thread_data_[index].insert_count = Configuration::K;
    }

    inline void refresh_extract_indices(size_type index) {
        std::uniform_int_distribution<size_type> dist{0, num_queues_ - 1};
        for (auto &idx : thread_data_[index].extract_index) {
            idx = dist(gen);
        }
        thread_data_[index].extract_count = Configuration::K;
    }

   public:
    explicit multiqueue(unsigned int const num_threads, allocator_type const &alloc = allocator_type())
        : pq_list_(alloc), comp_() {
        assert(num_threads >= 1);
#ifdef NUMA_SUPPORT
        if (Configuration::NumaFriendly) {
            numa_set_interleave_mask(numa_all_nodes_ptr);
            pq_list_.resize(num_threads * Configuration::C);
            numa_set_interleave_mask(numa_no_nodes_ptr);
            for (std::size_t i = 0; i < pq_list_.size(); ++i) {
                numa_set_preferred(i / (pq_list_.size() / (numa_max_node() + 1)));
                pq_list_[i].pq.reserve_and_touch(Configuration::ReservePerQueue);
                numa_set_preferred(-1);
            }
        } else
#else
        {
            pq_list_.resize(num_threads * Configuration::C);
            for (std::size_t i = 0; i < pq_list_.size(); ++i) {
                pq_list_[i].pq.reserve(Configuration::ReservePerQueue);
            }
        }
#endif
    }

    explicit multiqueue(unsigned int const num_threads, typename multiqueue::key_comparator const &comp,
                        allocator_type const &alloc = allocator_type())
        : pq_list_(alloc), comp_(comp) {
        assert(num_threads >= 1);
#ifdef NUMA_SUPPORT
        if (Configuration::NumaFriendly) {
            numa_set_interleave_mask(numa_all_nodes_ptr);
            pq_list_.resize(num_threads * Configuration::C);
            numa_set_interleave_mask(numa_no_nodes_ptr);
            for (std::size_t i = 0; i < pq_list_.size(); ++i) {
                numa_set_preferred(i / (pq_list_.size() / (numa_max_node() + 1)));
                pq_list_[i].pq.reserve_and_touch(Configuration::ReservePerQueue);
                numa_set_preferred(-1);
            }
        } else
#else
        {
            pq_list_.resize(num_threads * Configuration::C);
            for (std::size_t i = 0; i < pq_list_.size(); ++i) {
                pq_list_[i].pq.reserve(Configuration::ReservePerQueue);
            }
        }
#endif
    }

    handle_type get_handle(unsigned int id) const noexcept {
        return handle_type{id};
    }

    void push(Handle const &handle, typename multiqueue::value_type const &value) {
        if (thread_data_[handle.id_].insert_count == 0) {
            refresh_insert_index(handle.id_);
        }
        size_type index = thread_data_[handle.id_].insert_index;
        while (!pq_list_[index].try_lock()) {
            refresh_insert_index(handle.id_);
            index = thread_data_[handle.id_].insert_index;
        }
        --thread_data_[handle.id_].insert_count;
        pq_list_[index].pq.push(value);
        pq_list_[index].unlock();
    }

    bool extract_top(Handle const &handle, typename multiqueue::value_type &retval) {
        if (thread_data_[handle.id_].extract_count == 0) {
            refresh_extract_indices(handle.id_);
        }
        size_type first_index = thread_data_[handle.id_].extract_index[0];
        size_type second_index = thread_data_[handle.id_].extract_index[1];
        do {
            if (pq_list_[first_index].try_lock()) {
                if (pq_list_[second_index].try_lock()) {
                    break;
                }
                pq_list_[first_index].unlock();
            }
            refresh_extract_indices(handle.id_);
            first_index = thread_data_[handle.id_].extract_index[0];
            second_index = thread_data_[handle.id_].extract_index[1];
        } while (true);
        --thread_data_[handle.id_].extract_count;
        // When we get here, we hold the lock for both queues
        bool first_empty = !pq_list_[first_index].pq.refresh_top();
        if (first_empty) {
            pq_list_[first_index].unlock();
        }
        bool second_empty = !pq_list_[second_index].pq.refresh_top();
        if (second_empty) {
            pq_list_[second_index].unlock();
        }
        if (first_empty && second_empty) {
            refresh_extract_indices(handle.id_);
            return false;
        }
        if (!first_empty && !second_empty &&
            comp_(pq_list_[second_index].pq.top().first, pq_list_[first_index].pq.top().first)) {
            pq_list_[first_index].unlock();
            first_index = second_index;
        } else if (first_empty) {
            first_index = second_index;
        }
        pq_list_[first_index].pq.extract_top(retval);
        pq_list_[first_index].unlock();
        if (first_emtpy || second_empty) {
            refresh_extract_indices(handle.id_);
        }
        return true;
    }

    static std::string description() {
        std::stringstream ss;
        ss << "multiqueue\n";
        ss << "C: " << Configuration::C << '\n';
        ss << "K: " << Configuration::K << '\n';
        if (Configuration::UseMergeHeap) {
            ss << "Using merge heap, node size: " << Configuration::NodeSize << '\n';
        } else {
            if (Configuration::WithDeletionBuffer) {
                ss << "Using deletion buffer with size: " << Configuration::DeletionBufferSize << '\n';
            }
            if (Configuration::WithInsertionBuffer) {
                ss << "Using insertion buffer with size: " << Configuration::InsertionBufferSize << '\n';
            }
            ss << "Heap degree: " << Configuration::HeapDegree << '\n;
        }
        if (Configuration::NumaFriendly) {
            ss << "Numa friendly\n";
        }
        ss << "Preallocation for " << Configuration::ReservePerQueue << " elements per internal pq\n";
        return ss.str();
    }
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
