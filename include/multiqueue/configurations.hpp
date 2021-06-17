/**
******************************************************************************
* @file:   configurations.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 18:41
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef CONFIGURATIONS_HPP_INCLUDED
#define CONFIGURATIONS_HPP_INCLUDED

#include "multiqueue/sequential/heap/full_down_strategy.hpp"
#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/sequential/heap/merge_heap.hpp"
#include "multiqueue/util/buffer.hpp"
#include "multiqueue/util/extractors.hpp"
#include "multiqueue/util/ring_buffer.hpp"
#include "sequential/heap/heap.hpp"

#include <cstddef>
#include <memory>

namespace multiqueue {
namespace configuration {

struct Default {
    // Number of queues per thread
    static constexpr unsigned int C = 4;
    // Stickiness of selected queue
    static constexpr unsigned int K = 1;
    // Activate/Deactivate deletion and insertion buffer (only with merge heap deactivated)
    static constexpr bool WithDeletionBuffer = true;
    static constexpr bool WithInsertionBuffer = true;
    // Buffer sizes (number of elements)
    static constexpr std::size_t DeletionBufferSize = 8;
    static constexpr std::size_t InsertionBufferSize = 8;
    // Use a merging heap (implies using buffers with sizes dependent on the node size)
    static constexpr bool UseMergeHeap = false;
    // Node size used only by the merge heap
    static constexpr std::size_t NodeSize = 128;
    // Use pheromones on the locks
    static constexpr bool WithPheromones = false;
    // Make multiqueue numa friendly (induces more overhead)
    static constexpr bool NumaFriendly = false;
    // degree of the heap tree (effect only if merge heap deactivated)
    static constexpr unsigned int HeapDegree = 8;
    // Number of elements to preallocate in each queue
    static constexpr std::size_t ReservePerQueue = 1'000'000;
    using HeapAllocator = std::allocator<int>;
    using SiftStrategy = sequential::sift_strategy::FullDown;
};

struct NoBuffering : Default {
    static constexpr bool WithDeletionBuffer = false;
    static constexpr bool WithInsertionBuffer = false;
};

struct DeleteBuffering : Default {
    static constexpr bool WithDeletionBuffer = true;
    static constexpr bool WithInsertionBuffer = false;
};

struct InsertBuffering : Default {
    static constexpr bool WithDeletionBuffer = false;
    static constexpr bool WithInsertionBuffer = true;
};

struct FullBuffering : Default {
    static constexpr bool WithDeletionBuffer = true;
    static constexpr bool WithInsertionBuffer = true;
};

struct Merging : Default {
    static constexpr bool UseMergeHeap = true;
};

}  // namespace configuration

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

    inline std::size_t size() const noexcept {
      return heap.size();
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

    inline std::size_t size() const noexcept {
      return heap.size() + insertion_buffer.size();
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

    inline std::size_t size() const noexcept {
      return heap.size() + deletion_buffer.size();
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

    inline std::size_t size() const noexcept {
      return heap.size() + insertion_buffer.size() + deletion_buffer.size();
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

    inline std::size_t size() const noexcept {
      return heap.size() + insertion_buffer.size() + deletion_buffer.size();
    }
};

template <typename Key, typename T, typename Comparator, typename Configuration>
using internal_priority_queue_t = PriorityQueueConfiguration<
    Configuration::UseMergeHeap, Configuration::UseMergeHeap ? true : Configuration::WithInsertionBuffer,
    Configuration::UseMergeHeap ? true : Configuration::WithDeletionBuffer, Key, T, Comparator, Configuration>;

}  // namespace multiqueue

#endif  //! CONFIGURATIONS_HPP_INCLUDED
