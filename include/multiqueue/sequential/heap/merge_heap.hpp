/**
******************************************************************************
* @file:   merge_heap.hpp
*
* @author: Marvin Williams
* @date:   2021/03/02 16:21
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef SEQUENTIAL_HEAP_MERGE_HEAP_HPP_INCLUDED
#define SEQUENTIAL_HEAP_MERGE_HEAP_HPP_INCLUDED

#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/util/extractors.hpp"
#include "multiqueue/util/inplace_merge.hpp"

#include <algorithm>
#include <cassert>
#include <iterator>
#include <memory>       // allocator
#include <type_traits>  // is_constructible, enable_if
#include <utility>      // move, forward
#include <vector>

namespace multiqueue {
namespace sequential {

template <typename T, typename Key, typename KeyExtractor, typename Comparator, std::size_t NodeSize,
          typename Allocator = std::allocator<T>>
class merge_heap : private heap_base<T, Key, KeyExtractor, Comparator> {
    using base_type = heap_base<T, Key, KeyExtractor, Comparator>;

   public:
    using value_type = typename base_type::value_type;
    using key_type = typename base_type::key_type;
    using key_extractor = typename base_type::key_extractor;
    using comp_type = typename base_type::comp_type;
    using reference = typename base_type::reference;
    using const_reference = typename base_type::const_reference;

    using node_type = std::array<value_type, NodeSize>;
    using allocator_type = Allocator;
    using container_type = std::vector<node_type>;
    using iterator = typename container_type::const_iterator;
    using const_iterator = typename container_type::const_iterator;
    using difference_type = typename container_type::difference_type;
    using size_type = std::size_t;

    static_assert(NodeSize > 0 && (NodeSize & (NodeSize - 1)) == 0,
                  "NodeSize must be greater than 0 and a power of two");

   private:
    static constexpr auto node_size_ = NodeSize;
    container_type data_;

   private:
    static constexpr size_type parent_index(size_type const index) noexcept {
        assert(index > 0);
        return (index - 1) >> 1;
    }

    static constexpr size_type first_child_index(size_type const index) noexcept {
        return (index << 1) + 1;
    }

    constexpr bool compare_last(size_type const lhs_index, size_type const rhs_index) const
        noexcept(base_type::is_key_extract_noexcept &&base_type::is_compare_noexcept) {
        return value_compare(data_[lhs_index].back(), data_[rhs_index].back());
    }

    // Find the index of the smallest `num_children` children of the node at
    // index `index`
    size_type min_child_index(size_type index) const noexcept(noexcept(compare_last(0, 0))) {
        assert(index < data_.size());
        index = first_child_index(index);
        assert(index + 1 < data_.size());
        return compare_last(index, index + 1) ? index : index + 1;
    }

#ifndef NDEBUG
    bool is_heap() const {
        if (data_.empty()) {
            return true;
        };
        auto value_comparator = [&](const_reference lhs, const_reference rhs) { return value_compare(lhs, rhs); };
        if (!std::is_sorted(data_.front().begin(), data_.front().end(), value_comparator)) {
            return false;
        }
        for (size_type i = 0; i < data_.size(); ++i) {
            for (auto j = first_child_index(i); j < first_child_index(i) + 2u; ++j) {
                if (j >= data_.size()) {
                    return true;
                }
                if (!std::is_sorted(data_[j].begin(), data_[j].end(), value_comparator)) {
                    return false;
                }
                if (value_compare(data_[j].front(), data_[i].back())) {
                    return false;
                }
            }
        }
        return true;
    }
#endif

   public:
    merge_heap() = default;

    explicit merge_heap(allocator_type const &alloc) noexcept(std::is_nothrow_constructible_v<base_type>)
        : base_type(), data_(alloc) {
    }

    explicit merge_heap(comp_type const &comp, allocator_type const &alloc = allocator_type()) noexcept(
        std::is_nothrow_constructible_v<base_type, comp_type>)
        : base_type(comp), data_(alloc) {
    }

    constexpr comp_type const &get_comparator() const noexcept {
        return base_type::to_comparator();
    }

    inline iterator begin() const noexcept {
        return data_.cbegin();
    }

    inline const_iterator cbegin() const noexcept {
        return data_.cbegin();
    }

    inline iterator end() const noexcept {
        return data_.cend();
    }

    inline const_iterator cend() const noexcept {
        return data_.cend();
    }

    [[nodiscard]] inline bool empty() const noexcept {
        return data_.empty();
    }

    inline size_type size() const noexcept {
        return data_.size() * NodeSize;
    }

    inline const_reference top() const {
        assert(!empty());
        return data_.front().front();
    }

    inline node_type const &top_node() const {
        assert(!empty());
        return data_.front();
    }

    inline node_type &top_node() {
        assert(!empty());
        return data_.front();
    }

    inline void reserve(std::size_t const cap) {
        data_.reserve(cap / NodeSize + (cap % NodeSize == 0 ? 0 : 1));
    }

    inline void reserve_and_touch(std::size_t const cap) {
        auto const num_nodes = cap / NodeSize + (cap % NodeSize == 0 ? 0 : 1);
        if (data_.size() < num_nodes) {
            size_type const old_size = size();
            data_.resize(num_nodes);
            // this does not free allocated memory
            data_.resize(old_size);
        }
    }

    void pop_node() {
        assert(!empty());
        auto value_comparator = [this](const_reference lhs, const_reference rhs) { return value_compare(lhs, rhs); };
        size_type index = 0;
        size_type const first_incomplete_parent = parent_index(data_.size());
        while (index < first_incomplete_parent) {
            auto min_child = first_child_index(index);
            auto max_child = min_child + 1;
            assert(max_child < data_.size());
            if (compare_last(max_child, min_child)) {
                std::swap(min_child, max_child);
            }
            util::inplace_merge(data_[min_child].begin(), data_[max_child].begin(), data_[index].begin(), NodeSize,
                                value_comparator);
            index = min_child;
        }
        // If we have a child, we cannot have two, so we can just move the node into the hole.
        if (first_child_index(index) + 1 == data_.size()) {
            data_[index] = std::move(data_.back());
        } else if (index + 1 < data_.size()) {
            auto reverse_value_comparator = [this](const_reference lhs, const_reference rhs) {
                return value_compare(rhs, lhs);
            };

            size_type parent;
            while (index > 0 &&
                   (parent = parent_index(index), value_compare(data_.back().front(), data_[parent].back()))) {
                util::inplace_merge(data_[parent].rbegin(), data_.back().rbegin(), data_[index].rbegin(), NodeSize,
                                    reverse_value_comparator);
                index = parent;
            }
            std::move(data_.back().begin(), data_.back().end(), data_[index].begin());
        }
        data_.pop_back();
        assert(is_heap());
    }

    template <typename Iter>
    void extract_top_node(Iter output) {
        assert(!empty());
        std::move(data_.front().begin(), data_.front.end(), output);
        pop_node();
    }

    template <typename Iter>
    void insert(Iter first, Iter last) {
        auto reverse_value_comparator = [this](const_reference lhs, const_reference rhs) {
            return value_compare(rhs, lhs);
        };
        auto index = data_.size();
        data_.push_back({});
        while (index > 0) {
            auto const parent = parent_index(index);
            assert(parent < index);
            if (!value_compare(*first, data_[parent].back())) {
                break;
            }
            util::inplace_merge(data_[parent].rbegin(), std::reverse_iterator{last}, data_[index].rbegin(), NodeSize,
                                reverse_value_comparator);
            index = parent;
        }
        std::move(first, last, data_[index].begin());
        assert(is_heap());
    }

    inline void clear() noexcept {
        data_.clear();
    }
};

template <typename T, typename Comparator = std::less<T>, std::size_t NodeSize = 64,
          typename Allocator = std::allocator<T>>
using value_merge_heap = merge_heap<T, T, util::identity<T>, Comparator, NodeSize, Allocator>;

template <typename Key, typename T, typename Comparator = std::less<Key>, std::size_t NodeSize = 64,
          typename Allocator = std::allocator<std::pair<Key, T>>>
using key_value_merge_heap =
    merge_heap<std::pair<Key, T>, Key, util::get_nth<std::pair<Key, T>, 0>, Comparator, NodeSize, Allocator>;

}  // namespace sequential
}  // namespace multiqueue

#endif  //! SEQUENTIAL_HEAP_MERGE_HEAP_HPP_INCLUDED
