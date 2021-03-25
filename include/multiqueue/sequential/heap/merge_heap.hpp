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
#ifndef MERGE_HEAP_HPP_INCLUDED
#define MERGE_HEAP_HPP_INCLUDED

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
class merge_heap {
   public:
    using value_type = T;
    using key_type = Key;
    using key_extractor = KeyExtractor;
    using key_comparator = Comparator;
    using allocator_type = Allocator;

    using node_type = std::array<value_type, NodeSize>;

   public:
    using container_type = std::vector<node_type>;
    using reference = value_type &;
    using const_reference = value_type const &;
    using iterator = typename container_type::const_iterator;
    using const_iterator = typename container_type::const_iterator;
    using difference_type = typename container_type::difference_type;
    using size_type = std::size_t;

    static constexpr bool is_nothrow_comparable =
        std::is_nothrow_invocable_v<decltype(&key_comparator::operator()), key_type const &>;
    static constexpr std::size_t mask = NodeSize - 1;

    static_assert(std::is_invocable_r_v<bool, key_comparator const &, key_type const &, key_type const &>,
                  "Keys must be comparable using the signature `bool Comparator(Key const&, Key const&) const &`");
    static_assert(
        std::is_invocable_r_v<key_type const &, key_extractor const &, value_type const &>,
        "Keys must be extractable from values using the signature `Key const& KeyExtractor(Value const&) const &`");
    static_assert(std::is_default_constructible_v<key_extractor>, "`KeyExtractor` must be default-constructible");
    static_assert(NodeSize > 0 && (NodeSize & (NodeSize - 1)) == 0,
                  "NodeSize must be greater than 0 and a power of two");

   private:
    struct heap_data : private key_comparator, private key_extractor {
       public:
        container_type container;

       public:
        explicit heap_data() noexcept(std::is_nothrow_default_constructible_v<key_comparator>)
            : key_comparator{}, key_extractor{}, container() {
        }

        explicit heap_data(key_comparator const &comp) noexcept : key_comparator{comp}, key_extractor{}, container() {
        }

        explicit heap_data(allocator_type const &alloc) noexcept(
            std::is_nothrow_default_constructible_v<key_comparator>)
            : key_comparator{}, key_extractor{}, container(alloc) {
        }

        explicit heap_data(key_comparator const &comp, allocator_type const &alloc) noexcept
            : key_comparator{comp}, key_extractor{}, container(alloc) {
        }

        constexpr key_comparator const &to_comparator() const noexcept {
            return *this;
        }
        constexpr key_extractor const &to_key_extractor() const noexcept {
            return *this;
        }
    };

    heap_data data_;

   private:
    static constexpr size_type parent_index(size_type const index) noexcept {
        assert(index > 0);
        return (index - 1) >> 1;
    }

    static constexpr size_type first_child_index(size_type const index) noexcept {
        return (index << 1) + 1;
    }

    constexpr key_type const &extract_key(value_type const &value) const noexcept {
        return data_.to_key_extractor()(value);
    }

    constexpr bool key_compare(key_type const &lhs, key_type const &rhs) const noexcept(is_nothrow_comparable) {
        return data_.to_comparator()(lhs, rhs);
    }

    constexpr bool value_compare(value_type const &lhs, value_type const &rhs) const noexcept(is_nothrow_comparable) {
        return data_.to_comparator()(extract_key(lhs), extract_key(rhs));
    }

    constexpr bool compare_first(std::size_t const lhs_index, std::size_t const rhs_index) const
        noexcept(is_nothrow_comparable) {
        return value_compare(data_.container[lhs_index].front(), data_.container[rhs_index].front());
    }

    constexpr bool compare_last(std::size_t const lhs_index, std::size_t const rhs_index) const
        noexcept(is_nothrow_comparable) {
        return value_compare(data_.container[lhs_index].back(), data_.container[rhs_index].back());
    }

    // Find the index of the smallest `num_children` children of the node at
    // index `index`
    size_type min_child_index(size_type index) const noexcept(is_nothrow_comparable) {
        assert(index < data_.container.size());
        index = first_child_index(index);
        assert(index + 1 < data_.container.size());
        return compare_last(index, index + 1) ? index : index + 1;
    }

#ifndef NDEBUG
    bool is_heap() const {
        auto value_comparator = [&](const_reference lhs, const_reference rhs) { return value_compare(lhs, rhs); };
        if (data_.container.empty()) {
            return true;
        };
        if (!std::is_sorted(data_.container.front().begin(), data_.container.front().end(), value_comparator)) {
            return false;
        }
        for (size_type i = 0; i < data_.container.size(); ++i) {
            for (auto j = first_child_index(i); j < first_child_index(i) + 2u; ++j) {
                if (j >= data_.container.size()) {
                    return true;
                }
                if (!std::is_sorted(data_.container[j].begin(), data_.container[j].end(), value_comparator)) {
                    return false;
                }
                if (value_compare(data_.container[j].front(), data_.container[i].back())) {
                    return false;
                }
            }
        }
        return true;
    }
#endif

   public:
    merge_heap() noexcept(std::is_nothrow_constructible_v<heap_data>) : data_{} {
    }

    explicit merge_heap(key_comparator const &comp) noexcept : data_{comp} {
    }

    explicit merge_heap(allocator_type const &alloc) noexcept(
        std::is_nothrow_constructible_v<heap_data, allocator_type const &>)
        : data_{alloc} {
    }

    explicit merge_heap(key_comparator const &comp, allocator_type const &alloc) noexcept : data_{comp, alloc} {
    }

    constexpr key_comparator key_comp() const noexcept {
        return data_.to_comparator();
    }

    inline void init_touch(std::size_t size) {
        assert(data_.container.empty());
        data_.container.resize(size / NodeSize);
        // clear does not free allocated memory
        data_.container.clear();
    }

    [[nodiscard]] inline bool empty() const noexcept {
        return data_.container.empty();
    }

    inline size_type size() const noexcept {
        return data_.container.size() * NodeSize;
    }

    inline const_reference top() const {
        assert(!empty());
        return data_.container.front().front();
    }

    inline node_type const &top_node() const {
        assert(!empty());
        return data_.container.front();
    }

    inline node_type &top_node() {
        assert(!empty());
        return data_.container.front();
    }

    inline void reserve_nodes(size_type num_nodes) {
        data_.container.reserve(num_nodes);
    }

    void pop_node() {
        assert(!empty());
        auto value_comparator = [this](const_reference lhs, const_reference rhs) { return value_compare(lhs, rhs); };
        std::size_t index = 0;
        std::size_t const first_incomplete_parent = parent_index(data_.container.size());
        while (index < first_incomplete_parent) {
            auto min_child = first_child_index(index);
            auto max_child = min_child + 1;
            assert(max_child < data_.container.size());
            if (compare_last(max_child, min_child)) {
                std::swap(min_child, max_child);
            }
            util::inplace_merge(data_.container[min_child].begin(), data_.container[max_child].begin(),
                                data_.container[index].begin(), NodeSize, value_comparator);
            index = min_child;
        }
        // If we have a child, we cannot have two, so we can just move the node into the hole.
        if (first_child_index(index) + 1 == data_.container.size()) {
            data_.container[index] = std::move(data_.container.back());
        } else if (index + 1 < data_.container.size()) {
            auto reverse_value_comparator = [this](const_reference lhs, const_reference rhs) {
                return value_compare(rhs, lhs);
            };
            while (index > 0) {
                auto const parent = parent_index(index);
                assert(parent < index);
                if (!value_compare(data_.container.back().front(), data_.container[parent].back())) {
                    break;
                }
                util::inplace_merge(data_.container[parent].rbegin(), data_.container.back().rbegin(),
                                    data_.container[index].rbegin(), NodeSize, reverse_value_comparator);
                index = parent;
            }
            std::move(data_.container.back().begin(), data_.container.back().end(), data_.container[index].begin());
        }
        data_.container.pop_back();
        assert(is_heap());
    }

    template <typename Iter>
    void extract_top_node(Iter output) {
        assert(!empty());
        std::move(data_.container.front().begin(), data_.container.front.end(), output);
        pop_node();
    }

    template <typename Iter>
    void insert(Iter first, Iter last) {
        auto reverse_value_comparator = [this](const_reference lhs, const_reference rhs) {
            return value_compare(rhs, lhs);
        };
        std::size_t index = data_.container.size();
        data_.container.push_back({});
        while (index > 0) {
            auto const parent = parent_index(index);
            assert(parent < index);
            if (!value_compare(*first, data_.container[parent].back())) {
                break;
            }
            util::inplace_merge(data_.container[parent].rbegin(), std::reverse_iterator{last},
                                data_.container[index].rbegin(), NodeSize, reverse_value_comparator);
            index = parent;
        }
        std::move(first, last, data_.container[index].begin());
        assert(is_heap());
    }

    inline void clear() noexcept {
        data_.container.clear();
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

#endif  //! MERGE_HEAP_HPP_INCLUDED
