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

#include <cassert>
#include <memory>       // allocator
#include <type_traits>  // is_constructible, enable_if
#include <utility>      // move, forward
#include <vector>

namespace multiqueue {
namespace local_nonaddressable {

template <typename T, typename Key, typename KeyExtractor, typename Comparator, unsigned int Degree,
          unsigned int NodeSize, typename Allocator = std::allocator<T>>
class merge_heap {
   public:
    using value_type = T;
    using key_type = Key;
    using key_extractor = KeyExtractor;
    using key_comparator = Comparator;
    using allocator_type = Allocator;

   private:
    struct Node {
        std::array<value_type, NodeSize> data;
        unsigned int start_pos;
    };

   public:
    using container_type = std::vector<Node>;
    using reference = value_type &;
    using const_reference = value_type const &;
    using iterator = typename container_type::const_iterator;
    using const_iterator = typename container_type::const_iterator;
    using difference_type = typename container_type::difference_type;
    using size_type = std::size_t;

    static constexpr unsigned int degree = Degree;
    static constexpr bool is_nothrow_comparable =
        std::is_nothrow_invocable_v<decltype(&key_comparator::operator()), key_type const &>;

    static_assert(std::is_invocable_r_v<bool, key_comparator const &, key_type const &, key_type const &>,
                  "Keys must be comparable using the signature `bool Comparator(Key const&, Key const&) const &`");
    static_assert(
        std::is_invocable_r_v<key_type const &, key_extractor const &, value_type const &>,
        "Keys must be extractable from values using the signature `Key const& KeyExtractor(Value const&) const &`");
    static_assert(std::is_default_constructible_v<key_extractor>, "`KeyExtractor` must be default-constructible");
    static_assert(Degree >= 1u, "Degree must be at least one");

   private:
    struct heap_data : private key_comparator, private key_extractor {
       public:
        container_type container;
        unsigned int top_size = 0;

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
        return (index - 1u) / Degree;
    }

    static constexpr size_type first_child_index(size_type const index) noexcept {
        return index * Degree + 1u;
    }

    constexpr bool compare(key_type const &lhs, key_type const &rhs) const noexcept(is_nothrow_comparable) {
        return data_.to_comparator()(lhs, rhs);
    }

    constexpr std::size_t last_value_index(std::size_t const node_index) const noexcept {
        return (data_.container[node_index].start_pos + (node_index == 0 ? data_.top_size - 1 : NodeSize - 1)) %
            NodeSize;
    }

    constexpr key_type const &extract_key(value_type const &value) const noexcept {
        return data_.to_key_extractor()(value);
    }

    constexpr bool compare_first(std::size_t const lhs_index, std::size_t const rhs_index) const
        noexcept(is_nothrow_comparable) {
        return data_.to_comparator()(extract_key(data_.container[lhs_index][data_.container[lhs_index].start_pos]),
                                     extract_key(data_.container[rhs_index][data_.container[rhs_index].start_pos]));
    }
    constexpr bool compare_last(std::size_t const lhs_index, std::size_t const rhs_index) const
        noexcept(is_nothrow_comparable) {
        return data_.to_comparator()(extract_key(data_.container[lhs_index][last_value_index(lhs_index)]),
                                     extract_key(data_.container[rhs_index][last_value_index(rhs_index)]));
    }

    // Find the index of the smallest `num_children` children of the node at
    // index `index`
    size_type min_child_index(size_type index) const noexcept(is_nothrow_comparable) {
        assert(index < data_.container.size());
        index = first_child_index(index);
        if (Degree == 1) {
            return index;
        }
        auto const last = index + Degree;
        assert(last <= data_.container.size());
        auto result = index;
        while (++index < last) {
            if (compare_last(index, result)) {
                result = index;
            }
        }
        return result;
    }

    // Find the index of the smallest `num_children` children of the node at
    // index `index`
    size_type min_child_index(size_type index, size_type const num_children) const noexcept(is_nothrow_comparable) {
        assert(index < data_.container.size());
        index = first_child_index(index);
        if (num_children == 1) {
            return index;
        }
        auto const last = index + num_children;
        assert(last <= data_.container.size());
        auto result = index;
        while (++index < last) {
            if (compare_last(index, result)) {
                result = index;
            }
        }
        return result;
    }

    // Merges lhs and rhs into out and move the remaining values into the bigger (by compare_last)
    size_type merge(size_type lhs, size_type rhs, size_type out) {

    }


#ifndef NDEBUG
    bool is_heap() const {
        for (size_type i = 0; i < data_.container.size(); i++) {
            for (auto j = first_child_index(i); j < first_child_index(i) + Degree; ++j) {
                if (j >= data_.container.size()) {
                    return true;
                }
                if (compare(extract_key(data_.container[j]), extract_key(data_.container[i]))) {
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

    inline iterator begin() const noexcept {
        return data_.container.cbegin();
    }

    inline const_iterator cbegin() const noexcept {
        return data_.container.cbegin();
    }

    inline iterator end() const noexcept {
        return data_.container.cend();
    }

    inline const_iterator cend() const noexcept {
        return data_.container.cend();
    }

    [[nodiscard]] inline bool empty() const noexcept {
        return data_.container.empty();
    }

    inline size_type size() const noexcept {
        return data_.container.size();
    }

    inline const_reference top() const {
        return data_.container.front();
    }

    inline void reserve(size_type new_cap) {
        data_.container.reserve(new_cap);
    }

    void pop() {
        assert(!data_.container.empty());
        --top_size;
        ++data_.container[0].start_pos;
        if (top_size == 0) {
          
        }
        assert(is_heap());
    }

    void extract_top(value_type &retval) {
        assert(!data_.container.empty());
        retval = std::move(data_.container.front());
        pop();
    }

    void insert_copy(value_type value) {
        if (empty()) {
            data_.container.push_back(value);
        } else {
            if (auto const parent = parent_index(size());
                compare(extract_key(value), extract_key(data_.container[parent]))) {
                data_.container.push_back(std::move(data_.container[parent]));
                auto const index = SiftStrategy::sift_up_hole(*this, parent, extract_key(value));
                data_.container[index] = value;
            } else {
                data_.container.push_back(value);
            }
        }
        assert(is_heap());
    }

    template <typename insert_type>
    void insert(insert_type &&value) {
        if (empty()) {
            data_.container.push_back(std::forward<insert_type>(value));
        } else {
            if (auto const parent = parent_index(size());
                compare(extract_key(value), extract_key(data_.container[parent]))) {
                data_.container.push_back(std::move(data_.container[parent]));
                auto const index = SiftStrategy::sift_up_hole(*this, parent, extract_key(value));
                data_.container[index] = std::forward<insert_type>(value);
            } else {
                data_.container.push_back(std::forward<insert_type>(value));
            }
        }
        assert(is_heap());
    }

    template <typename... Args>
    void emplace(Args &&...args) {
        data_.container.emplace_back(std::forward<Args>(args)...);
        if (size() > 1) {
            if (auto const parent = parent_index(size() - 1);
                compare(extract_key(data_.container.back()), extract_key(data_.container[parent]))) {
                auto value = std::move(data_.container.back());
                data_.container.back() = std::move(data_.container[parent]);
                auto const index = SiftStrategy::sift_up_hole(*this, parent, extract_key(value));
                data_.container[index] = std::move(value);
            }
        }
        assert(is_heap());
    }

    // This function constructs the value as if its key was `key`.
    // The heap can be corrupted if the provided key `key` does not
    // behave the same as the key of the constructed value under the comparator
    // `Comparator`.
    template <typename... Args>
    void emplace_known(key_type const &key, Args &&...args) {
        if (empty()) {
            data_.container.emplace_back(std::forward<Args>(args)...);
        } else {
            if (auto const parent = parent_index(size()); compare(key, extract_key(data_.container[parent]))) {
                data_.container.push_back(std::move(data_.container[parent]));
                auto const index = SiftStrategy::sift_up_hole(*this, parent, key);
                data_.container[index] = value_type(std::forward<Args>(args)...);
            } else {
                data_.container.emplace_back(std::forward<Args>(args)...);
            }
        }
        assert(is_heap());
    }

    inline void clear() noexcept {
        data_.container.clear();
    }
};

}  // namespace local_nonaddressable
}  // namespace multiqueue

#endif  //! MERGE_HEAP_HPP_INCLUDED
