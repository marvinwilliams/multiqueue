/**
******************************************************************************
* @file:   heap.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 17:26
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef SEQUENTIAL_HEAP_HEAP_HPP_INCLUDED
#define SEQUENTIAL_HEAP_HEAP_HPP_INCLUDED

#include "multiqueue/sequential/heap/full_down_strategy.hpp"
#include "multiqueue/util/extractors.hpp"

#include <cassert>
#include <cstddef>
#include <memory>       // allocator
#include <type_traits>  // is_constructible, enable_if
#include <utility>      // move, forward, pair
#include <vector>

namespace multiqueue {
namespace sequential {

template <typename T, typename Key, typename KeyExtractor, typename Comparator>
struct heap_base : private KeyExtractor, private Comparator {
    using value_type = T;
    using key_type = Key;
    using key_extractor = KeyExtractor;
    using comp_type = Comparator;
    using reference = value_type &;
    using const_reference = value_type const &;

    static_assert(
        std::is_invocable_r_v<key_type const &, key_extractor const &, value_type const &>,
        "Keys must be extractable from values using the signature `Key const& KeyExtractor(Value const&) const &`");
    static_assert(std::is_invocable_r_v<bool, comp_type const &, key_type const &, key_type const &>,
                  "Keys must be comparable using the signature `bool Comparator(Key const&, Key const&) const &`");
    static_assert(std::is_default_constructible_v<key_extractor>, "`KeyExtractor` must be default-constructible");

   protected:
    static constexpr bool is_key_extract_noexcept = noexcept(std::declval<key_extractor>()(std::declval<value_type>()));
    static constexpr bool is_compare_noexcept =
        noexcept(std::declval<comp_type>()(std::declval<key_type>(), std::declval<key_type>()));

    heap_base() = default;

    explicit heap_base(comp_type const &comp) noexcept(std::is_nothrow_default_constructible_v<key_extractor>)
        : key_extractor(), comp_type(comp) {
    }

    constexpr key_extractor const &to_key_extractor() const noexcept {
        return *this;
    }

    constexpr comp_type const &to_comparator() const noexcept {
        return *this;
    }

    constexpr key_type const &extract_key(value_type const &value) const noexcept(is_key_extract_noexcept) {
        return to_key_extractor()(value);
    }

    constexpr bool compare(key_type const &lhs, key_type const &rhs) const noexcept(is_compare_noexcept) {
        return to_comparator()(lhs, rhs);
    }

    constexpr bool value_compare(value_type const &lhs, value_type const &rhs) const noexcept(is_compare_noexcept) {
        return compare(extract_key(lhs), extract_key(rhs));
    }
};

template <typename T, typename Key, typename KeyExtractor, typename Comparator, unsigned int Degree,
          typename SiftStrategy, typename Allocator>
class heap : private heap_base<T, Key, KeyExtractor, Comparator> {
    friend SiftStrategy;
    using base_type = heap_base<T, Key, KeyExtractor, Comparator>;

   public:
    using value_type = typename base_type::value_type;
    using key_type = typename base_type::key_type;
    using key_extractor = typename base_type::key_extractor;
    using comp_type = typename base_type::comp_type;
    using reference = typename base_type::reference;
    using const_reference = typename base_type::const_reference;

    using allocator_type = Allocator;
    using container_type = std::vector<typename heap::value_type, allocator_type>;
    using iterator = typename container_type::const_iterator;
    using const_iterator = typename container_type::const_iterator;
    using difference_type = typename container_type::difference_type;
    using size_type = std::size_t;

    static_assert(Degree >= 1, "Degree must be at least one");

   private:
    static constexpr auto degree_ = Degree;
    container_type data_;

   private:
    static constexpr std::size_t parent_index(std::size_t const index) noexcept {
        return (index - 1) / Degree;
    }

    static constexpr std::size_t first_child_index(std::size_t const index) noexcept {
        return index * Degree + 1;
    }

    // Find the index of the smallest `num_children` children of the node at
    // index `index`
    constexpr size_type min_child_index(size_type index, size_type const num_children = Degree) const
        noexcept(base_type::is_key_extract_noexcept &&base_type::is_compare_noexcept) {
        assert(index < size());
        index = first_child_index(index);
        if (num_children == 1) {
            return index;
        }
        auto const last = index + num_children;
        assert(last <= size());
        auto result = index++;
        for (; index < last; ++index) {
            if (value_compare(data_[index], data_[result])) {
                result = index;
            }
        }
        return result;
    }

#ifndef NDEBUG
    bool is_heap() const {
        for (size_type i = 0; i < size(); i++) {
            auto const first_child = first_child_index(i);
            for (auto j = 0; j < Degree; ++j) {
                if (first_child + j >= size()) {
                    return true;
                }
                if (value_compare(data_[first_child + j], data_[i])) {
                    return false;
                }
            }
        }
        return true;
    }
#endif
   public:
    heap() = default;

    explicit heap(allocator_type const &alloc) noexcept(std::is_nothrow_default_constructible_v<base_type>)
        : base_type(), data_(alloc) {
    }

    explicit heap(comp_type const &comp, allocator_type const &alloc = allocator_type()) noexcept(
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
        return data_.size();
    }

    inline const_reference top() const {
        return data_.front();
    }

    void pop() {
        assert(!data_.empty());
        auto const index = SiftStrategy::remove(*this, 0);
        if (index + 1 < size()) {
            data_[index] = std::move(data_.back());
        }
        data_.pop_back();
        assert(is_heap());
    }

    void extract_top(value_type &retval) {
        assert(!data_.empty());
        retval = std::move(data_.front());
        pop();
    }

    void insert(value_type const &value) {
        size_type parent;
        if (!empty() && (parent = parent_index(size()), value_compare(value, data_[parent]))) {
            data_.push_back(std::move(data_[parent]));
            auto const index = SiftStrategy::sift_up_hole(*this, parent, extract_key(value));
            data_[index] = value;
            assert(is_heap());
        } else {
            data_.push_back(value);
        }
    }

    void insert(value_type &&value) {
        size_type parent;
        if (!empty() && (parent = parent_index(size()), value_compare(value, data_[parent]))) {
            data_.push_back(std::move(data_[parent]));
            auto const index = SiftStrategy::sift_up_hole(*this, parent, extract_key(value));
            data_[index] = std::move(value);
            assert(is_heap());
        } else {
            data_.push_back(std::move(value));
        }
    }

    // This function constructs the value as if its key was `key`.
    // The heap can be corrupted if the provided key `key` does not
    // behave the same as the key of the constructed value under the comparator
    // `Comparator`.
    template <typename... Args>
    void emplace_known(key_type const &key, Args &&...args) {
        size_type parent;
        if (!empty() && (parent = parent_index(size()), compare(key, extract_key(data_[parent])))) {
            data_.push_back(std::move(data_[parent]));
            auto const index = SiftStrategy::sift_up_hole(*this, parent, key);
            data_[index] = value_type(std::forward<Args>(args)...);
            assert(extract_key(data_[index]) == key);
            assert(is_heap());
        } else {
            data_.emplace_back(std::forward<Args>(args)...);
            assert(extract_key(data_.back()) == key);
        }
    }

    inline void reserve(std::size_t const cap) {
        data_.reserve(cap);
    }

    inline void reserve_and_touch(std::size_t const cap) {
        if (size() < cap) {
            size_type const old_size = size();
            data_.resize(cap);
            // this does not free allocated memory
            data_.resize(old_size);
        }
    }

    inline void clear() noexcept {
        data_.clear();
    }
};

template <typename T, typename Comparator = std::less<T>, unsigned int Degree = 4,
          typename SiftStrategy = sift_strategy::FullDown, typename Allocator = std::allocator<T>>
using value_heap = heap<T, T, util::identity<T>, Comparator, Degree, SiftStrategy, Allocator>;

template <typename Key, typename T, typename Comparator = std::less<Key>, unsigned int Degree = 4,
          typename SiftStrategy = sift_strategy::FullDown, typename Allocator = std::allocator<std::pair<Key, T>>>
using key_value_heap =
    heap<std::pair<Key, T>, Key, util::get_nth<std::pair<Key, T>, 0>, Comparator, Degree, SiftStrategy, Allocator>;

}  // namespace sequential
}  // namespace multiqueue

#endif  //! SEQUENTIAL_HEAP_HEAP_HPP_INCLUDED
