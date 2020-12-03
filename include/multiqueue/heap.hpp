/******************************************************************************
 * File:             heap.hpp
 *
 * Author:           Marvin Williams
 * Created:          12/16/20
 * Description:      This header defines the base class for the heap data
 *                   structure.
 *****************************************************************************/

#ifndef HEAP_HPP_MKBRIGPA
#define HEAP_HPP_MKBRIGPA

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <iostream>
#include <memory>
#include <type_traits>
#include <utility>

namespace multiqueue {

namespace local_nonaddressable {

template <typename Key, typename Value, typename KeyExtractor,
          typename Comparator, typename Container, unsigned int Degree>
class heap {
public:
  using key_type = Key;
  using value_type = Value;
  using key_extractor_type = KeyExtractor;
  using comp_type = Comparator;
  using reference = typename Container::reference;
  using const_reference = typename Container::const_reference;
  using iterator = typename Container::const_iterator;
  using const_iterator = typename Container::const_iterator;
  using difference_type = typename Container::difference_type;
  using size_type = typename Container::size_type;

  static_assert(std::is_invocable_r_v<bool, Comparator const &,
                                      key_type const &, key_type const &>,
                "`Comparator` must be invokable with the signature `bool "
                "Comparator(Key const&, Key const&) const`");
  static_assert(std::is_same_v<value_type, typename Container::value_type>,
                "The value type of `Container` must be `Value`");
  static_assert(
      std::is_invocable_r_v<Key const &, KeyExtractor &&, Value const &>,
      "`KeyExtractor` must be invokable with the signature `Key const& "
      "KeyExtractor(Value const&) &&`");
  static_assert(std::is_default_constructible_v<Container>,
                "The container must be default-constructible");
  static_assert(Degree >= 1u, "Degree must be at least one");

private:
  struct heap_impl : private Comparator, private key_extractor_type {
    Container data;
    heap_impl() noexcept(std::is_nothrow_default_constructible_v<Comparator>) =
        default;

    explicit heap_impl(const Comparator c) noexcept : Comparator(c) {}

    template <typename Allocator,
              typename =
                  std::enable_if_t<std::uses_allocator_v<Container, Allocator>>>
    explicit heap_impl(
        std::allocator_arg_t,
        Allocator const
            &allocator) noexcept(std::
                                     is_nothrow_default_constructible_v<
                                         Comparator>)
        : data(allocator) {}

    template <typename Allocator,
              typename =
                  std::enable_if_t<std::uses_allocator_v<Container, Allocator>>>
    heap_impl(std::allocator_arg_t, Allocator const &allocator,
              Comparator const &c) noexcept
        : Comparator(c), data(allocator) {}

    static constexpr size_type parent_index(size_type const index) noexcept {
      return (index - 1u) / Degree;
    }

    static constexpr size_type
    first_child_index(size_type const index) noexcept {
      return index * Degree + 1u;
    }

    // Find the index of the smallest `num_children` children of the node at
    // index `index`
    constexpr size_type min_child_index(size_type index) const {
      assert(index < data.size());
      index = first_child_index(index);
      if (Degree == 1) {
        return index;
      }
      auto const last = index + Degree;
      assert(last <= data.size());
      auto result = index;
      while (++index < last) {
        if (comparator()(key_extractor()(data[index]),
                         key_extractor()(data[result]))) {
          result = index;
        }
      }
      return result;
    }

    // Find the index of the smallest `num_children` children of the node at
    // index `index`
    constexpr size_type min_child_index(size_type index,
                                        size_type const num_children) const {
      assert(index < data.size());
      index = first_child_index(index);
      if (num_children == 1) {
        return index;
      }
      auto const last = index + num_children;
      assert(last <= data.size());
      auto result = index;
      while (++index < last) {
        if (comparator()(key_extractor()(data[index]),
                         key_extractor()(data[result]))) {
          result = index;
        }
      }
      return result;
    }

    bool is_heap() const {
      for (size_t i = 0; i < data.size(); i++) {
        for (auto j = first_child_index(i); j < first_child_index(i) + Degree;
             ++j) {
          if (j >= data.size()) {
            return true;
          }
          if (comparator()(key_extractor()(data[j]),
                           key_extractor()(data[i]))) {
            return false;
          }
        }
      }
      return true;
    }

    // Sifts the hole at index `index` up to the top of the heap.
    constexpr void sift_up_hole(size_type index) {
      assert(index < data.size());
      while (index > 0) {
        auto const parent = parent_index(index);
        assert(parent < index);
        data[index] = std::move(data[parent]);
        index = parent;
      }
    }

    // Sifts the hole at index `index` up until either the top of the heap is
    // reached or key `key` is not smaller than the parent of the returned
    // hole index.
    constexpr size_type sift_up_hole(size_type index, key_type const &key) {
      assert(index < data.size());
      while (index > 0) {
        auto const parent = parent_index(index);
        assert(parent < index);
        if (!comparator()(key, key_extractor()(data[parent]))) {
          break;
        }
        data[index] = std::move(data[parent]);
        index = parent;
      }
      return index;
    }

    // Sifts the value at index `index` up until either the top of the heap is
    // reached or the value's is not smaller than its parent's key. The final
    // index of the value is returned.
    constexpr size_type sift_up(size_type index) {
      assert(index < data.size());
      if (index > 0) {
        auto const parent = parent_index(index);
        if (comparator()(key_extractor()(data[index]),
                         key_extractor()(data[parent]))) {
          auto value = std::move(data[index]);
          data[index] = std::move(data[parent]);
          index = sift_up_hole(parent, key_extractor()(value));
          data[index] = std::move(value);
        }
      }
      return index;
    }

    // Sifts the hole at index `index` down to the bottom of the heap.
    constexpr size_type sift_down_hole(size_type index) {
      assert(data.size() > 0u && index < data.size());
      // The node that will be the parent of the next inserted node
      size_type const first_incomplete_parent = parent_index(data.size());
      assert(first_incomplete_parent < data.size());
      // This loop exits too early if we descent into a parent node with [1,
      // degree - 1] children
      while (index < first_incomplete_parent) {
        auto const child = min_child_index(index);
        assert(child < data.size());
        data[index] = std::move(data[child]);
        index = child;
      }
      if (index == first_incomplete_parent) {
        if (size_type const num_children = (data.size() - 1) % Degree;
            num_children != 0u) {
          // Loop has exited too early and we need to sift the hole down once
          // more
          auto const child = min_child_index(index, num_children);
          assert(child < data.size());
          data[index] = std::move(data[child]);
          index = child;
        }
      }
      return index;
    }

    // Sifts the hole at index `index` down until either the bottom of the
    // heap is reached or no child of the returned hole index is smaller than
    // key `key`.
    constexpr size_type sift_down_hole(size_type const index,
                                       key_type const &key) {
      assert(data.size() > 0u && index < data.size());

      // The node that will be the parent of the next inserted node
      size_type const first_incomplete_parent = parent_index(data.size());
      assert(first_incomplete_parent < data.size());
      // This loop might exit too early if any node has [1, degree - 1]
      // children
      while (index < first_incomplete_parent) {
        auto const child = min_child_index(index);
        if (!comparator()(key_extractor()(data[child]), key)) {
          return index;
        }
        data[index] = std::move(data[child]);
        index = child;
      }
      if (index == first_incomplete_parent) {
        if (size_type const num_children = (data.size() - 1) % Degree;
            num_children != 0u) {
          // Loop has exited too early and we need to sift the hole down once
          // more
          auto const child = min_child_index(index, num_children);
          assert(child < data.size());
          if (comparator()(key_extractor()(data[child]), key)) {
            data[index] = std::move(data[child]);
            index = child;
          }
        }
      }
      return index;
    }

    // Sifts the value at index `index` down until either the bottom of the
    // heap is reached or no child's key  is smaller than the value's key. The
    // final index of the value is returned.
    constexpr size_type sift_down(size_type index) {
      assert(data.size() > 0u && index < data.size());

      // The node that will be the parent of the next inserted node
      size_type const first_incomplete_parent = parent_index(data.size());
      assert(first_incomplete_parent < data.size());
      // This loop might exit too early if any node has [1, degree - 1]
      // children
      while (index < first_incomplete_parent) {
        auto const child = min_child_index(index);
        data[index] = std::move(data[child]);
        index = child;
      }
      if (index == first_incomplete_parent) {
        if (size_type const num_children = (data.size() - 1) % Degree;
            num_children != 0u) {
          // Loop has exited too early and we need to sift the hole down once
          // more
          auto const child = min_child_index(index, num_children);
          assert(child < data.size());
          data[index] = std::move(data[child]);
          index = child;
        }
      }
      return index;
    }

    constexpr comp_type const &comparator() const { return *this; }
    constexpr key_extractor_type const &key_extractor() const { return *this; }
  };

  heap_impl impl_;

public:
  heap() = default;

  explicit heap(Comparator const &c) : impl_(c) {}

  template <
      typename Allocator,
      typename = std::enable_if_t<std::uses_allocator_v<Container, Allocator>>>
  explicit heap(std::allocator_arg_t, Allocator const &a) noexcept(
      std::is_nothrow_constructible_v<heap_impl, std::allocator_arg_t,
                                      Allocator const &>)
      : impl_(std::allocator_arg, a) {}

  template <
      typename Allocator,
      typename = std::enable_if_t<std::uses_allocator_v<Container, Allocator>>>
  heap(std::allocator_arg_t, Allocator const &allocator, Comparator const &c)
      : impl_(std::allocator_arg, allocator, c) {}

  constexpr comp_type comparator() const
      noexcept(std::is_nothrow_copy_constructible_v<Comparator>) {
    return static_cast<Comparator const &>(impl_);
  }

  constexpr iterator begin() const noexcept { return impl_.data.cbegin(); }

  constexpr const_iterator cbegin() const noexcept {
    return impl_.data.cbegin();
  }

  constexpr iterator end() const noexcept { return impl_.data.cend(); }

  constexpr const_iterator cend() const { return impl_.data.cend(); }

  [[nodiscard]] constexpr bool empty() const noexcept {
    return impl_.data.empty();
  }

  constexpr size_type size() const noexcept { return impl_.data.size(); }

  constexpr size_type max_size() const noexcept {
    return impl_.data.max_size();
  }

  constexpr const_reference top() const { return impl_.data.front(); }

  constexpr void reserve(size_type new_cap) { impl_.data.reserve(new_cap); }

  constexpr void remove_front() {
    assert(!impl_.data.empty());
    auto index = impl_.sift_down_hole(0u);
    if (index + 1 < impl_.data.size()) {
      index =
          impl_.sift_up_hole(index, impl_.key_extractor()(impl_.data.back()));
      impl_.data[index] = std::move(impl_.data.back());
    }
    impl_.data.pop_back();
    assert(impl_.is_heap());
  }

  constexpr value_type extract_front() {
    assert(!impl_.data.empty());
    auto ret = std::move(impl_.data.front());
    remove_front();
    return ret;
  }

  constexpr void insert_copy(value_type value) {
    if (empty()) {
      impl_.data.push_back(std::move(value));
    } else {
      if (auto const parent = impl_.parent_index(size());
          impl_.comparator()(impl_.key_extractor()(value),
                             impl_.key_extractor()(impl_.data[parent]))) {
        impl_.data.push_back(std::move(impl_.data[parent]));
        auto const index =
            impl_.sift_up_hole(parent, impl_.key_extractor()(value));
        impl_.data[index] = std::move(value);
      } else {
        impl_.data.push_back(std::move(value));
      }
    }
    assert(impl_.is_heap());
  }

  constexpr void insert_reference(value_type const &value) {
    if (empty()) {
      impl_.data.push_back(value);
    } else {
      if (auto const parent = impl_.parent_index(size());
          impl_.comparator()(impl_.key_extractor()(value),
                             impl_.key_extractor()(impl_.data[parent]))) {
        impl_.data.push_back(std::move(impl_.data[parent]));
        auto const index =
            impl_.sift_up_hole(parent, impl_.key_extractor()(value));
        impl_.data[index] = value;
      } else {
        impl_.data.push_back(value);
      }
    }
    assert(impl_.is_heap());
  }

  constexpr void insert_reference(value_type &&value) {
    if (empty()) {
      impl_.data.push_back(std::move(value));
    } else {
      if (auto const parent = impl_.parent_index(size());
          impl_.comparator()(impl_.key_extractor()(value),
                             impl_.key_extractor()(impl_.data[parent]))) {
        impl_.data.push_back(std::move(impl_.data[parent]));
        auto const index =
            impl_.sift_up_hole(parent, impl_.key_extractor()(value));
        impl_.data[index] = std::move(value);
      } else {
        impl_.data.push_back(std::move(value));
      }
    }
    assert(impl_.is_heap());
  }

  template <typename... Args> constexpr void emplace(Args &&...args) {
    impl_.data.emplace_back(std::forward<Args>(args)...);
    impl_.sift_up(size() - 1);
    assert(impl_.is_heap());
  }

  // This function constructs the value as if its key was `key`.
  // The heap can be corrupted if the provided key `key` does not
  // behave the same as the key of the constructed value under the comparator
  // `Comparator`.
  template <typename... Args>
  constexpr void emplace_known(key_type const &key, Args &&...args) {
    if (empty()) {
      impl_.data.emplace_back(std::forward<Args>(args)...);
    } else {
      if (auto const parent = impl_.parent_index(size());
          impl_.comparator()(key, impl_.key_extractor()(impl_.data[parent]))) {
        impl_.data.push_back(std::move(impl_.data[parent]));
        auto const index = impl_.sift_up_hole(parent, key);
        impl_.data[index] = value_type(std::forward<Args>(args)...);
      } else {
        impl_.data.emplace_back(std::forward<Args>(args)...);
      }
    }
    assert(impl_.is_heap());
  }

  // This function constructs the value as if its key was `key`. It then
  // adjusts the value`s position according to its actual key.
  template <typename... Args>
  constexpr void emplace_hint(key_type const &key, Args &&...args) {
    if (empty()) {
      impl_.data.emplace_back(std::forward<Args>(args)...);
    } else {
      if (auto parent = impl_.parent_index(size());
          impl_.comparator()(key, impl_.key_extractor()(impl_.data[parent]))) {
        impl_.data.push_back(std::move(impl_.data[parent]));
        auto index = impl_.sift_up_hole(parent, key);
        impl_.data[index] = value_type(std::forward<Args>(args)...);
        auto const new_index = impl_.sift_up(index);
        if (new_index == index) {
          impl_.sift_down(new_index);
        }
      } else {
        impl_.data.emplace_back(std::forward<Args>(args)...);
        impl_.sift_up(size() - 1);
      }
    }
    assert(impl_.is_heap());
  }

  constexpr void clear() noexcept { impl_.data.clear(); }
};

} // namespace local_nonaddressable

} // namespace multiqueue

#endif /* end of include guard: HEAP_HPP_MKBRIGPA */
