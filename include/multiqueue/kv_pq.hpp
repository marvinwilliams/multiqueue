/******************************************************************************
 * File:             kv_pq.hpp
 *
 * Author:
 * Created:          01/07/21
 * Description:
 *****************************************************************************/

#ifndef KV_PQ_HPP_8MTIGDLA
#define KV_PQ_HPP_8MTIGDLA

#include "multiqueue/heap.hpp"

#include <cassert>
#include <functional> // std::identity, std::less
#include <utility>    // std::swap
#include <vector>

namespace multiqueue {
namespace local_nonaddressable {

template <typename Key, typename T, typename Comparator = std::less<Key>,
          typename Container = std::vector<std::pair<Key, T>>,
          unsigned int Degree = 4>
class kv_pq {
public:
  using key_type = Key;
  using value_type = std::pair<Key, T>;
  using key_comparator = Comparator;
  struct value_comparator : private Comparator {
    friend class kv_pq<Key, T, Comparator, Container, Degree>;

  private:
    value_comparator(Comparator c) : Comparator(c) {}

  public:
    constexpr bool operator()(value_type const &lhs,
                              value_type const &rhs) const {
      return Comparator::operator()(lhs.first, rhs.first);
    }
  };

private:
  struct pair_first {
    constexpr Key const &operator()(value_type const &v) const noexcept {
      return v.first;
    }
    constexpr Key &operator()(value_type &v) const noexcept { return v.first; }
  };

  using heap_type =
      heap<key_type, value_type, pair_first, key_comparator, Container, Degree>;

  heap_type heap_;

public:
  using reference = typename heap_type::reference;
  using const_reference = typename heap_type::const_reference;
  using iterator = typename heap_type::iterator;
  using const_iterator = typename heap_type::const_iterator;
  using difference_type = typename heap_type::difference_type;
  using size_type = typename heap_type::size_type;

  kv_pq() = default;

  explicit kv_pq(Comparator const &c) : heap_(c) {}

  template <
      typename Allocator,
      typename = std::enable_if_t<std::uses_allocator_v<Container, Allocator>>>
  explicit kv_pq(std::allocator_arg_t, Allocator const &a)
      : heap_(std::allocator_arg, a) {}

  template <
      typename Allocator,
      typename = std::enable_if_t<std::uses_allocator_v<Container, Allocator> &&
                                  std::is_default_constructible_v<Comparator>>>
  explicit kv_pq(std::allocator_arg_t, Allocator const &a, Comparator const &c)
      : heap_(std::allocator_arg, a, c) {}

  constexpr iterator begin() noexcept { return heap_.begin(); }

  constexpr const_iterator begin() const noexcept { return heap_.cbegin(); }

  constexpr const_iterator cbegin() const noexcept { return heap_.cbegin(); }

  constexpr iterator end() noexcept { return heap_.end(); }

  constexpr const_iterator end() const noexcept { return heap_.cend(); }

  constexpr const_iterator cend() const noexcept { return heap_.cend(); }

  constexpr size_type size() const noexcept { return heap_.size(); }

  constexpr size_type max_size() const noexcept { return heap_.max_size(); }

  [[nodiscard]] constexpr bool empty() const noexcept { return heap_.empty(); }

  constexpr const_reference top() const { return heap_.top(); }

  constexpr void clear() noexcept { heap_.clear(); }

  constexpr void reserve(size_type new_cap) { heap_.reserve(new_cap); }

  constexpr void pop() {
    assert(!empty());
    heap_.remove_front();
  }

  constexpr value_type extract_top() { return heap_.extract_front(); }

  constexpr void push(value_type const &value) {
    heap_.insert_reference(value);
  }

  constexpr void push(value_type &&value) {
    heap_.insert_reference(std::move(value));
  }

  template <typename... Args> constexpr void emplace(Args &&...args) {
    heap_.emplace(std::forward<Args>(args)...);
  }

  template <typename... Args>
  constexpr void emplace_hint(key_type const &key, Args &&...args) {
    heap_.emplace_hint(key, std::forward<Args>(args)...);
  }

  template <typename... Args>
  constexpr void emplace_key(key_type const &key, Args &&...args) {
    heap_.emplace_known(key, std::piecewise_construct,
                        std::forward_as_tuple(key),
                        std::forward_as_tuple(std::forward<Args>(args)...));
  }

  template <typename... Args>
  constexpr void emplace_key(key_type &&key, Args &&...args) {
    heap_.emplace_known(key, std::piecewise_construct,
                        std::forward_as_tuple(std::move(key)),
                        std::forward_as_tuple(std::forward<Args>(args)...));
  }
};

} // namespace local_nonaddressable
} // namespace multiqueue

namespace std {
template <typename Key, typename T, typename Comparator, typename Container,
          unsigned int Degree, typename Alloc>
struct uses_allocator<multiqueue::local_nonaddressable::kv_pq<
                          Key, T, Comparator, Container, Degree>,
                      Alloc> : public uses_allocator<Container, Alloc>::type {};
} // namespace std

#endif /* end of include guard: KV_PQ_HPP_8MTIGDLA */
