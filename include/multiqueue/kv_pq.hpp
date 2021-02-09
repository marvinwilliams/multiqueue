/******************************************************************************
 * File:             kv_pq.hpp
 *
 * Author:
 * Created:          01/07/21
 * Description:
 *****************************************************************************/
#pragma once
#ifndef test_test _INCLUDED
#define test_test _INCLUDED

#endif  //!test_test_INCLUDED
#ifndef KV_PQ_HPP_8MTIGDLA
#define KV_PQ_HPP_8MTIGDLA

#include "multiqueue/heap/full_down_strategy.hpp"
#include "multiqueue/heap/heap.hpp"
#include "multiqueue/util/extractors.hpp"

#include <cassert>
#include <functional> // std::identity, std::less
#include <utility>    // std::swap
#include <vector>

namespace multiqueue {
namespace local_nonaddressable {

template <typename T>
struct default_kv_heap_settings {
    static constexpr unsigned int Degree = 4;
    using Container = std::vector<T>;
    using Strategy = full_down_strategy;
};

template <typename Key, typename Value, typename Comparator = std::less<Key>,
          template <typename> typename HeapSettings = default_kv_heap_settings>
class kv_pq {
   public:
    using key_type = Key;
    using mapped_type = Value;
    using value_type = std::pair<Key, Value>;
    using key_comparator = Comparator;

    using heap_type = heap<value_type, Key, util::pair_first, Comparator, HeapSettings<value_type>::Degree,
                           typename HeapSettings<value_type>::Container, typename HeapSettings<value_type>::Strategy>;

    class value_comparator : private key_comparator {
        friend kv_pq;
        explicit value_comparator(key_comparator const &comp) : key_comparator{comp} {
        }

       public:
        constexpr bool operator()(value_type const &lhs, value_type const &rhs) const {
            return key_comparator::operator()(util::pair_first::operator()(lhs), util::pair_first::operator()(rhs));
        }
    };

   public:
    using reference = typename heap_type::reference;
    using const_reference = typename heap_type::const_reference;
    using iterator = typename heap_type::iterator;
    using const_iterator = typename heap_type::const_iterator;
    using difference_type = typename heap_type::difference_type;
    using size_type = typename heap_type::size_type;

   private:
    heap_type heap_;

   public:
    kv_pq() = default;

    explicit kv_pq(Comparator const &c) : heap_(c) {
    }

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

    inline iterator begin() noexcept {
        return heap_.begin();
    }

    inline const_iterator begin() const noexcept {
        return heap_.cbegin();
    }

    inline const_iterator cbegin() const noexcept {
        return heap_.cbegin();
    }

    inline iterator end() noexcept {
        return heap_.end();
    }

    inline const_iterator end() const noexcept {
        return heap_.cend();
    }

    inline const_iterator cend() const noexcept {
        return heap_.cend();
    }

    inline size_type size() const noexcept {
        return heap_.size();
    }

    [[nodiscard]] inline bool empty() const noexcept {
        return heap_.empty();
    }

    inline const_reference top() const {
        return heap_.top();
    }

    inline void clear() noexcept {
        heap_.clear();
    }

    inline void reserve(size_type new_cap) {
        heap_.reserve(new_cap);
    }

    void pop() {
        assert(!empty());
        heap_.pop();
    }

    void extract_top(value_type &value) {
        heap_.extract_top(value);
    }

    template <typename insert_type>
    void push(insert_type &&value) {
        heap_.insert(std::forward<insert_type>(value));
    }

    template <typename insert_key_type, typename... Args>
    void emplace_key(insert_key_type &&key, Args &&...args) {
        heap_.emplace_known(key, std::piecewise_construct, std::forward_as_tuple(std::forward<insert_key_type>(key)),
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
