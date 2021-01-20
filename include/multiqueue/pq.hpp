/******************************************************************************
 * File:             pq.hpp
 *
 * Author:
 * Created:          01/05/21
 * Description:
 *****************************************************************************/

#ifndef PQ_HPP_0PET2KO5
#define PQ_HPP_0PET2KO5

#include "multiqueue/heap/full_down_strategy.hpp"
#include "multiqueue/heap/heap.hpp"
#include "multiqueue/util/extractors.hpp"

#include <cassert>
#include <functional>  // std::less
#include <utility>     // std::swap
#include <vector>

namespace multiqueue {
namespace local_nonaddressable {

template <typename T>
struct default_heap_settings {
    static constexpr unsigned int Degree = 4;
    using Container = std::vector<T>;
    using Strategy = full_down_strategy;
};

template <typename T, typename Comparator = std::less<T>,
          template <typename> typename HeapSettings = default_heap_settings>
class pq {
   public:
    using key_type = T;
    using value_type = T;

    using key_comparator = Comparator;
    using value_comparator = Comparator;
    using heap_type = heap<T, T, util::identity, Comparator, HeapSettings<value_type>::Degree,
                           typename HeapSettings<value_type>::Container, typename HeapSettings<value_type>::Strategy>;

   private:
    heap_type heap_;

   public:
    using reference = typename heap_type::reference;
    using const_reference = typename heap_type::const_reference;
    using iterator = typename heap_type::iterator;
    using const_iterator = typename heap_type::const_iterator;
    using difference_type = typename heap_type::difference_type;
    using size_type = typename heap_type::size_type;

   public:
    pq() = default;

    explicit pq(Comparator const &c) : heap_(c) {
    }

    template <typename Allocator, typename = std::enable_if_t<std::uses_allocator_v<heap_type, Allocator>>>
    explicit pq(std::allocator_arg_t, Allocator const &a) : heap_(std::allocator_arg, a) {
    }

    template <typename Allocator,
              typename = std::enable_if_t<std::uses_allocator_v<heap_type, Allocator> &&
                                          std::is_default_constructible_v<Comparator>>>
    explicit pq(std::allocator_arg_t, Allocator const &a, Comparator const &c) : heap_(std::allocator_arg, a, c) {
    }

    constexpr key_comparator key_comp() const noexcept {
        return heap_.key_comp();
    }

    constexpr value_comparator value_comp() const noexcept {
        return key_comparator();
    }

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
        heap_.remove_front();
    }

  [[nodiscard]] constexpr value_type extract_top() {
    return heap_.extract_front();
  }

  constexpr void push(value_type value) {
    heap_.insert_copy(std::move(value));
  }

    void push(value_type value) {
        heap_.insert_copy(std::move(value));
    }

    template <typename... Args>
    void emplace(Args &&...args) {
        heap_.emplace(std::forward<Args>(args)...);
    }
};

}  // namespace local_nonaddressable
}  // namespace multiqueue

namespace std {
template <typename T, typename Comparator, template <typename> typename HeapSettings, typename Alloc>
struct uses_allocator<multiqueue::local_nonaddressable::pq<T, Comparator, HeapSettings>, Alloc>
    : public uses_allocator<typename multiqueue::local_nonaddressable::pq<T, Comparator, HeapSettings>::heap_type,
                            Alloc>::type {};
}  // namespace std

#endif /* end of include guard: PQ_HPP_0PET2KO5 */
