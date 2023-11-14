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

#include "multiqueue/buffered_pq.hpp"
#include "multiqueue/guarded_pq.hpp"
#include "multiqueue/handle.hpp"
#include "multiqueue/heap.hpp"
#include "multiqueue/stick_policy/random.hpp"

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <type_traits>

namespace multiqueue {

namespace defaults {

template <typename Key, typename Value>
struct KeyOfValue {
    static_assert(std::is_same_v<Key, Value>, "KeyOfValue not specialized for this value type");
};

template <typename Key>
struct KeyOfValue<Key, Key> {
    static constexpr Key const &get(Key const &key) noexcept {
        return key;
    }
};

template <typename Key, typename T>
struct KeyOfValue<Key, std::pair<Key, T>> {
    static constexpr Key const &get(std::pair<Key, T> const &p) noexcept {
        return p.first;
    }
};

template <typename T, typename Compare>
struct Sentinel {
    using key_type = T;
    using key_compare = Compare;
    static constexpr key_type const &key_of_value(T const &v) noexcept {
        return v;
    }
    static constexpr bool implicit_sentinel = false;
    static constexpr key_type sentinel() noexcept {
        static_assert(std::is_default_constructible_v<key_type>, "key_type must be default-constructible");
        return key_type();
    }
};

template <typename Key>
struct Sentinel<Key, std::less<Key>> {
    static constexpr bool is_implicit = std::numeric_limits<Key>::is_bounded;
    static constexpr Key get() noexcept {
        return std::numeric_limits<Key>::lowest();
    }
};

template <typename Key>
struct Sentinel<Key, std::less<>> : Sentinel<Key, std::less<Key>> {};

template <typename Key>
struct Sentinel<Key, std::greater<Key>> {
    static constexpr bool is_implicit = std::numeric_limits<Key>::is_bounded;
    static constexpr Key get() noexcept {
        return std::numeric_limits<Key>::max();
    }
};

template <typename Key>
struct Sentinel<Key, std::greater<>> : Sentinel<Key, std::greater<Key>> {};

template <typename Value, typename KeyOfValue, typename Compare>
struct ValueCompare {
    Compare comp;

    bool operator()(Value const &lhs, Value const &rhs) const noexcept {
        return comp(KeyOfValue::get(lhs), KeyOfValue::get(rhs));
    }
};

struct Traits {
    using stick_policy_policy_type = stick_policy::Random<2>;
    static constexpr bool strict_comparison = true;
    static constexpr bool count_stats = false;
    static constexpr unsigned int num_pop_tries = 1;
    static constexpr bool scan_on_failed_pop = true;
};

template <typename Value, typename KeyOfValue, typename Compare, typename Traits>
using InnerPQ = BufferedPQ<Heap<Value, ValueCompare<Value, KeyOfValue, Compare>>>;

}  // namespace defaults

template <typename Key, typename Compare = std::less<>, typename Value = Key,
          typename KeyOfValue = defaults::KeyOfValue<Key, Value>, typename Traits = defaults::Traits,
          typename PriorityQueue = defaults::InnerPQ<Value, KeyOfValue, Compare, Traits>,
          typename Sentinel = defaults::Sentinel<Key, Compare>, typename Allocator = std::allocator<PriorityQueue>>
class MultiQueue {
   public:
    using key_type = Key;
    using value_type = Value;
    using key_compare = Compare;
    using value_compare = typename PriorityQueue::value_compare;
    using priority_queue_type = PriorityQueue;
    using reference = value_type &;
    using const_reference = value_type const &;
    using size_type = std::size_t;
    using allocator_type = Allocator;
    using sentinel_type = Sentinel;
    using key_of_value_type = KeyOfValue;
    using handle_type = Handle<MultiQueue>;
    friend handle_type;
    using traits_type = Traits;
    using stick_policy_config_type = typename traits_type::stick_policy_policy_type::Config;

   private:
    using internal_priority_queue_type = GuardedPQ<key_type, value_type, KeyOfValue, priority_queue_type, Sentinel>;
    using internal_allocator_type =
        typename std::allocator_traits<allocator_type>::template rebind_alloc<internal_priority_queue_type>;
    using stick_policy_shared_data_type = typename handle_type::stick_policy_shared_data_type;

    internal_priority_queue_type *pq_list_{};
    size_type num_pqs_;
    [[no_unique_address]] stick_policy_config_type stick_policy_config_;
    [[no_unique_address]] stick_policy_shared_data_type stick_policy_shared_data_;
    [[no_unique_address]] key_compare comp_;
    [[no_unique_address]] internal_allocator_type alloc_;

   public:
    MultiQueue(const MultiQueue &) = delete;
    MultiQueue(MultiQueue &&) = delete;
    MultiQueue &operator=(const MultiQueue &) = delete;
    MultiQueue &operator=(MultiQueue &&) = delete;

    explicit MultiQueue(size_type num_pqs, stick_policy_config_type const &config = {},
                        priority_queue_type const &pq = {}, key_compare const &comp = {},
                        allocator_type const &alloc = {})
        : num_pqs_{num_pqs},
          stick_policy_config_{config},
          stick_policy_shared_data_(num_pqs_),
          comp_{comp},
          alloc_(alloc) {
        assert(num_pqs_ > 0);

        pq_list_ = std::allocator_traits<internal_allocator_type>::allocate(alloc_, num_pqs_);
        for (auto *it = pq_list_; it != pq_list_ + num_pqs_; ++it) {
            std::allocator_traits<internal_allocator_type>::construct(alloc_, it, pq);
        }
    }

    explicit MultiQueue(size_type num_pqs, typename PriorityQueue::size_type initial_capacity,
                        stick_policy_config_type const &config = {},
                        priority_queue_type const &pq = {}, key_compare const &comp = {},
                        allocator_type const &alloc = {})
        : MultiQueue(num_pqs, config, pq, comp, alloc) {
        auto cap_per_queue = (initial_capacity + num_pqs_ - 1) / num_pqs_;
        for (auto *it = pq_list_; it != pq_list_ + num_pqs_; ++it) {
            it->reserve(cap_per_queue);
        }
    }

    template <typename ForwardIt>
    explicit MultiQueue(ForwardIt first, ForwardIt last,
                        stick_policy_config_type const &config = {},
                        key_compare const &comp = {}, allocator_type const &alloc = {})
        : num_pqs_{std::distance(first, last)},
          stick_policy_config_{config},
          stick_policy_shared_data_(num_pqs_),
          comp_{comp},
          alloc_(alloc) {
        pq_list_ = std::allocator_traits<internal_allocator_type>::allocate(alloc_, num_pqs_);
        for (auto *it = pq_list_; it != pq_list_ + num_pqs_; ++it, ++first) {
            std::allocator_traits<internal_allocator_type>::construct(alloc_, it, *first);
        }
    }

    ~MultiQueue() noexcept {
        for (auto *it = pq_list_; it != pq_list_ + num_pqs_; ++it) {
            std::allocator_traits<internal_allocator_type>::destroy(alloc_, it);
        }
        std::allocator_traits<internal_allocator_type>::deallocate(alloc_, pq_list_, num_pqs_);
    }

    handle_type get_handle() noexcept {
        return handle_type(*this);
    }

    [[nodiscard]] stick_policy_config_type const &get_stick_policy_config() const noexcept {
        return stick_policy_config_;
    }

    [[nodiscard]] size_type num_pqs() const noexcept {
        return num_pqs_;
    }

    [[nodiscard]] key_compare key_comp() const {
        return comp_;
    }
};

}  // namespace multiqueue
