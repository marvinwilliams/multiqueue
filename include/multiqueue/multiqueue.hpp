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
#include "multiqueue/handle.hpp"
#include "multiqueue/heap.hpp"
#include "multiqueue/lockable_pq.hpp"
#include "multiqueue/operation_policy/noop.hpp"
#include "multiqueue/sentinel.hpp"
#include "multiqueue/utils.hpp"

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <type_traits>

namespace multiqueue {

template <typename Value, typename KeyOfValue, typename Compare>
using DefaultPriorityQueue = BufferedPQ<Heap<Value, utils::ValueCompare<Value, KeyOfValue, Compare>>>;

struct DefaultOperationTraits {
    using policy_type = operation_policy::Random<>;
    static constexpr bool scan_if_empty = true;
};

template <typename Key, typename Value, typename KeyOfValue, typename Compare = std::less<>,
          typename OperationTraits = DefaultOperationTraits,
          typename PriorityQueue = DefaultPriorityQueue<Value, KeyOfValue, Compare>,
          typename Sentinel = sentinel::Implicit<Key, Compare>, typename Allocator = std::allocator<PriorityQueue>>
class MultiQueue {
   public:
    using key_type = Key;
    using value_type = Value;
    using key_compare = Compare;
    using key_of_value_type = KeyOfValue;
    using operation_traits_type = OperationTraits;
    using priority_queue_type = PriorityQueue;
    using value_compare = typename priority_queue_type::value_compare;
    using reference = value_type &;
    using const_reference = value_type const &;
    using size_type = std::size_t;
    using allocator_type = Allocator;
    using sentinel_type = Sentinel;
    using operation_config_type = typename operation_traits_type::policy_type::Config;

   private:
    using lockable_pq_type = LockablePQ<key_type, value_type, KeyOfValue, priority_queue_type, sentinel_type>;
    using internal_allocator_type =
        typename std::allocator_traits<allocator_type>::template rebind_alloc<lockable_pq_type>;
    using operation_policy_data_type = typename operation_traits_type::policy_type::SharedData;

    class Context {
        friend MultiQueue;

       public:
        using key_type = MultiQueue::key_type;
        using value_type = MultiQueue::value_type;
        using pq_type = MultiQueue::lockable_pq_type;
        using operation_policy_data_type = MultiQueue::operation_policy_data_type;

       private:
        size_type num_pqs_{};
        lockable_pq_type *pq_list_{nullptr};
        [[no_unique_address]] operation_policy_data_type data_;
        [[no_unique_address]] key_compare comp_;
        [[no_unique_address]] internal_allocator_type alloc_;

        explicit Context(size_type num_pqs, operation_config_type const &config, priority_queue_type const &pq,
                         key_compare const &comp, allocator_type const &alloc)
            : num_pqs_{num_pqs},
              pq_list_{std::allocator_traits<internal_allocator_type>::allocate(alloc_, num_pqs)},
              data_{config, num_pqs},
              comp_{comp},
              alloc_{alloc} {
            assert(num_pqs_ > 0);

            for (auto *it = pq_list_; it != pq_list_ + num_pqs_; ++it) {
                std::allocator_traits<internal_allocator_type>::construct(alloc_, it, pq);
            }
        }

        explicit Context(size_type num_pqs, typename priority_queue_type::size_type initial_capacity,
                         operation_config_type const &config, priority_queue_type const &pq, key_compare const &comp,
                         allocator_type const &alloc)
            : Context(num_pqs, config, pq, comp, alloc) {
            auto cap_per_queue = (initial_capacity + num_pqs - 1) / num_pqs;
            for (auto *it = pq_list_; it != pq_list_ + num_pqs_; ++it) {
                it->get_pq().reserve(cap_per_queue);
            }
        }

        template <typename ForwardIt>
        explicit Context(ForwardIt first, ForwardIt last, operation_config_type const &config, key_compare const &comp,
                         allocator_type const &alloc)
            : num_pqs_{std::distance(first, last)},
              pq_list_{std::allocator_traits<internal_allocator_type>::allocate(alloc_, num_pqs_)},
              data_{config, std::distance(first, last)},
              comp_{comp},
              alloc_(alloc) {
            for (auto *it = pq_list_; it != pq_list_ + num_pqs_; ++it, ++first) {
                std::allocator_traits<internal_allocator_type>::construct(alloc_, it, *first);
            }
        }

        ~Context() noexcept {
            for (auto *it = pq_list_; it != pq_list_ + num_pqs_; ++it) {
                std::allocator_traits<internal_allocator_type>::destroy(alloc_, it);
            }
            std::allocator_traits<internal_allocator_type>::deallocate(alloc_, pq_list_, num_pqs_);
        }

       public:
        Context(const Context &) = delete;
        Context(Context &&) = delete;
        Context &operator=(const Context &) = delete;
        Context &operator=(Context &&) = delete;

        [[nodiscard]] lockable_pq_type *pq_list() const noexcept {
            return pq_list_;
        }

        [[nodiscard]] constexpr size_type num_pqs() const noexcept {
            return num_pqs_;
        }

        [[nodiscard]] operation_policy_data_type &operation_policy_data() noexcept {
            return data_;
        }

        [[nodiscard]] bool compare(key_type const &lhs, key_type const &rhs) const noexcept {
            return Sentinel::compare(comp_, lhs, rhs);
        }

        [[nodiscard]] static constexpr key_type sentinel() noexcept {
            return Sentinel::sentinel();
        }

        [[nodiscard]] static constexpr bool is_sentinel(key_type const &key) noexcept {
            return Sentinel::is_sentinel(key);
        }

        [[nodiscard]] static constexpr key_type get_key(value_type const &value) noexcept {
            return KeyOfValue::get(value);
        }
    };

    Context context_;

   public:
    using handle_type =
        Handle<Context, typename operation_traits_type::policy_type, operation_traits_type::scan_if_empty>;

    explicit MultiQueue(size_type num_pqs, operation_config_type const &config = {},
                        priority_queue_type const &pq = priority_queue_type(), key_compare const &comp = {},
                        allocator_type const &alloc = {})
        : context_{num_pqs, config, pq, comp, internal_allocator_type(alloc)} {
    }

    explicit MultiQueue(size_type num_pqs, typename priority_queue_type::size_type initial_capacity,
                        operation_config_type const &config = {}, priority_queue_type const &pq = priority_queue_type(),
                        key_compare const &comp = {}, allocator_type const &alloc = {})
        : context_{num_pqs, initial_capacity, config, pq, comp, internal_allocator_type(alloc)} {
    }

    template <typename ForwardIt>
    explicit MultiQueue(ForwardIt first, ForwardIt last, operation_config_type const &config = {},
                        key_compare const &comp = {}, allocator_type const &alloc = {})
        : context_{first, last, config, comp, internal_allocator_type(alloc)} {
    }

    handle_type get_handle() noexcept {
        return handle_type(context_);
    }

    [[nodiscard]] size_type num_pqs() const noexcept {
        return context_.num_pqs_;
    }

    [[nodiscard]] key_compare key_comp() const {
        return context_.comp_;
    }

    [[nodiscard]] allocator_type get_allocator() const {
        return allocator_type_(context_.alloc_);
    }
};

template <typename T, typename Compare = std::less<>, typename OperationTraits = DefaultOperationTraits,
          typename PriorityQueue = DefaultPriorityQueue<T, utils::Identity, Compare>,
          typename Sentinel = sentinel::Implicit<T, Compare>, typename Allocator = std::allocator<PriorityQueue>>
using ValueMultiQueue = MultiQueue<T, T, utils::Identity, Compare, OperationTraits, PriorityQueue, Sentinel, Allocator>;

template <typename Key, typename T, typename Compare = std::less<>, typename OperationTraits = DefaultOperationTraits,
          typename PriorityQueue = DefaultPriorityQueue<std::pair<Key, T>, utils::PairFirst, Compare>,
          typename Sentinel = sentinel::Implicit<T, Compare>, typename Allocator = std::allocator<PriorityQueue>>
using KeyValueMultiQueue =
    MultiQueue<Key, std::pair<Key, T>, utils::PairFirst, Compare, OperationTraits, PriorityQueue, Sentinel, Allocator>;
}  // namespace multiqueue
