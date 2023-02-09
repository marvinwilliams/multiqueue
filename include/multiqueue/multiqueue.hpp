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
#include "multiqueue/config.hpp"
#include "multiqueue/guarded_pq.hpp"
#include "multiqueue/heap.hpp"
#include "multiqueue/sentinel_traits.hpp"
#include "multiqueue/stick_policy.hpp"
#include "multiqueue/value_traits.hpp"

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <ostream>
#include <random>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace multiqueue {

namespace detail {

enum class PushResult { Success, Locked };
enum class PopResult { Success, Locked, Invalid, Empty };

template <typename Key, typename T, typename KeyCompare, StickPolicy Policy,
          template <typename, typename> typename PriorityQueue, typename ValueTraits, typename SentinelTraits,
          typename Allocator>
class MultiQueueImpl {
    static_assert(std::is_same_v<Key, typename ValueTraits::key_type>,
                  "MultiQueue must have the same key_type as its ValueTraits");
    static_assert(std::is_same_v<T, typename ValueTraits::mapped_type>,
                  "MultiQueue must have the same mapped_type as its ValueTraits");

   public:
    using key_type = Key;
    using mapped_type = typename ValueTraits::mapped_type;
    using value_type = typename ValueTraits::value_type;
    using key_compare = KeyCompare;
    using value_traits_type = ValueTraits;
    using policy_impl_type = detail::stick_policy_impl_type<MultiQueueImpl, Policy>;
    using sentinel_traits_type = SentinelTraits;
    using reference = value_type &;
    using const_reference = value_type const &;
    using size_type = std::size_t;
    using allocator_type = Allocator;
    using push_result = PushResult;
    using pop_result = PopResult;

    class value_compare {
        friend MultiQueueImpl;
        [[no_unique_address]] key_compare comp;

        explicit value_compare(key_compare const &compare = key_compare{}) : comp{compare} {
        }

       public:
        constexpr bool operator()(value_type const &lhs, value_type const &rhs) const {
            return comp(ValueTraits::key_of_value(lhs), ValueTraits::key_of_value(rhs));
        }
    };

    using pq_type = GuardedPQ<PriorityQueue<value_type, value_compare>, ValueTraits, SentinelTraits>;

   private:
    using shared_data_type = typename policy_impl_type::SharedData;
    using pq_alloc_type = typename std::allocator_traits<allocator_type>::template rebind_alloc<pq_type>;
    using pq_alloc_traits = std::allocator_traits<pq_alloc_type>;

    pq_type *pq_list_;
    size_type num_pqs_;
    Config config_;
    [[no_unique_address]] key_compare comp_;
    [[no_unique_address]] pq_alloc_type alloc_;
    [[no_unique_address]] shared_data_type shared_data_;

   public:
    MultiQueueImpl(MultiQueueImpl const &) = delete;
    MultiQueueImpl(MultiQueueImpl &&) = delete;
    MultiQueueImpl &operator=(MultiQueueImpl const &) = delete;
    MultiQueueImpl &operator=(MultiQueueImpl &&) = delete;

    explicit MultiQueueImpl(size_type n, Config const &c, key_compare const &kc, allocator_type const &a)
        : num_pqs_{n}, config_{c}, comp_{kc}, alloc_(a), shared_data_(n) {
        assert(n > 0);

        pq_list_ = pq_alloc_traits::allocate(alloc_, num_pqs_);
        for (pq_type *pq = pq_list_; pq != pq_list_ + num_pqs_; ++pq) {
            pq_alloc_traits::construct(alloc_, pq, value_comp());
        }
    }

    ~MultiQueueImpl() noexcept {
        for (pq_type *pq = pq_list_; pq != pq_list_ + num_pqs_; ++pq) {
            pq_alloc_traits::destroy(alloc_, pq);
        }
        pq_alloc_traits::deallocate(alloc_, pq_list_, num_pqs_);
    }

    [[nodiscard]] size_type num_pqs() const noexcept {
        return num_pqs_;
    }

    [[nodiscard]] Config const &config() const noexcept {
        return config_;
    }

    [[nodiscard]] shared_data_type &shared_data() noexcept {
        return shared_data_;
    }

    PushResult try_push(size_type idx, const_reference value) {
        if (!pq_list_[idx].try_lock()) {
            return PushResult::Locked;
        }
        pq_list_[idx].unsafe_push(value);
        pq_list_[idx].unlock();
        return PushResult::Success;
    }

    PopResult try_pop_from(size_type idx, reference retval
#ifdef MQ_COMPARE_STRICT
                           ,
                           key_type const &ref_key
#endif
    ) {
        assert(idx < num_pqs_);
        if (!pq_list_[idx].try_lock()) {
            return PopResult::Locked;
        }
        if (pq_list_[idx].unsafe_empty()
#ifdef MQ_COMPARE_STRICT
            || ValueTraits::key_of_value(pq_list_[idx].unsafe_top()) != ref_key
#endif
        ) {
            // Top got changed before locking
            pq_list_[idx].unlock();
            return PopResult::Invalid;
        }
        retval = pq_list_[idx].unsafe_top();
        pq_list_[idx].unsafe_pop();
        pq_list_[idx].unlock();
        return PopResult::Success;
    }

    template <std::size_t N>
    PopResult try_pop_compare(std::array<size_type, N> const &idx, reference retval) {
        if constexpr (N == 1) {
            auto key = pq_list_[idx[0]].concurrent_top_key();
            return key == SentinelTraits::sentinel() ? PopResult::Empty
                                                     : try_pop_from(idx[0], retval
#ifdef MQ_COMPARE_STRICT
                                                                    ,
                                                                    key
#endif
                                                       );
        } else if constexpr (N == 2) {
            std::array<key_type, 2> key = {pq_list_[idx[0]].concurrent_top_key(),
                                           pq_list_[idx[1]].concurrent_top_key()};
            if constexpr (!SentinelTraits::is_implicit) {
                if (key[0] == SentinelTraits::sentinel()) {
                    return key[1] == SentinelTraits::sentinel() ? PopResult::Empty
                                                                : try_pop_from(idx[1], retval
#ifdef MQ_COMPARE_STRICT
                                                                               ,
                                                                               key[1]
#endif
                                                                  );
                }
                if (key[1] == SentinelTraits::sentinel()) {
                    return try_pop_from(idx[0], retval
#ifdef MQ_COMPARE_STRICT
                                        ,
                                        key[0]
#endif
                    );
                }
            }
            size_type i = comp_(key[0], key[1]) ? 1 : 0;
            return key[i] == SentinelTraits::sentinel() ? PopResult::Empty
                                                        : try_pop_from(idx[i], retval
#ifdef MQ_COMPARE_STRICT
                                                                       ,
                                                                       key[i]
#endif
                                                          );
        } else {
            size_type best = 0;
            auto best_key = pq_list_[idx[best]].concurrent_top_key();
            for (size_type i = 1; i < N; ++i) {
                if (auto key = pq_list_[idx[i]].concurrent_top_key(); key != SentinelTraits::sentinel() &&
                    (best_key == SentinelTraits::sentinel() || comp_(best_key, key))) {
                    best = i;
                    best_key = key;
                }
            }
            return best_key == SentinelTraits::sentinel() ? PopResult::Empty
                                                          : try_pop_from(idx[best], retval
#ifdef MQ_COMPARE_STRICT
                                                                         ,
                                                                         best_key
#endif
                                                            );
        }
    }

    bool try_pop_any(size_type start_idx, reference retval) {
        assert(start_idx < num_pqs_);
        for (size_type i = 0; i < num_pqs_; ++i) {
            auto idx = (start_idx + i) % num_pqs_;
            auto key = pq_list_[idx].concurrent_top_key();
            if (key != SentinelTraits::sentinel()) {
                auto result = try_pop_from(idx, retval
#ifdef MQ_COMPARE_STRICT
                                           ,
                                           key
#endif
                );
                if (result == PopResult::Success) {
                    return true;
                }
            }
        }
        return false;
    }

    key_compare key_comp() const {
        return comp_;
    }

    value_compare value_comp() const {
        return value_compare{comp_};
    }
};

template <typename T, typename Compare>
using DefaultPriorityQueue = BufferedPQ<Heap<T, Compare>>;

}  // namespace detail

template <typename Key, typename T, typename KeyCompare = std::less<Key>, StickPolicy Policy = StickPolicy::None,
          template <typename, typename> typename PriorityQueue = detail::DefaultPriorityQueue,
          typename ValueTraits = value_traits<Key, T>, typename SentinelTraits = sentinel_traits<Key, KeyCompare>,
          typename Allocator = std::allocator<Key>>
class MultiQueue {
    using impl_type =
        detail::MultiQueueImpl<Key, T, KeyCompare, Policy, PriorityQueue, ValueTraits, SentinelTraits, Allocator>;

   public:
    using key_type = typename impl_type::key_type;
    using mapped_type = typename impl_type::mapped_type;
    using value_type = typename impl_type::value_type;
    using key_compare = typename impl_type::key_compare;
    using value_compare = typename impl_type::value_compare;
    using size_type = typename impl_type::size_type;
    using reference = typename impl_type::reference;
    using const_reference = typename impl_type::const_reference;
    using pq_type = typename impl_type::pq_type;
    using allocator_type = typename impl_type::allocator_type;

    class Handle {
        friend MultiQueue;
        typename impl_type::policy_impl_type policy_impl_;

        explicit Handle(int id, impl_type &d) noexcept : policy_impl_(id, d) {
        }

       public:
        void push(const_reference value) {
            policy_impl_.push(value);
        }

        bool try_pop(reference retval) {
            return policy_impl_.try_pop(retval);
        }

        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle &operator=(Handle &&) noexcept = default;
        Handle(Handle &&) noexcept = default;
        ~Handle() = default;
    };

   private:
    impl_type impl_;

    static constexpr unsigned int next_power_of_two(unsigned int n) {
        unsigned int r = 1;
        while (r < n) {
            r <<= 1;
        }
        return r;
    }

   public:
    explicit MultiQueue(int num_threads, Config const &cfg, key_compare const &kc = key_compare(),
                        allocator_type const &a = allocator_type())
        : impl_(next_power_of_two(static_cast<unsigned int>(num_threads * cfg.c)), cfg, kc, a) {
    }

    explicit MultiQueue(int num_threads, key_compare const &kc = key_compare(),
                        allocator_type const &a = allocator_type())
        : MultiQueue(num_threads, Config{}, kc, a) {
    }

    Handle get_handle(int id) noexcept {
        return Handle(id, impl_);
    }

    [[nodiscard]] Config const &config() const noexcept {
        return impl_.config();
    }

    size_type num_pqs() const noexcept {
        return impl_.num_pqs();
    }

    key_compare key_comp() const {
        return impl_.key_comp();
    }

    value_compare value_comp() const {
        return impl_.value_comp();
    }
};

}  // namespace multiqueue
