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
#include <random>
#include <stdexcept>
#include <type_traits>

namespace multiqueue {

namespace detail {

enum class PushResult { Success, Locked };
enum class PopResult { Success, Locked, Invalid, Empty };

template <typename T, typename Compare, StickPolicy Policy, template <typename, typename> typename PriorityQueue,
          typename Allocator>
class MultiQueueImpl {
   public:
    using key_type = unsigned long;
    using value_type = T;
    using value_compare = Compare;
    using policy_impl_type = detail::stick_policy_impl_type<MultiQueueImpl, Policy>;
    using reference = value_type &;
    using const_reference = value_type const &;
    using size_type = std::size_t;
    using allocator_type = Allocator;
    using push_result = PushResult;
    using pop_result = PopResult;

    using pq_type = GuardedPQ<PriorityQueue<value_type, value_compare>>;

   private:
    using shared_data_type = typename policy_impl_type::SharedData;
    using pq_alloc_type = typename std::allocator_traits<allocator_type>::template rebind_alloc<pq_type>;
    using pq_alloc_traits = std::allocator_traits<pq_alloc_type>;

    pq_type *pq_list_;
    size_type num_pqs_;
    Config config_;
    [[no_unique_address]] value_compare comp_;
    [[no_unique_address]] pq_alloc_type alloc_;
    [[no_unique_address]] shared_data_type shared_data_;

   public:
    MultiQueueImpl(MultiQueueImpl const &) = delete;
    MultiQueueImpl(MultiQueueImpl &&) = delete;
    MultiQueueImpl &operator=(MultiQueueImpl const &) = delete;
    MultiQueueImpl &operator=(MultiQueueImpl &&) = delete;

    explicit MultiQueueImpl(size_type n, std::size_t cap, Config const &c, value_compare const &comp,
                            allocator_type const &a)
        : num_pqs_{n}, config_{c}, comp_{comp}, alloc_(a), shared_data_(n) {
        assert(n > 0);

        auto cap_per_queue = (2 * cap) / n;
        pq_list_ = pq_alloc_traits::allocate(alloc_, num_pqs_);
        for (pq_type *pq = pq_list_; pq != pq_list_ + num_pqs_; ++pq) {
            pq_alloc_traits::construct(alloc_, pq, cap_per_queue, value_comp());
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
            return key == std::numeric_limits<unsigned long>::max() ? PopResult::Empty
                                                                    : try_pop_from(idx[0], retval
#ifdef MQ_COMPARE_STRICT
                                                                                   ,
                                                                                   key
#endif
                                                                      );
        } else if constexpr (N == 2) {
            std::array<key_type, 2> key = {pq_list_[idx[0]].concurrent_top_key(),
                                           pq_list_[idx[1]].concurrent_top_key()};
            if (key[0] == std::numeric_limits<unsigned long>::max()) {
                return key[1] == std::numeric_limits<unsigned long>::max() ? PopResult::Empty
                                                                           : try_pop_from(idx[1], retval
#ifdef MQ_COMPARE_STRICT
                                                                                          ,
                                                                                          key[1]
#endif
                                                                             );
            }
            if (key[1] == std::numeric_limits<unsigned long>::max()) {
                return try_pop_from(idx[0], retval
#ifdef MQ_COMPARE_STRICT
                                    ,
                                    key[0]
#endif
                );
            }
            size_type i = key[0] > key[1] ? 1 : 0;
            return key[i] == std::numeric_limits<unsigned long>::max() ? PopResult::Empty
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
                if (auto key = pq_list_[idx[i]].concurrent_top_key();
                    key != std::numeric_limits<unsigned long>::max() &&
                    (best_key == std::numeric_limits<unsigned long>::max() || best_key > key)) {
                    best = i;
                    best_key = key;
                }
            }
            return best_key == std::numeric_limits<unsigned long>::max() ? PopResult::Empty
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
            if (key != std::numeric_limits<unsigned long>::max()) {
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

    value_compare value_comp() const {
        return value_compare{comp_};
    }
};

template <typename T, typename Compare>
using DefaultPriorityQueue = BufferedPQ<Heap<T, Compare>>;

}  // namespace detail

template <typename T, typename Compare = std::less<T>, StickPolicy Policy = StickPolicy::None,
          template <typename, typename> typename PriorityQueue = detail::DefaultPriorityQueue,
          typename Allocator = std::allocator<T>>
class MultiQueue {
    using impl_type = detail::MultiQueueImpl<T, Compare, Policy, PriorityQueue, Allocator>;

   public:
    using value_type = typename impl_type::value_type;
    using value_compare = typename impl_type::value_compare;
    using size_type = typename impl_type::size_type;
    using reference = typename impl_type::reference;
    using const_reference = typename impl_type::const_reference;
    using pq_type = typename impl_type::pq_type;
    using allocator_type = typename impl_type::allocator_type;

   private:
    impl_type impl_;

    auto &get_tld() noexcept {
        static std::atomic_int id = 0;
        static thread_local typename impl_type::policy_impl_type policy_impl(id.fetch_add(1), impl_);
        return policy_impl;
    }

    static constexpr unsigned int next_power_of_two(unsigned int n) {
        unsigned int r = 1;
        while (r < n) {
            r <<= 1;
        }
        return r;
    }

   public:
    explicit MultiQueue()
        : impl_(next_power_of_two(static_cast<unsigned int>(Galois::getActiveThreads() * 2)), 0U,
                Config{}, value_compare{}, allocator_type{}) {
    }
    explicit MultiQueue(int num_threads, Config const &cfg = Config{}, value_compare const &comp = value_compare(),
                        allocator_type const &a = allocator_type())
        : impl_(next_power_of_two(static_cast<unsigned int>(num_threads * cfg.pqs_per_thread)), 0U, cfg, comp, a) {
    }

    explicit MultiQueue(int num_threads, std::size_t cap, Config const &cfg = Config{},
                        value_compare const &comp = value_compare(), allocator_type const &a = allocator_type())
        : impl_(next_power_of_two(static_cast<unsigned int>(num_threads * cfg.pqs_per_thread)), cap, cfg, comp, a) {
    }

    void push(const_reference value) {
        get_tld().push(value);
    }
    bool try_pop(reference retval) {
        return get_tld().try_pop(retval);
    }

    [[nodiscard]] Config const &config() const noexcept {
        return impl_.config();
    }

    size_type num_pqs() const noexcept {
        return impl_.num_pqs();
    }

    value_compare value_comp() const {
        return impl_.value_comp();
    }
};

}  // namespace multiqueue
