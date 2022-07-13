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
#ifndef MULTIQUEUE_HPP_INCLUDED
#define MULTIQUEUE_HPP_INCLUDED

#include "multiqueue/build_config.hpp"

#include "multiqueue/guarded_pq.hpp"
#include "multiqueue/sentinel_traits.hpp"
#include "multiqueue/value_traits.hpp"

#include "multiqueue/external/fastrange.h"
#include "multiqueue/external/xoroshiro256starstar.hpp"

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

#ifdef MULTIQUEUE_ELEMENT_DISTRIBUTION
#include <algorithm>
#include <utility>
#include <vector>
#endif

#ifndef L1_CACHE_LINESIZE
#error Need to define L1_CACHE_LINESIZE
#endif

namespace multiqueue {

template <typename StickPolicy>
struct MultiQueueConfig {
    std::uint64_t seed = 1;
    std::size_t c = 4;
    typename StickPolicy::Config stick_policy_config;
};

template <typename Key, typename T, typename KeyCompare = std::less<>,
          typename StickPolicy = BuildConfig::DefaultStickPolicy,
          template <typename, typename> typename PriorityQueue = BuildConfig::DefaultPriorityQueue,
          typename ValueTraits = value_traits<Key, T>, typename SentinelTraits = sentinel_traits<Key, KeyCompare>,
          typename Allocator = std::allocator<typename ValueTraits::value_type>>
class MultiQueue {
   public:
    static_assert(std::is_same_v<Key, typename ValueTraits::key_type> &&
                      std::is_same_v<T, typename ValueTraits::mapped_type>,
                  "Key and T must be the same in ValueTraits");
    static_assert(std::is_same_v<Key, typename SentinelTraits::type>, "Key must be the same as type in SentinelTraits");

    using key_type = typename ValueTraits::key_type;
    using mapped_type = typename ValueTraits::mapped_type;
    using value_type = typename ValueTraits::value_type;
    using key_compare = KeyCompare;
    class value_compare {
        friend class MultiQueue<Key, T, KeyCompare, StickPolicy, PriorityQueue, ValueTraits, SentinelTraits, Allocator>;

       protected:
        [[no_unique_address]] KeyCompare comp;

        explicit value_compare(KeyCompare const &c) : comp{c} {
        }

       public:
        constexpr bool operator()(value_type const &lhs, value_type const &rhs) const noexcept {
            return comp(ValueTraits::key_of_value(lhs), ValueTraits::key_of_value(rhs));
        }
    };
    using reference = value_type &;
    using const_reference = value_type const &;
    using allocator_type = Allocator;
    using config_type = MultiQueueConfig<StickPolicy>;
    using size_type = std::size_t;

    static_assert(std::is_same_v<value_type, typename Allocator::value_type>);

   private:
    using alloc_traits = std::allocator_traits<allocator_type>;
    using pq_type = PriorityQueue<typename ValueTraits::value_type, value_compare>;
    using guarded_pq_type = GuardedPQ<ValueTraits, SentinelTraits, pq_type>;
    using pq_alloc_type = typename std::allocator_traits<allocator_type>::template rebind_alloc<guarded_pq_type>;
    using pq_alloc_traits = std::allocator_traits<pq_alloc_type>;

   public:
    using pointer = typename alloc_traits::pointer;
    using const_pointer = typename alloc_traits::const_pointer;

    friend StickPolicy;

    class Handle {
        friend MultiQueue;
        using data_t = typename StickPolicy::ThreadData;

        data_t data_;
        MultiQueue &mq_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit Handle(MultiQueue &mq, unsigned int id, std::uint64_t seed) noexcept : data_{id, seed}, mq_{mq} {
        }

       public:
        bool try_pop(reference retval) noexcept {
            size_type first_index = StickPolicy::get_pop_pq(mq_.num_pqs_, 0, data_, mq_.stick_policy_data_);
            size_type second_index = StickPolicy::get_pop_pq(mq_.num_pqs_, 1, data_, mq_.stick_policy_data_);
            auto first_key = mq_.pq_list_[first_index].concurrent_top_key();
            auto second_key = mq_.pq_list_[second_index].concurrent_top_key();
            do {
                if (!mq_.compare(first_key, second_key)) {
                    if (first_key == SentinelTraits::sentinel()) {
                        StickPolicy::pop_failed_callback(0, data_);
                        StickPolicy::pop_failed_callback(1, data_);
                        return false;
                    }
                    if (mq_.pq_list_[first_index].try_pop(retval)) {
                        break;
                    }
                    StickPolicy::pop_failed_callback(0, data_);
                    first_index = StickPolicy::get_pop_pq(mq_.num_pqs_, 0, data_, mq_.stick_policy_data_);
                    first_key = mq_.pq_list_[first_index].concurrent_top_key();
                } else {
                    if (second_key == SentinelTraits::sentinel()) {
                        StickPolicy::pop_failed_callback(0, data_);
                        StickPolicy::pop_failed_callback(1, data_);
                        return false;
                    }
                    if (mq_.pq_list_[second_index].try_pop(retval)) {
                        break;
                    }
                    StickPolicy::pop_failed_callback(1, data_);
                    second_index = StickPolicy::get_pop_pq(mq_.num_pqs_, 1, data_, mq_.stick_policy_data_);
                    second_key = mq_.pq_list_[second_index].concurrent_top_key();
                }
            } while (true);
            StickPolicy::pop_callback(data_);
            return true;
        }

        void push(const_reference value) noexcept {
            size_type index = StickPolicy::get_push_pq(mq_, data_);
            while (!mq_.pq_list_[index].try_push(value)) {
                StickPolicy::push_failed_callback(data_);
                index = StickPolicy::get_push_pq(mq_, data_);
            }
            StickPolicy::push_callback(data_);
        }

        void push(value_type &&value) noexcept {
            size_type index = StickPolicy::get_push_pq(mq_.num_pqs_, data_, mq_.stick_policy_data_);
            while (!mq_.pq_list_[index].try_push(std::move((value)))) {
                StickPolicy::push_failed_callback(data_);
                index = StickPolicy::get_push_pq(mq_.num_pqs_, data_, mq_.stick_policy_data_);
            }
            StickPolicy::push_callback(data_);
        }

        [[nodiscard]] bool is_empty(size_type pos) noexcept {
            assert(pos < mq_.num_pqs());
            return mq_.pq_list_[pos].concurrent_empty();
        }
    };

   private:
    // False sharing is avoided by class alignment, but the members do not need to reside in individual cache lines,
    // as they are not written concurrently
    guarded_pq_type *pq_list_;
    size_type num_pqs_;
    xoroshiro256starstar rng_;
    unsigned int num_handles_;
    [[no_unique_address]] key_compare comp_;
    [[no_unique_address]] pq_alloc_type alloc_;
    // strategy data in separate cache line, as it might be written to
    [[no_unique_address]] alignas(2 * L1_CACHE_LINESIZE) typename StickPolicy::GlobalData stick_policy_data_;

#ifdef MULTIQUEUE_ABORT_MISALIGNMENT
    void abort_on_data_misalignment() {
        for (guarded_pq_type *s = pq_list_; s != pq_list_ + num_pqs(); ++s) {
            if (reinterpret_cast<std::uintptr_t>(s) % (2 * L1_CACHE_LINESIZE) != 0) {
                std::abort();
            }
        }
    }
#endif

    bool compare(key_type const &lhs, key_type const &rhs) noexcept {
        if constexpr (SentinelTraits::is_implicit) {
            return comp_(lhs, rhs);
        } else {
            if (lhs == SentinelTraits::sentinel()) {
                return true;
            }
            if (rhs == SentinelTraits::sentinel()) {
                return false;
            }
            return comp_(lhs, rhs);
        }
    }

   public:
    explicit MultiQueue(unsigned int num_threads, config_type const &params, key_compare const &comp = key_compare(),
                        allocator_type const &alloc = allocator_type())
        : num_pqs_{num_threads * params.c},
          rng_{params.seed},
          num_handles_{0},
          comp_{comp},
          alloc_{alloc},
          stick_policy_data_(num_pqs_, params.stick_policy_config) {
        assert(num_threads > 0);
        assert(params.c > 0);
        pq_list_ = pq_alloc_traits::allocate(alloc_, num_pqs_);
        for (guarded_pq_type *pq = pq_list_; pq != pq_list_ + num_pqs_; ++pq) {
            pq_alloc_traits::construct(alloc_, pq, value_compare{comp_});
        }
#ifdef MULTIQUEUE_ABORT_MISALIGNMENT
        abort_on_data_misalignment();
#endif
    }

    ~MultiQueue() noexcept {
        for (guarded_pq_type *s = pq_list_; s != pq_list_ + num_pqs_; ++s) {
            pq_alloc_traits::destroy(alloc_, s);
        }
        pq_alloc_traits::deallocate(alloc_, pq_list_, num_pqs_);
    }

    Handle get_handle() noexcept {
        static std::mutex m;
        std::scoped_lock l{m};
        return Handle{*this, num_handles_++, rng_()};
    }

    bool try_pop(reference retval) noexcept {
        guarded_pq_type *first = pq_list_ + fastrange64(rng_(), num_pqs_);
        guarded_pq_type *second = pq_list_ + fastrange64(rng_(), num_pqs_);
        if (first->unsafe_empty() && second->unsafe_empty()) {
            return false;
        }
        if (first->unsafe_empty()) {
            retval = second->pop();
            second->update_top();
        } else if (second->unsafe_empty()) {
            retval = first->pop();
            first->update_top();
        } else {
            if (comp_(ValueTraits::key_of_value(first->unsafe_top()),
                      ValueTraits::key_of_value(second->unsafe_top()))) {
                first = second;
            }
            retval = first->pop();
            first->update_top();
        }
        return true;
    }

    void push(const_reference value) noexcept {
        guarded_pq_type *pq = pq_list_ + fastrange64(rng_(), num_pqs_);
        while (!pq->try_lock()) {
            pq = pq_list_ + fastrange64(rng_(), num_pqs_);
        }
        pq->push(value);
    }

    void push(value_type &&value) noexcept {
        guarded_pq_type *pq = pq_list_ + fastrange64(rng_(), num_pqs_);
        while (!pq->try_lock()) {
            pq = pq_list_ + fastrange64(rng_(), num_pqs_);
        }
        pq->push(std::move(value));
    }

    constexpr size_type num_pqs() const noexcept {
        return num_pqs_;
    }

#ifdef MULTIQUEUE_ELEMENT_DISTRIBUTION
    std::vector<std::size_t> get_distribution() const {
        std::vector<std::size_t> distribution(num_pqs());
        std::transform(pq_list_, pq_list_ + num_pqs_, distribution.begin(), [](auto const &pq) { return pq.size(); });
        return distribution;
    }

    std::vector<std::size_t> get_top_distribution(std::size_t k) {
        std::vector<std::pair<value_type, std::size_t>> removed_elements;
        removed_elements.reserve(k);
        std::vector<std::size_t> distribution(num_pqs(), 0);
        for (std::size_t i = 0; i < k; ++i) {
            auto pq = std::max_element(pq_list_, pq_list_ + num_pqs_, [&](auto const &lhs, auto const &rhs) {
                return compare(lhs.concurrent_top_key(), rhs.concurrent_top_key());
            });
            if (pq->concurrent_top_key() == SentinelTraits::sentinel()) {
                break;
            }
            assert(!pq->unsafe_empty());
            std::pair<value_type, std::size_t> result;
            result.first = pq->unsafe_pop();
            result.second = static_cast<std::size_t>(std::distance(pq_list_, pq));
            removed_elements.push_back(result);
            ++distribution[result.second];
        }
        for (auto [val, index] : removed_elements) {
            pq_list_[index].unsafe_push(std::move(val));
        }
        return distribution;
    }
#endif
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
