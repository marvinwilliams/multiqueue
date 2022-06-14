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

#ifndef L1_CACHE_LINESIZE
#error Need to define L1_CACHE_LINESIZE
#endif

#include "multiqueue/guarded_pq.hpp"
#include "multiqueue/heap.hpp"
#include "multiqueue/sentinel_traits.hpp"
#include "multiqueue/value_traits.hpp"
#include "stick_policy/random.hpp"

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

namespace multiqueue {

template <typename StickPolicy>
struct MultiqueueConfig {
    std::uint64_t seed = 1;
    std::size_t c = 4;
    typename StickPolicy::Config stick_policy_config;
};

template <typename Key, typename T, typename KeyCompare, typename StickPolicy = stick_policy::Random,
          typename ValueTraits = value_traits<Key, T>, typename SentinelTraits = sentinel_traits<Key, KeyCompare>,
          template <typename, typename> typename PriorityQueue = Heap,
          typename Allocator = std::allocator<typename ValueTraits::value_type>>
class MultiQueue {
   public:
    using key_type = typename ValueTraits::key_type;
    using mapped_type = typename ValueTraits::mapped_type;
    using value_type = typename ValueTraits::value_type;
    using key_compare = KeyCompare;
    struct value_compare {
       private:
        [[no_unique_address]] KeyCompare comp_;

       public:
        explicit value_compare(KeyCompare const &comp = KeyCompare{}) : comp_{comp} {
        }

        constexpr bool operator()(typename ValueTraits::value_type const &lhs,
                                  typename ValueTraits::value_type const &rhs) const noexcept {
            return comp_(ValueTraits::key_of_value(lhs), ValueTraits::key_of_value(rhs));
        }
    };
    using allocator_type = Allocator;
    using config_type = MultiqueueConfig<StickPolicy>;
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
    using reference = typename alloc_traits::reference;
    using const_reference = typename alloc_traits::const_reference;

    friend StickPolicy;

    class Handle {
        friend MultiQueue;
        using data_t = typename StickPolicy::handle_data_t;

        data_t data_;
        MultiQueue &mq_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit Handle(MultiQueue &mq, unsigned int id, std::uint64_t seed) noexcept : mq_{mq}, data_{id, seed} {
        }

       public:
        bool try_pop(reference retval) noexcept {
            size_type first_index = StickPolicy::get_pop_pq<0>(mq_, data_);
            size_type second_index = StickPolicy::get_pop_pq<1>(mq_, data_);
            auto first_key = mq_.pq_list_[first_index].top_key();
            auto second_key = mq_.pq_list_[second_index].top_key();
            do {
                if (mq_.compare_with_sentinel(first_key, second_key)) {
                    if (first_key == SentinelTraits::sentinel()) {
                        StickPolicy::pop_failed_callback<0>(data_);
                        StickPolicy::pop_failed_callback<1>(data_);
                        return false;
                    }
                    if (mq_.pq_list_[first_index].try_lock_if_nonempty()) {
                        break;
                    }
                    StickPolicy::pop_failed_callback<0>(data_);
                    first_index = StickPolicy::get_pop_pq<0>(mq_, data_);
                    first_key = mq_.pq_list_[first_index].top_key();
                } else {
                    if (second_key == SentinelTraits::sentinel()) {
                        StickPolicy::pop_failed_callback<0>(data_);
                        StickPolicy::pop_failed_callback<1>(data_);
                        return false;
                    }
                    if (mq_.pq_list_[second_index].try_lock_if_nonempty()) {
                        first_index = second_index;
                        break;
                    }
                    StickPolicy::pop_failed_callback<1>(data_);
                    second_index = StickPolicy::get_pop_pq<1>(mq_, data_);
                    second_key = mq_.pq_list_[second_index].top_key();
                }
            } while (true);
            // first is guaranteed to be nonempty
            mq_.pq_list_[first_index].pop(retval);
            mq_.pq_list_[first_index].unlock();
            StickPolicy::pop_callback(data_);
            return true;
        }

        void push(const_reference value) noexcept {
            size_type index = StickPolicy::get_push_pq(mq_, data_);
            while (!mq_.pq_list_[index].try_lock()) {
                StickPolicy::push_failed_callback(data_);
                index = StickPolicy::get_push_pq(mq_, data_);
            }
            mq_.pq_list_[index].push(value);
            mq_.pq_list_[index].unlock();
            StickPolicy::push_callback(data_);
        }

        void push(value_type &&value) noexcept {
            size_type index = StickPolicy::get_push_pq(mq_, data_);
            while (!mq_.pq_list_[index].try_lock()) {
                StickPolicy::push_failed_callback(data_);
                index = StickPolicy::get_push_pq(mq_, data_);
            }
            mq_.pq_list_[index].push(std::move(value));
            mq_.pq_list_[index].unlock();
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
    bool compare_with_sentinel(key_type const &lhs, key_type const &rhs) noexcept {
        if constexpr (SentinelTraits::is_implicit) {
            return comp_(lhs, rhs);
        } else {
            if (lhs == SentinelTraits::sentinel()) {
                return false;
            }
            if (rhs == SentinelTraits::sentinel()) {
                return true;
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
            pq_alloc_traits::construct(alloc_, pq, comp_);
        }
#ifdef MULTIQUEUE_ABORT_MISALIGNMENT
        abort_on_data_misalignment();
#endif
    }

    explicit MultiQueue(size_type initial_capacity, unsigned int num_threads, config_type const &params,
                        key_compare const &comp = key_compare(), allocator_type const &alloc = allocator_type())
        : num_pqs_{num_threads * params.c},
          rng_{params.seed},
          num_handles_{0},
          comp_{comp},
          alloc_{alloc},
          stick_policy_data_(num_pqs_, params.stick_policy_config) {
        assert(num_threads > 0);
        assert(params.c > 0);
        pq_list_ = pq_alloc_traits::allocate(alloc_, num_pqs_);
        std::size_t cap_per_pq = (2 * initial_capacity) / num_pqs_;
        for (guarded_pq_type *pq = pq_list_; pq != pq_list_ + num_pqs_; ++pq) {
            pq_alloc_traits::construct(alloc_, pq, cap_per_pq, comp_);
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
        if (first->unsafe_empty() || (!second->unsafe_empty() && comp_(ValueTraits::key_of_value(first->unsafe_top()), ValueTraits::key_of_value(second->unsafe_top()))) {
            first = second;
            first_key = second_key;
        }
        if (first->unsafe_empty()) {
            return false;
        }
        // first is guaranteed to be nonempty
        first->pop(retval);
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
        std::transform(pq_list_.get(), pq_list_.get() + num_pqs(), distribution.begin(),
                       [](auto const &pq) { return pq.unsafe_size(); });
        return distribution;
    }

    std::vector<std::size_t> get_top_distribution(std::size_t k) {
        std::vector<std::pair<value_type, std::size_t>> removed_elements;
        removed_elements.reserve(k);
        std::vector<std::size_t> distribution(num_pqs(), 0);
        for (std::size_t i = 0; i < k; ++i) {
            auto min = std::min_element(pq_list_.get(), pq_list_.get() + num_pqs(),
                                        [](auto const &lhs, auto const &rhs) { return lhs.top_key() < rhs.top_key(); });
            if (min->min_key() == Sentinel()()) {
                break;
            }
            assert(!min->empty());
            std::pair<value_type, std::size_t> result;
            min->extract_top(ExtractKey()(result));
            result.second = static_cast<std::size_t>(min - std::begin(pq_list_));
            removed_elements.push_back(result);
            ++distribution[result.second];
        }
        for (auto [val, index] : removed_elements) {
            pq_list_[index].push(std::move(val));
        }
        return distribution;
    }
#endif
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
