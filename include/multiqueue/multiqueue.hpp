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
#include "multiqueue/multiqueue_impl.hpp"
#include "multiqueue/sentinel_traits.hpp"
#include "multiqueue/stick_policies.hpp"
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

template <typename Key, typename T, typename KeyCompare = std::less<>, StickPolicy S = BuildConfig::DefaultStickPolicy,
          template <typename, typename> typename PriorityQueue = BuildConfig::DefaultPriorityQueue,
          typename ValueTraits = value_traits<Key, T>, typename SentinelTraits = sentinel_traits<Key, KeyCompare>,
          typename Allocator = std::allocator<typename ValueTraits::value_type>>
class MultiQueue {
   private:
    using impl_type = MultiQueueImpl<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, S>;
    using pq_type = typename impl_type::pq_type;

   public:
    using key_type = typename impl_type::key_type;
    using mapped_type = typename impl_type::mapped_type;
    using value_type = typename impl_type::value_type;
    using key_compare = typename impl_type::key_compare;
    using value_compare = typename impl_type::value_compare;
    using reference = typename impl_type::reference;
    using const_reference = typename impl_type::const_reference;
    using size_type = typename impl_type::size_type;
    using config_type = typename impl_type::Config;
    using handle_type = typename impl_type::Handle;

    static_assert(std::is_same_v<value_type, typename Allocator::value_type>);

   private:
    using allocator_type = Allocator;
    using alloc_traits = std::allocator_traits<allocator_type>;
    using pq_alloc_type = typename std::allocator_traits<allocator_type>::template rebind_alloc<pq_type>;
    using pq_alloc_traits = std::allocator_traits<pq_alloc_type>;

   public:
    using pointer = typename alloc_traits::pointer;
    using const_pointer = typename alloc_traits::const_pointer;

   private:
    // False sharing is avoided by class alignment, but the members do not need to reside in individual cache lines,
    // as they are not written concurrently
    impl_type impl_;
    [[no_unique_address]] pq_alloc_type alloc_;

#ifdef MULTIQUEUE_CHECK_ALIGNMENT
    bool check_alignment() {
        for (pq_type *s = impl_.pq_list; s != impl_.pq_list + impl_.num_pqs; ++s) {
            if (reinterpret_cast<std::uintptr_t>(s) % (2 * L1_CACHE_LINESIZE) != 0) {
                return false;
            }
        }
        return true;
    }
#endif

   public:
    explicit MultiQueue(unsigned int num_threads, config_type const &config, key_compare const &comp = key_compare(),
                        allocator_type const &alloc = allocator_type())
        : impl_{num_threads * config.c, config}, alloc_{alloc} {
        assert(num_threads > 0);
        assert(config.c > 0);
        impl_.num_pqs = num_threads * config.c;
        impl_.rng.seed(config.seed);
        impl_.comp = comp;

        impl_.pq_list = pq_alloc_traits::allocate(alloc_, impl_.num_pqs);
        for (pq_type *pq = impl_.pq_list; pq != impl_.pq_list + impl_.num_pqs; ++pq) {
            pq_alloc_traits::construct(alloc_, pq, impl_.value_comp());
        }
#ifdef MULTIQUEUE_CHECK_ALIGNMENT
        if (!check_alignment()) {
            std::abort();
        }
#endif
    }

    ~MultiQueue() noexcept {
        for (pq_type *s = impl_.pq_list; s != impl_.pq_list + impl_.num_pqs; ++s) {
            pq_alloc_traits::destroy(alloc_, s);
        }
        pq_alloc_traits::deallocate(alloc_, impl_.pq_list, impl_.num_pqs);
    }

    handle_type get_handle() noexcept {
        return impl_.get_handle();
    }

    bool try_pop(reference retval) noexcept {
        pq_type *first = impl_.pq_list + fastrange64(impl_.rng(), impl_.num_pqs);
        pq_type *second = impl_.pq_list + fastrange64(impl_.rng(), impl_.num_pqs);
        if (!first->unsafe_empty()) {
            if (!second->unsafe_empty()) {
                if (impl_.comp(ValueTraits::key_of_value(first->unsafe_top()),
                               ValueTraits::key_of_value(second->unsafe_top()))) {
                    retval = second->unsafe_pop();
                } else {
                    retval = first->unsafe_pop();
                }
            } else {
                retval = first->unsafe_pop();
            }
            return true;
        }
        if (!second->unsafe_empty()) {
            retval = second->unsafe_pop();
            return true;
        }
        return false;
    }

    void push(const_reference value) noexcept {
        size_type index = fastrange64(impl_.rng(), impl_.num_pqs);
        impl_.pq_list[index].unsafe_push(value);
    }

    constexpr size_type num_pqs() const noexcept {
        return impl_.num_pqs;
    }

#ifdef MULTIQUEUE_ELEMENT_DISTRIBUTION
    std::vector<std::size_t> get_distribution() const {
        std::vector<std::size_t> distribution(num_pqs());
        std::transform(impl_.pq_list, impl_.pq_list + impl_.num_pqs, distribution.begin(),
                       [](auto const &pq) { return pq.size(); });
        return distribution;
    }

    std::vector<std::size_t> get_top_distribution(std::size_t k) {
        std::vector<std::pair<value_type, std::size_t>> removed_elements;
        removed_elements.reserve(k);
        std::vector<std::size_t> distribution(num_pqs(), 0);
        for (std::size_t i = 0; i < k; ++i) {
            auto pq =
                std::max_element(impl_.pq_list, impl_.pq_list + impl_.num_pqs, [&](auto const &lhs, auto const &rhs) {
                    return impl_.compare(lhs.concurrent_top_key(), rhs.concurrent_top_key());
                });
            if (pq->concurrent_top_key() == SentinelTraits::sentinel()) {
                break;
            }
            assert(!pq->unsafe_empty());
            std::pair<value_type, std::size_t> result;
            result.first = pq->unsafe_pop();
            result.second = static_cast<std::size_t>(std::distance(impl_.pq_list, pq));
            removed_elements.push_back(result);
            ++distribution[result.second];
        }
        for (auto [val, index] : removed_elements) {
            impl_.pq_list[index].unsafe_push(std::move(val));
        }
        return distribution;
    }
#endif
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
