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

#ifndef L1_CACHE_LINESIZE
#define L1_CACHE_LINESIZE 64
#endif

#ifndef PAGESIZE
#define PAGESIZE 4096
#endif

#include "multiqueue/config.hpp"
#include "multiqueue/guarded_pq.hpp"
#include "multiqueue/heap.hpp"
#include "multiqueue/multiqueue_impl.hpp"
#include "multiqueue/sentinel_traits.hpp"
#include "multiqueue/stick_policy.hpp"
#include "multiqueue/value_traits.hpp"

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <string>
#include <type_traits>

#ifdef MULTIQUEUE_ELEMENT_DISTRIBUTION
#include <algorithm>
#include <utility>
#include <vector>
#endif

namespace multiqueue {

template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits>
struct MultiQueueImplBase {
    using key_type = Key;
    static_assert(std::is_same_v<key_type, typename ValueTraits::key_type>,
                  "MultiQueue must have the same key_type as its ValueTraits");
    using mapped_type = T;
    static_assert(std::is_same_v<mapped_type, typename ValueTraits::mapped_type>,
                  "MultiQueue must have the same mapped_type as its ValueTraits");
    using value_type = typename ValueTraits::value_type;
    using key_compare = KeyCompare;
    class value_compare {
        friend MultiQueueImplBase;
        [[no_unique_address]] key_compare comp;

        explicit value_compare(key_compare const &c = key_compare{}) : comp{c} {
        }

       public:
        constexpr bool operator()(value_type const &lhs, value_type const &rhs) const noexcept {
            return comp(ValueTraits::key_of_value(lhs), ValueTraits::key_of_value(rhs));
        }
    };

    struct KeyOfValue {
        static constexpr key_type get(value_type const &v) noexcept {
            return ValueTraits::key_of_value(v);
        }
    };
    struct Sentinel {
        static constexpr key_type get() noexcept {
            return SentinelTraits::sentinel();
        }
    };
    using inner_pq_type = PriorityQueue<value_type, value_compare>;
    using pq_type = GuardedPQ<key_type, KeyOfValue, inner_pq_type, Sentinel>;
    using size_type = std::size_t;
    using reference = value_type &;
    using const_reference = value_type const &;

   public:
    pq_type *pq_list;
    size_type num_pqs;
    xoroshiro256starstar rng;
    [[no_unique_address]] key_compare comp;

   public:
    explicit MultiQueueImplBase(size_type n, Config const &config, key_compare const &compare)
        : num_pqs{n}, rng(config.seed), comp{compare} {
    }

    std::size_t random_index() noexcept {
        return fastrange64(rng(), num_pqs);
    }

    bool compare(key_type const &lhs, key_type const &rhs) noexcept {
        if constexpr (!SentinelTraits::is_implicit) {
            if (rhs == SentinelTraits::sentinel()) {
                return false;
            }
            if (lhs == SentinelTraits::sentinel()) {
                return true;
            }
        }
        return comp(lhs, rhs);
    }

    value_compare value_comp() const {
        return value_compare{comp};
    }
};

template <typename T, typename Compare>
using DefaultPriorityQueue = BufferedPQ<64, 64, Heap<T, Compare, 8>>;

template <typename Key, typename T, typename KeyCompare = std::less<Key>, StickPolicy P = StickPolicy::None,
          template <typename, typename> typename PriorityQueue = DefaultPriorityQueue,
          typename ValueTraits = value_traits<Key, T>, typename SentinelTraits = sentinel_traits<Key, KeyCompare>,
          typename Allocator = std::allocator<Key>>
class MultiQueue {
    using impl_base_type = MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits>;
    using impl_type = MultiQueueImpl<impl_base_type, P>;

   public:
    using key_type = typename impl_type::key_type;
    using mapped_type = typename impl_type::mapped_type;
    using value_type = typename impl_type::value_type;
    using key_compare = typename impl_type::key_compare;
    using value_compare = typename impl_type::value_compare;
    using size_type = typename impl_type::size_type;
    using reference = typename impl_type::reference;
    using const_reference = typename impl_type::const_reference;
    using handle_type = typename impl_type::handle_type;
    using inner_pq_type = typename impl_type::inner_pq_type;
    using pq_type = typename impl_type::pq_type;
    using allocator_type = Allocator;

   private:
    using pq_alloc_type = typename std::allocator_traits<allocator_type>::template rebind_alloc<pq_type>;
    using pq_alloc_traits = std::allocator_traits<pq_alloc_type>;

    // False sharing is avoided by class alignment, but the members do not need to reside in individual cache lines,
    // as they are not written concurrently
    impl_type impl_;
    [[no_unique_address]] pq_alloc_type alloc_;

   public:
    explicit MultiQueue(unsigned int num_threads, Config const &config, key_compare const &comp = key_compare(),
                        allocator_type const &alloc = allocator_type())
        : impl_{num_threads, config, comp}, alloc_{alloc} {
        assert(impl_.num_pqs > 0);

        impl_.pq_list = pq_alloc_traits::allocate(alloc_, impl_.num_pqs);
#ifdef MULTIQUEUE_CHECK_ALIGNMENT
        if (reinterpret_cast<std::uintptr_t>(impl_.pq_list) % (GUARDED_PQ_ALIGNMENT) != 0) {
            std::abort();
        }
#endif
        for (pq_type *pq = impl_.pq_list; pq != impl_.pq_list + impl_.num_pqs; ++pq) {
            pq_alloc_traits::construct(alloc_, pq, impl_.value_comp());
        }
    }

    ~MultiQueue() noexcept {
        for (pq_type *pq = impl_.pq_list; pq != impl_.pq_list + impl_.num_pqs; ++pq) {
            pq_alloc_traits::destroy(alloc_, pq);
        }
        pq_alloc_traits::deallocate(alloc_, impl_.pq_list, impl_.num_pqs);
    }

    handle_type get_handle() noexcept {
        return impl_.get_handle();
    }

    bool try_pop(reference retval) noexcept {
        pq_type *first = impl_.pq_list + impl_.random_index();
        pq_type *second = impl_.pq_list + impl_.random_index();
        if (!first->unsafe_empty()) {
            if (!second->unsafe_empty()) {
                if (impl_.comp(ValueTraits::key_of_value(first->unsafe_top()),
                               ValueTraits::key_of_value(second->unsafe_top()))) {
                    retval = second->unsafe_top();
                    second->unsafe_pop();
                } else {
                    retval = first->unsafe_top();
                    first->unsafe_pop();
                }
            } else {
                retval = first->unsafe_top();
                first->unsafe_pop();
            }
            return true;
        }
        if (!second->unsafe_empty()) {
            retval = second->unsafe_top();
            second->unsafe_pop();
            return true;
        }
        return false;
    }

    void push(const_reference value) noexcept {
        size_type index = impl_.random_index();
        impl_.pq_list[index].unsafe_push(value);
    }

    size_type num_pqs() const noexcept {
        return impl_.num_pqs;
    }

    value_compare value_comp() const {
        return impl_.value_comp();
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
            result.first = pq->unsafe_top();
            pq->unsafe_pop();
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

    std::ostream &describe(std::ostream &out) const {
        out << "MultiQueue\n";
        out << "Number of PQs: " << impl_.num_pqs << '\n';
        out << "Sentinel: " << SentinelTraits::sentinel() << " ("
            << (SentinelTraits::is_implicit ? "implicit" : "explicit") << ")\n";
        impl_.describe(out);
        return out;
    }
};

}  // namespace multiqueue
