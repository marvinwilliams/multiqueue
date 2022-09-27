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
#include "multiqueue/third_party/pcg_random.hpp"
#include "multiqueue/value_traits.hpp"

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <ostream>
#include <random>
#include <stdexcept>
#include <string>
#include <type_traits>

#ifdef MULTIQUEUE_ELEMENT_DISTRIBUTION
#include <algorithm>
#include <utility>
#include <vector>
#endif

namespace multiqueue {

namespace detail {

template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits>
struct MultiQueueImplData {
    static_assert(std::is_same_v<Key, typename ValueTraits::key_type>,
                  "MultiQueue must have the same key_type as its ValueTraits");
    static_assert(std::is_same_v<T, typename ValueTraits::mapped_type>,
                  "MultiQueue must have the same mapped_type as its ValueTraits");
    using key_type = Key;
    using mapped_type = typename ValueTraits::mapped_type;
    using value_type = typename ValueTraits::value_type;
    using key_compare = KeyCompare;
    class value_compare {
        friend MultiQueueImplData;
        [[no_unique_address]] key_compare comp;

        explicit value_compare(key_compare const &compare = key_compare{}) : comp{compare} {
        }

       public:
        constexpr bool operator()(value_type const &lhs, value_type const &rhs) const {
            return comp(ValueTraits::key_of_value(lhs), ValueTraits::key_of_value(rhs));
        }
    };
    using value_traits_type = ValueTraits;
    using sentinel_traits_type = SentinelTraits;
    using reference = value_type &;
    using const_reference = value_type const &;
    using size_type = std::size_t;

    using pq_type = GuardedPQ<PriorityQueue<value_type, value_compare>, ValueTraits, SentinelTraits>;

    class sentinel_aware_compare {
        friend MultiQueueImplData;
        [[no_unique_address]] key_compare comp;

        explicit sentinel_aware_compare(key_compare const &compare = key_compare{}) : comp{compare} {
        }

       public:
        constexpr bool operator()(key_type const &lhs, key_type const &rhs) const {
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
    };

    pq_type *pq_list = nullptr;
    size_type num_pqs{0};
    pcg32 rng;
    [[no_unique_address]] key_compare comp;

    explicit MultiQueueImplData(size_type n, std::uint32_t seed, key_compare const &compare)
        : num_pqs{n}, rng(std::seed_seq{seed}), comp{compare} {
    }

    template <typename Generator>
    size_type random_pq_index(Generator &g) noexcept {
        /* return std::uniform_int_distribution<size_type>{0, num_pqs - 1}(g); */
        return g() & (num_pqs - 1);
    }

    size_type random_pq_index() noexcept {
        return random_pq_index(rng);
    }

    value_compare value_comp() const {
        return value_compare{comp};
    }

    sentinel_aware_compare sentinel_aware_comp() const {
        return sentinel_aware_compare{comp};
    }

    static constexpr bool is_sentinel(key_type const &key) noexcept {
        return key == SentinelTraits::sentinel();
    }
};

template <typename T, typename Compare>
using DefaultPriorityQueue = BufferedPQ<Heap<T, Compare>>;

}  // namespace detail

template <typename Key, typename T, typename KeyCompare = std::less<Key>, StickPolicy P = StickPolicy::None,
          template <typename, typename> typename PriorityQueue = detail::DefaultPriorityQueue,
          typename ValueTraits = value_traits<Key, T>, typename SentinelTraits = sentinel_traits<Key, KeyCompare>,
          typename Allocator = std::allocator<Key>>
class MultiQueue {
    using impl_data_type = detail::MultiQueueImplData<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits>;
    using policy_type = stick_policy_impl_type<impl_data_type, P>;

   public:
    using key_type = typename policy_type::key_type;
    using mapped_type = typename policy_type::mapped_type;
    using value_type = typename policy_type::value_type;
    using key_compare = typename policy_type::key_compare;
    using value_compare = typename policy_type::value_compare;
    using size_type = typename policy_type::size_type;
    using reference = typename policy_type::reference;
    using const_reference = typename policy_type::const_reference;
    using pq_type = typename policy_type::pq_type;

    using handle_type = typename policy_type::handle_type;
    using allocator_type = Allocator;

   private:
    using pq_alloc_type = typename std::allocator_traits<allocator_type>::template rebind_alloc<pq_type>;
    using pq_alloc_traits = std::allocator_traits<pq_alloc_type>;

    // False sharing is avoided by class alignment, but the members do not need to reside in individual cache lines,
    // as they are not written concurrently
    policy_type policy_;
    [[no_unique_address]] pq_alloc_type alloc_;

    static constexpr std::size_t next_power_of_two(std::size_t n) {
        return std::size_t{1} << static_cast<unsigned int>(std::ceil(std::log2(n)));
    }

   public:
    explicit MultiQueue(int num_threads, Config const &config, key_compare const &comp = key_compare(),
                        allocator_type const &alloc = allocator_type())
        : policy_{next_power_of_two(static_cast<std::size_t>(num_threads) * config.c), config, comp}, alloc_{alloc} {
        assert(policy_.num_pqs > 0);

        policy_.pq_list = pq_alloc_traits::allocate(alloc_, policy_.num_pqs);
#ifdef MULTIQUEUE_CHECK_ALIGNMENT
        if (reinterpret_cast<std::uintptr_t>(policy_.pq_list) % (BuildConfiguration::Pagesize) != 0) {
            std::abort();
        }
#endif
        for (pq_type *pq = policy_.pq_list; pq != policy_.pq_list + policy_.num_pqs; ++pq) {
            pq_alloc_traits::construct(alloc_, pq, policy_.value_comp());
        }
    }

    explicit MultiQueue(int num_threads, key_compare const &comp = key_compare(),
                        allocator_type const &alloc = allocator_type())
        : MultiQueue(num_threads, Config{}, comp, alloc) {
    }

    MultiQueue(MultiQueue const &) = delete;
    MultiQueue(MultiQueue &&) = delete;
    MultiQueue &operator=(MultiQueue const &) = delete;
    MultiQueue &operator=(MultiQueue &&) = delete;

    ~MultiQueue() noexcept {
        for (pq_type *pq = policy_.pq_list; pq != policy_.pq_list + policy_.num_pqs; ++pq) {
            pq_alloc_traits::destroy(alloc_, pq);
        }
        pq_alloc_traits::deallocate(alloc_, policy_.pq_list, policy_.num_pqs);
    }

    handle_type get_handle() noexcept {
        return policy_.get_handle();
    }

    bool try_pop(reference retval) noexcept {
        pq_type &first = policy_.pq_list[policy_.random_pq_index()];
        pq_type &second = policy_.pq_list[policy_.random_pq_index()];
        if (first.unsafe_empty() && second.unsafe_empty()) {
            return false;
        }
        if (first.unsafe_empty() ||
            (!second.unsafe_empty() && policy_.value_comp()(first.unsafe_top(), second.unsafe_top()))) {
            retval = second.unsafe_top();
            second.unsafe_pop();
        } else {
            retval = first.unsafe_top();
            first.unsafe_pop();
        }
        return true;
    }

    void push(const_reference value) noexcept {
        policy_.pq_list[policy_.random_pq_index()].unsafe_push(value);
    }

    size_type num_pqs() const noexcept {
        return policy_.num_pqs;
    }

    value_compare value_comp() const {
        return policy_.value_comp();
    }
#ifdef MULTIQUEUE_ELEMENT_DISTRIBUTION
    [[nodiscard]] std::vector<std::size_t> get_distribution() const {
        std::vector<std::size_t> distribution(num_pqs());
        std::transform(policy_.pq_list, policy_.pq_list + policy_.num_pqs, distribution.begin(),
                       [](auto const &pq) { return pq.size(); });
        return distribution;
    }

    std::vector<std::size_t> get_top_distribution(std::size_t k) {
        std::vector<std::pair<value_type, std::size_t>> removed_elements;
        removed_elements.reserve(k);
        std::vector<std::size_t> distribution(num_pqs(), 0);
        for (std::size_t i = 0; i < k; ++i) {
            auto pq = std::max_element(policy_.pq_list, policy_.pq_list + policy_.num_pqs,
                                       [&](auto const &lhs, auto const &rhs) {
                                           return policy_.compare(lhs.concurrent_top_key(), rhs.concurrent_top_key());
                                       });
            if (pq->concurrent_top_key() == SentinelTraits::sentinel()) {
                break;
            }
            assert(!pq->unsafe_empty());
            std::pair<value_type, std::size_t> result;
            result.first = pq->unsafe_top();
            pq->unsafe_pop();
            result.second = static_cast<std::size_t>(std::distance(policy_.pq_list, pq));
            removed_elements.push_back(result);
            ++distribution[result.second];
        }
        for (auto [val, index] : removed_elements) {
            policy_.pq_list[index].unsafe_push(std::move(val));
        }
        return distribution;
    }
#endif
};

}  // namespace multiqueue
