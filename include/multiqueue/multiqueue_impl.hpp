/**
******************************************************************************
* @file:   multiqueue_impl.hpp
*
* @author: Marvin Williams
* @date:   2021/07/20 17:19
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef MULTIQUEUE_IMPL_HPP_INCLUDED
#define MULTIQUEUE_IMPL_HPP_INCLUDED

#include "multiqueue/guarded_pq.hpp"
#include "multiqueue/stick_policies.hpp"

#include "multiqueue/external/fastrange.h"
#include "multiqueue/external/xoroshiro256starstar.hpp"

#include <allocator>
#include <cassert>
#include <cstddef>
#include <functional>
#include <mutex>
#include <utility>

namespace multiqueue {

template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits, typename Allocator>
class MultiQueueImplBase {
    static_assert(std::is_same_v<Key, typename ValueTraits::key_type> &&
                      std::is_same_v<T, typename ValueTraits::mapped_type>,
                  "Key and T must be the same in ValueTraits");
    static_assert(std::is_same_v<Key, typename SentinelTraits::type>, "Key must be the same as type in SentinelTraits");

   public:
    using key_type = typename ValueTraits::key_type;
    using mapped_type = typename ValueTraits::mapped_type;
    using value_type = typename ValueTraits::value_type;
    using key_compare = KeyCompare;
    class value_compare {
        friend class MultiQueueImplBase;

       protected:
        [[no_unique_address]] key_compare comp;

        explicit value_compare(key_compare const &c) : comp{c} {
        }

       public:
        constexpr bool operator()(value_type const &lhs, value_type const &rhs) const noexcept {
            return comp(ValueTraits::key_of_value(lhs), ValueTraits::key_of_value(rhs));
        }
    };
    using reference = value_type &;
    using const_reference = value_type const &;
    using size_type = std::size_t;

    using allocator_type = Allocator;
    static_assert(std::is_same_v<value_type, typename Allocator::value_type>);

   protected:
    using inner_pq_type = PriorityQueue<typename ValueTraits::value_type, value_compare>;
    using pq_type = GuardedPQ<ValueTraits, SentinelTraits, inner_pq_type>;

    using pq_alloc_type = typename std::allocator_traits<allocator_type>::template rebind_alloc<pq_type>;
    using pq_alloc_traits = std::allocator_traits<pq_alloc_type>;

    pq_type *pq_list;
    size_type num_pqs;
    xoroshiro256starstar rng;
    [[no_unique_address]] pq_alloc_type alloc;
    [[no_unique_address]] key_compare comp;

    explicit MultiQueueImplBase(size_type n, std::uint64_t seed, key_compare const &c, allocator_type const &a)
        : num_pqs{n}, rng(seed), alloc{a}, comp{c} {
        assert(n > 0);

        pq_list = pq_alloc_traits::allocate(alloc, num_pqs);
#ifdef MULTIQUEUE_CHECK_ALIGNMENT
        if (reinterpret_cast<std::uintptr_t>(pq_list) % (GUARDED_PQ_ALIGNMENT) != 0) {
            std::abort();
        }
#endif
        for (pq_type *pq = pq_list; pq != pq_list + num_pqs; ++pq) {
            pq_alloc_traits::construct(alloc, pq, value_compare{comp});
        }
    }

    ~MultiQueueImplBase() noexcept {
        for (pq_type *pq = pq_list; pq != pq_list + num_pqs; ++pq) {
            pq_alloc_traits::destroy(alloc, s);
        }
        pq_alloc_traits::deallocate(alloc, pq_list, num_pqs);
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

   public:
    value_compare value_comp() const {
        return value_compare{comp};
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
};

template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits, StickPolicy policy, typename Allocator>
struct MultiQueueImpl;

template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits, typename Allocator>
struct MultiQueueImpl<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, StickPolicy::None, Allocator>
    : public MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, Allocator> {
    using base_type = MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, Allocator>;

    struct Config {
        std::uint64_t seed = 1;
        std::size_t c = 4;
    };

    class Handle {
        friend MultiQueueImpl;

        xoroshiro256starstar rng_;
        MultiQueueImpl &impl_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit Handle(MultiQueueImpl &impl, std::uint64_t seed) noexcept : rng_{seed}, impl_{impl} {
        }

        std::size_t random_index() noexcept {
            return fastrange64(rng(), impl_.num_pqs);
        }

       public:
        void push(typename base_type::const_reference value) noexcept {
            typename base_type::size_type index;
            do {
                index = random_index();
            } while (!impl_.pq_list[index].try_lock());
            impl_.pq_list[index].unsafe_push(value);
            impl_.pq_list[index].unlock();
        }

        bool try_pop(typename base_type::reference retval) noexcept {
            typename base_type::size_type index[2] = {random_index(), random_index()};
            typename base_type::key_type key[2] = {impl_.pq_list[index[0]].concurrent_top_key(),
                                                   impl_.pq_list[index[1]].concurrent_top_key()};
            std::size_t selected_index;
            do {
                unsigned int select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
                selected_index = index_[selected_pq];
                if (key[select_pq] == SentinelTraits::sentinel()) {
                    return false;
                }
                if (impl_.pq_list[select_index].try_lock(retval)) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        break;
                    }
                    retval = impl_.pq_list[select_index].unlock();
                }
                index[select_offset] = random_index();
                key[select_offset] = impl_.pq_list[index[selected_offset]].concurrent_top_key();
            } while (true);
            retval = impl_.pq_list[selected_index].unsafe_pop();
            retval = impl_.pq_list[selected_index].unlock();
            return true;
        }

        [[nodiscard]] bool is_empty(typename base_type::size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    MultiQueueImpl(unsigned int num_threads, Config const &config, typename base_type::key_compare const &comp,
                   typename base_type::allocator_type const &alloc)
        : base_type(num_threads * config.c, config.seed, comp, alloc) {
    }

    Handle get_handle() noexcept {
        static std::mutex m;
        auto l = std::unique_lock(m);
        auto seed = base_type::rng();
        l.unlock();
        return Handle{*this, seed};
    }
};

template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits, typename Allocator>
struct MultiQueueImpl<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, StickPolicy::Random, Allocator>
    : public MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, Allocator> {
    using base_type = MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, Allocator>;

    struct Config {
        std::uint64_t seed = 1;
        std::size_t c = 4;
        unsigned int stickiness;
    };

    unsigned int stickiness;

    class Handle {
        friend MultiQueueImpl;

        xoroshiro256starstar rng_;
        MultiQueueImpl &impl_;
        std::size_t index_[2];
        unsigned int use_count_[2];
        unsigned int push_pq_ = 0;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit Handle(MultiQueueImpl &impl, std::uint64_t seed) noexcept
            : rng_{seed},
              impl_{impl},
              index_{random_index(), random_index()},
              use_count_{impl_.stickiness, impl_.stickiness} {
        }

        std::size_t random_index() noexcept {
            return fastrange64(rng_(), impl_.num_pqs);
        }

       public:
        void push(typename base_type::const_reference value) noexcept {
            if (use_count_[push_pq_] == 0 || !impl_.pq_list[index_[push_pq_]].try_lock()) {
                do {
                    index_[push_pq_] = random_pq();
                } while (!impl_.pq_list[index_[push_pq_]].try_lock());
                use_count_[push_pq_] = impl_.stickiness;
            }
            impl_.pq_list[index_[push_pq_]].unsafe_push(value);
            impl_.pq_list[index_[push_pq_]].unlock();
            --use_count_[push_pq_];
            push_pq_ = 1 - push_pq_;
        }

        bool try_pop(typename base_type::reference retval) noexcept {
            if (use_count_[0] == 0) {
                index_[0] = random_index();
                use_count_[0] = impl_.stickiness;
            }
            if (use_count_[1] == 0) {
                index_[1] = random_index();
                use_count_[1] = impl_.stickiness;
            }

            typename base_type::key_type key[2] = {impl_.pq_list[index_[0]].concurrent_top_key(),
                                                   impl_.pq_list[index_[1]].concurrent_top_key()};
            std::size_t select_index;
            do {
                unsigned int select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
                select_index = index_[select_pq];
                if (key[select_pq] == SentinelTraits::sentinel()) {
                    // Both pqs are empty
                    use_count_[0] = 0;
                    use_count_[1] = 0;
                    return false;
                }
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        break;
                    }
                    impl_.pq_list[select_index].unlock();
                }
                index_[select_pq] = random_index();
                key[select_pq] = impl_.pq_list[index_[select_pq]].concurrent_top_key();
                use_count_[select_pq] = impl_.stickiness;
            } while (true);
            retval = impl_.pq_list[select_index].unsafe_top();
            impl_.pq_list[select_index].unsafe_pop();
            impl_.pq_list[select_index].unlock();
            --use_count[0];
            --use_count[1];
            return true;
        }

        [[nodiscard]] bool is_empty(typename base_type::size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    MultiQueueImpl(unsigned int num_threads, Config const &config, typename base_type::key_compare const &comp,
                   typename base_type::allocator_type const &alloc)
        : base_type(num_threads * config.c, config.seed, comp, alloc), stickiness{config.stickiness} {
    }

    Handle get_handle() noexcept {
        static std::mutex m;
        auto l = std::unique_lock(m);
        auto seed = base_type::rng();
        l.unlock();
        return Handle{*this, seed};
    }
};

template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits, typename Allocator>
struct MultiQueueImpl<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, StickPolicy::Swapping, Allocator>
    : public MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, Allocator> {
    using base_type = MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, Allocator>;

    struct Config : typename base_type::Config {
        std::uint64_t seed = 1;
        std::size_t c = 4;
        unsigned int stickiness;
    };

    struct alignas(L1_CACHE_LINESIZE) AlignedIndex {
        std::atomic_size_t i;
    };
    using Permutation = std::vector<AlignedIndex>;

    Permutation permutation;
    unsigned int stickiness;
    unsigned int handle_count = 0;

    class Handle {
        friend MultiQueueImpl;

        xoroshiro256starstar rng_;
        MultiQueueImpl &impl_;
        std::size_t permutation_index_[2];
        std::size_t index_[2];
        unsigned int use_count_[2];
        unsigned int push_pq_ = 0;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit Handle(MultiQueueImpl &impl, unsigned int id, std::uint64_t seed) noexcept
            : rng_{seed},
              impl_{impl},
              permutation_index_{id * 2, id * 2 + 1},
              index_{load_permutation(0), load_permutation(1)},
              use_count_{impl_.stickiness, impl_.stickiness} {
        }

        std::size_t load_index(unsigned int pq) {
            assert(pq <= 1);
            return permutation[permutation_index_[pq]].i.load(std::memory_order_relaxed);
        }

        std::size_t swap_assignment(unsigned int pq) {
            assert(pq <= 1);
            std::size_t source_assigned =
                permutation[permutation_index_[pq]].i.exchange(impl_.num_pqs, std::memory_order_relaxed);
            // Only handle itself may invalidate
            assert(source_assigned != impl_.num_pqs);
            std::size_t target_index;
            std::size_t target_assigned;
            do {
                target_index = random_index();
                target_assigned = permutation[target_index].i.load(std::memory_order_relaxed);
            } while (target_assigned == impl_.num_pqs ||
                     !permutation[target_index].i.compare_exchange_strong(target_assigned, source_assigned,
                                                                          std::memory_order_relaxed));
            permutation[permutation_index_[pq]].i.store(target_assigned, std::memory_order_relaxed);
            return target_assigned;
        }

        void lock_push_pq() noexcept {
            if (use_count_[push_pq_] != 0) {
                auto current_index = permutation[permutation_index_[push_pq_]].i.load(std::memory_order_relaxed);
            }
            do {
                swap_assignment(permutation_index_ + push_pq);
                push_index = permutation[permutation_index_ + push_pq].i.load(std::memory_order_relaxed);
            } while (!impl_.pq_list[push_index].try_lock());
            use_count_[push_pq] = impl_.stickiness;
            if (impl_.pq_list[index_[push_pq_]].try_lock()) {
                if (current_index != index_[push_pq_]) {
                    index_[push_pq_] = current_index;
                }
                return;
            }
        }

       public:
        void push(typename base_type::const_reference value) noexcept {
            lock_push_pq();
            impl_.pq_list[push_index].unsafe_push(value);
            impl_.pq_list[push_index].unlock();
            --use_count_[push_pq_];
            push_pq_ = 1 - push_pq_;
        }

        bool try_pop(typename base_type::reference retval) noexcept {
            if (use_count_[0] == 0) {
                swap_assignment(permutation_index_);
                use_count_[0] = impl_.stickiness;
            }
            if (use_count_[1] == 0) {
                swap_assignment(permutation_index_ + 1);
                use_count_[1] = impl_.stickiness;
            }
            std::size_t index[2]{permutation[permutation_index_].i.load(std::memory_order_relaxed),
                                 permutation[permutation_index_ + 1].i.load(std::memory_order_relaxed)};
            typename base_type::key_type key[2] = {impl_.pq_list[index[0]].concurrent_top_key(),
                                                   impl_.pq_list[index[1]].concurrent_top_key()};
            std::size_t select_index;
            do {
                unsigned int select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
                select_index = index[select_pq];
                if (key[select_pq] == SentinelTraits::sentinel()) {
                    use_count_[0] = 0;
                    use_count_[1] = 0;
                    return false;
                }
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].empty()) {
                        break;
                    }
                    impl_.pq_list[select_index].unlock();
                }
                swap_assignment(permutation_index_ + select_pq);
                index[select_pq] = permutation[permutation_index_ + select_pq].i.load(std::memory_order_relaxed);
                key[select_pq] = impl_.pq_list[index[selec_pq]].concurrent_top_key();
                use_count_[select_pq] = impl_.stickiness;
            } while (true);
            retval = impl_.pq_list[select_index].unsafe_top();
            impl_.pq_list[select_index].unsafe_pop();
            impl_.pq_list[select_index].unlock();
            --use_count_[0];
            --use_count_[1];
            return true;
        }

        [[nodiscard]] bool is_empty(typename base_type::size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    MultiQueueImpl(unsigned int num_threads, Config const &config, typename base_type::key_compare const &comp,
                   typename base_type::allocator_type const &alloc)
        : base_type(num_threads * config.c, config.seed, comp, alloc),
          permutation(num_threads * config.c),
          stickiness{config.stickiness} {
        for (std::size_t i = 0; i < num_pqs; ++i) {
            permutation[i].i = i;
        }
    }

    Handle get_handle() noexcept {
        static std::mutex m;
        auto l = std::unique_lock(m);
        auto seed = base_type::rng();
        auto id = handle_count++;
        l.unlock();
        return Handle{*this, id, seed};
    }
};

// This variant uses a global permutation defined by the parameters a and b, such that i*a + b mod p yields a
// number from [0,p-1] for i in [0,p-1] For this to be a permutation, a and b needs to be coprime. Each handle
// has a unique id, so that i in [3*id,3*id+2] identify the queues associated with this handle. The stickiness
// counter is global and can occasionally
template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits, typename Allocator>
struct MultiQueueImpl<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, StickPolicy::Permutation>
    : public MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, Allocator> {
    using base_type = MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, Allocator>;

    struct Config : typename base_type::Config {
        std::uint64_t seed = 1;
        std::size_t c = 4;
        unsigned int stickiness;
    };

    static constexpr std::uint64_t Mask = 0xffffffff;

    unsigned int stickiness;
    alignas(L1_CACHE_LINESIZE) std::atomic_uint64_t permutation;

    template <typename Generator>
    std::uint64_t update_permutation(std::uint64_t old, Generator &g) {
        std::uint64_t new_permutation = g() | 1;
        return permutation.compare_exchange_strong(old, new_permutation, std::memory_order_relaxed) ? new_permutation
                                                                                                    : old;
    }

    class Handle {
        friend MultiQueueImpl;

        xoroshiro256starstar rng_;
        MultiQueueImpl &impl_;

        std::size_t index_[2];
        std::uint64_t current_permutatiton_;
        unsigned int push_pq_;
        unsigned int use_count_;
        unsigned int permutation_index_;

        void refresh_permutation() noexcept {
            if (auto permutation = impl_.perm.load(std::memory_order_relaxed); permutation == current_permutation_) {
                if (use_count_ != 0) {
                    return;
                }
                current_permutation_ = impl_.update_permutation(rng_);
            } else {
                current_permutation_ = permutation;
            }
            std::size_t a = current_permutation_ & Mask;
            std::size_t b = current_permutation_ >> 32;
            assert(a & 1 == 1);
            index_[0] = (permutation_index_ * a + b) & (impl_.num_pqs - 1);
            index_[1] = ((permutation_index_ + 1) * a + b) & (impl_.num_pqs - 1);
            use_count_ = impl_.stickiness;
        }

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit Handle(MultiQueueImpl &impl, unsigned int id, std::uint64_t seed) noexcept
            : rng_{seed}, impl_{impl}, current_permutation_{0x1}, permutation_index_{id * 2} {
        }

       public:
        void push(typename base_type::const_reference value) noexcept {
            std::size_t index = push_index_;
            while (!impl_.pq_list[index].lock_push(value)) {
                // Fallback to random queue but still count as use
                index = random_index();
            }
            --push_use_count_;
            if (push_use_count_ == 0) {
                perm_cache_ = 0;
                impl_.update_permutation(current_perm, rng_);
            }
        }

        bool try_pop(typename base_type::reference retval) noexcept {
            auto current_perm = impl_.perm.load(std::memory_order_relaxed);
            if (current_perm != perm_cache_) {
                update_indices(current_perm);
            }
            std::size_t index[2] = {pop_index_[0], pop_index_[1]};
            typename base_type::key_type key[2] = {impl_.pq_list[index[0]].concurrent_top_key(),
                                                   impl_.pq_list[index[1]].concurrent_top_key()};
            do {
                bool best = impl_.compare(key[0], key[1]);
                if (key[best] == SentinelTraits::sentinel()) {
                    perm_cache_ = 0;
                    impl_.update_permutation(current_perm, rng_);
                    return false;
                }
                if (impl_.pq_list[index[best]].lock_pop(retval)) {
                    --pop_use_count_;
                    if (pop_use_count_ == 0) {
                        perm_cache_ = 0;
                        impl_.update_permutation(current_perm, rng_);
                    }
                    return true;
                }
                index[best] = random_index();
                key[best] = impl_.pq_list[index[best]].concurrent_top_key();
            } while (true);
        }

        [[nodiscard]] bool is_empty(typename base_type::size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    MultiQueueImpl(unsigned int num_threads, Config const &config, typename base_type::key_compare const &comp,
                   typename base_type::allocator_type const &alloc)
        : base_type(num_threads * config.c, config.seed, comp, alloc), stickiness{config.stickiness} {
    }

    Handle get_handle() noexcept {
        static std::mutex m;
        auto l = std::unique_lock(m);
        auto seed = base_type::rng();
        auto id = handle_count++;
        l.unlock();
        return Handle{*this, id, seed};
    }
};

}  // namespace multiqueue
#endif
