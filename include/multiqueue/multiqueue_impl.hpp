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

#include "multiqueue/stick_policies.hpp"

#include "multiqueue/external/fastrange.h"
#include "multiqueue/external/xoroshiro256starstar.hpp"

#include <cstddef>
#include <mutex>
#include <utility>

namespace multiqueue {

template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits>
struct MultiQueueImplBase {
    static_assert(std::is_same_v<Key, typename ValueTraits::key_type> &&
                      std::is_same_v<T, typename ValueTraits::mapped_type>,
                  "Key and T must be the same in ValueTraits");
    static_assert(std::is_same_v<Key, typename SentinelTraits::type>, "Key must be the same as type in SentinelTraits");

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
    using inner_pq_type = PriorityQueue<typename ValueTraits::value_type, value_compare>;
    using pq_type = GuardedPQ<ValueTraits, SentinelTraits, inner_pq_type>;

    struct Config {
        std::uint64_t seed = 1;
        std::size_t c = 4;
    };

    pq_type *pq_list;
    size_type num_pqs;
    xoroshiro256starstar rng;
    [[no_unique_address]] key_compare comp;

    value_compare value_comp() const {
        return value_compare{comp};
    }

    static constexpr key_type sentinel() noexcept {
        return SentinelTraits::sentinel();
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
};

template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits, StickPolicy policy>
struct MultiQueueImpl;

template <typename Key, typename T, typename KeyCompare, template <typename, typename> typename PriorityQueue,
          typename ValueTraits, typename SentinelTraits>
struct MultiQueueImpl<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, StickPolicy::None>
    : public MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits> {
    using base_type = MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits>;
    using Config = typename base_type::Config;

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

       public:
        void push(typename base_type::const_reference value) noexcept {
            typename base_type::size_type index;
            do {
                index = fastrange64(rng_(), impl_.num_pqs);
            } while (!impl_.pq_list[index].try_lock());
            impl_.pq_list[index].unsafe_push(value);
            impl_.pq_list[index].unlock();
        }

        bool try_pop(typename base_type::reference retval) noexcept {
            typename base_type::size_type index[2] = {fastrange64(rng_(), impl_.num_pqs),
                                                      fastrange64(rng_(), impl_.num_pqs)};
            typename base_type::key_type key[2] = {impl_.pq_list[index[0]].concurrent_top_key(),
                                                   impl_.pq_list[index[1]].concurrent_top_key()};
            std::size_t selected_index;
            do {
                unsigned int select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
                selected_index = index_[selected_pq];
                if (key[select_pq] == base_type::sentinel()) {
                    return false;
                }
                if (impl_.pq_list[select_index].try_lock(retval)) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        break;
                    }
                    retval = impl_.pq_list[select_index].unlock();
                }
                index[select_offset] = fastrange64(rng_(), impl_.num_pqs);
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

    MultiQueueImpl(std::size_t /* num_pqs */, Config const &) noexcept {
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
          typename ValueTraits, typename SentinelTraits>
struct MultiQueueImpl<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, StickPolicy::Random>
    : public MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits> {
    using base_type = MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits>;
    struct Config : typename base_type::Config {
        unsigned int stickiness;
    };

    unsigned int stickiness;

    class Handle {
        friend MultiQueueImpl;

        xoroshiro256starstar rng_;
        MultiQueueImpl &impl_;
        std::size_t index_[2];
        unsigned int use_count_[2];

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit Handle(MultiQueueImpl &impl, std::uint64_t seed) noexcept : rng_{seed}, impl_{impl} {
            index_[0] = fastrange64(rng_(), impl_.num_pqs);
            index_[1] = fastrange64(rng_(), impl_.num_pqs);
            use_count_[0] = impl_.stickiness;
            use_count_[1] = impl_.stickiness;
        }

       public:
        void push(typename base_type::const_reference value) noexcept {
            unsigned int push_pq = use_count_[1] < use_count_[0] ? 0 : 1;
            std::size_t push_index = index_[push_pq];
            if (use_count_[push_pq] == 0 || !impl_.pq_list[push_index].try_lock()) {
                do {
                    push_index = fastrange64(rng_(), impl_.num_pqs);
                } while (!impl_.pq_list[push_index].try_lock());
                use_count_[push_pq] = impl_.stickiness;
                index_[push_pq] = push_index;
            }
            impl_.pq_list[push_index].unsafe_push(value);
            impl_.pq_list[push_index].unlock();
            --use_count_[push_pq];
        }

        bool try_pop(typename base_type::reference retval) noexcept {
            if (use_count_[0] == 0) {
                index_[0] = fastrange64(rng_(), impl_.num_pqs);
                use_count_[0] = impl_.stickiness;
            }
            if (use_count_[1] == 0) {
                index_[1] = fastrange64(rng_(), impl_.num_pqs);
                use_count_[1] = impl_.stickiness;
            }

            typename base_type::key_type key[2] = {impl_.pq_list[index_[0]].concurrent_top_key(),
                                                   impl_.pq_list[index_[1]].concurrent_top_key()};
            std::size_t select_index;
            do {
                unsigned int select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
                select_index = index_[select_pq];
                if (key[select_pq] == base_type::sentinel()) {
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
                index_[select_pq] = fastrange64(rng_(), impl_.num_pqs);
                use_count_[select_pq] = impl_.stickiness;
                key[select_pq] = impl_.pq_list[index_[select_pq]].concurrent_top_key();
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

    MultiQueueImpl(std::size_t /* num_pqs */, Config const &config) noexcept : stickiness{config.stickiness} {
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
          typename ValueTraits, typename SentinelTraits>
struct MultiQueueImpl<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, StickPolicy::Swapping>
    : public MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits> {
    using base_type = MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits>;
    struct Config : typename base_type::Config {
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
        unsigned int use_count_[2];
        unsigned int base_index_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit Handle(MultiQueueImpl &impl, unsigned int id, std::uint64_t seed) noexcept
            : rng_{seed}, impl_{impl}, use_count_{impl_.stickiness, impl_.stickiness}, base_index_{id * 2} {
        }

        void swap_assignment(std::size_t index) {
            std::size_t source_assigned = permutation[index].i.exchange(impl_.num_pqs, std::memory_order_relaxed);
            // Only handle itself may invalidate
            assert(source_assigned != impl_.num_pqs);
            std::size_t target_index;
            std::size_t target_assigned;
            do {
                target_index = fastrange64(rng_(), impl_.num_pqs);
                target_assigned = permutation[target_index].i.load(std::memory_order_relaxed);
            } while (target_assigned == impl_.num_pqs ||
                     !permutation[target_index].i.compare_exchange_strong(target_assigned, source_assigned,
                                                                          std::memory_order_relaxed));
            permutation[index].i.store(target_assigned, std::memory_order_relaxed);
        }

       public:
        void push(typename base_type::const_reference value) noexcept {
            unsigned int push_pq = use_count_[1] < use_count_[0] ? 0 : 1;
            if (use_count_[push_pq] == 0) {
                swap_assignment(base_index_ + push_pq);




                push_use_count_ = impl_.stickiness;
            }
            if (use_count_[push_pq] == 0 || !impl_.pq_list[push_index].try_lock()) {
                do {
                    index = swap_assignment(push_index);
                } while (!impl_.pq_list[index].lock_push(value));
                use_count_ = impl_.stickiness;
            }
            std::size_t push_index = permutation[base_index_ + push_pq].i.load(std::memory_order_relaxed);
            --push_use_count_;
            if (push_use_count_ == 0) {
                swap_assignment(perm_base_index_ + 2);
                push_use_count_ = impl_.stickiness;
            }
        }

        bool try_pop(typename base_type::reference retval) noexcept {
            std::size_t index[2]{permutation[perm_base_index_].i.load(std::memory_order_relaxed),
                                 permutation[perm_base_index_ + 1].i.load(std::memory_order_relaxed)};
            typename base_type::key_type key[2] = {impl_.pq_list[index[0]].concurrent_top_key(),
                                                   impl_.pq_list[index[1]].concurrent_top_key()};
            do {
                bool best = impl_.compare(key[0], key[1]);
                if (key[best] == base_type::sentinel()) {
                    swap_assignment(perm_base_index_);
                    pop_use_count_[0] = impl_.stickiness;
                    swap_assignment(perm_base_index_ + 1);
                    pop_use_count_[1] = impl_.stickiness;
                    return false;
                }
                if (impl_.pq_list[index[best]].lock_pop(retval)) {
                    --pop_use_count_[0];
                    if (pop_use_count_[0] == 0) {
                        swap_assignment(perm_base_index_);
                        pop_use_count_[0] = impl_.stickiness;
                    }
                    --pop_use_count_[1];
                    if (pop_use_count_[1] == 0) {
                        swap_assignment(perm_base_index_ + 1);
                        pop_use_count_[1] = impl_.stickiness;
                    }
                    return true;
                }
                index[best] = swap_assignment(perm_base_index_ + best);
                key[best] = impl_.pq_list[index[best]].concurrent_top_key();
                pop_use_count_[best] = impl_.stickiness;
            } while (true);
        }

        [[nodiscard]] bool is_empty(typename base_type::size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    MultiQueueImpl(std::size_t num_pqs, Config const &config) noexcept
        : permutation(num_pqs), stickiness{config.stickiness} {
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
          typename ValueTraits, typename SentinelTraits>
struct MultiQueueImpl<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, StickPolicy::Permutation>
    : public MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits> {
    using base_type = MultiQueueImplBase<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits>;
    struct Config : typename base_type::Config {
        unsigned int stickiness;
    };

    static constexpr std::uint64_t PermutationMask = ((std::uint64_t{1} << 32) - 1);

    unsigned int stickiness;
    unsigned int num_threads;
    std::vector<std::uint64_t> valid_factors;
    alignas(L1_CACHE_LINESIZE) std::atomic_uint64_t perm;
    alignas(L1_CACHE_LINESIZE) std::atomic_uint stickiness_count;

    template <typename Generator>
    void update_permutation(std::uint64_t old, Generator &g) {
        std::uint64_t a = valid_factors[fastrange64(g(), valid_factors.size())];
        std::uint64_t b = g() & PermutationMask;
        perm.compare_exchange_strong(old, (a << 32) | b, std::memory_order_relaxed);
    }

    class Handle {
        friend MultiQueueImpl;

        xoroshiro256starstar rng_;
        std::size_t push_index_;
        unsigned int push_use_count_;
        std::size_t pop_index_[2];
        unsigned int pop_use_count_;
        std::uint64_t perm_cache_ = 0;
        unsigned int perm_base_index_;

        MultiQueueImpl &impl_;

        void update_indices(std::uint64_t new_perm) noexcept {
            push_index_ = ((perm_base_index_ + 2) * (new_perm >> 32) + (new_perm & PermutationMask)) % impl_.num_pqs;
            push_use_count_ = impl_.stickiness;
            pop_index_[0] = ((perm_base_index_) * (new_perm >> 32) + (new_perm & PermutationMask)) % impl_.num_pqs;
            pop_index_[1] = ((perm_base_index_ + 1) * (new_perm >> 32) + (new_perm & PermutationMask)) % impl_.num_pqs;
            pop_use_count_ = impl_.stickiness;
            perm_cache_ = new_perm;
        }

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit Handle(MultiQueueImpl &impl, unsigned int id, std::uint64_t seed) noexcept
            : rng_{seed}, perm_base_index_{id * 3}, impl_{impl} {
        }

       public:
        void push(typename base_type::const_reference value) noexcept {
            auto current_perm = impl_.perm.load(std::memory_order_relaxed);
            if (current_perm != perm_cache_) {
                update_indices(current_perm);
            }
            std::size_t index = push_index_;
            while (!impl_.pq_list[index].lock_push(value)) {
                // Fallback to random queue but still count as use
                index = fastrange64(rng_(), impl_.num_pqs);
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
                if (key[best] == base_type::sentinel()) {
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
                index[best] = fastrange64(rng_(), impl_.num_pqs);
                key[best] = impl_.pq_list[index[best]].concurrent_top_key();
            } while (true);
        }

        [[nodiscard]] bool is_empty(typename base_type::size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    MultiQueueImpl(std::size_t num_pqs, Config const &config) noexcept : stickiness{config.stickiness} {
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
