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

#include "multiqueue/stick_policy.hpp"

#include "multiqueue/external/fastrange.h"
#include "multiqueue/external/xoroshiro256starstar.hpp"

#include <allocator>
#include <cassert>
#include <cstddef>
#include <functional>
#include <mutex>
#include <utility>

namespace multiqueue {

template <typename PriorityQueue, typename KeyCompare, typename ValueTraits, typename SentinelTraits>
struct MultiQueueImplBase {
    using pq_type = PriorityQueue;
    using key_type = typename pq_type::key_type;
    using value_type = typename pq_type::value_type;
    using key_compare = KeyCompare;
    using value_compare = typename pq_type::value_compare;
    using size_type = std::size_t;
    using reference = value_type &;
    using const_reference = value_type const &;

    pq_type *pq_list;
    size_type num_pqs;
    xoroshiro256starstar rng;
    [[no_unique_address]] key_compare comp;

    explicit MultiQueueImplBase(size_type n, std::uint64_t seed, key_compare const &c)
        : num_pqs{n}, rng(seed), comp{c} {
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

template <typename PriorityQueue, typename KeyCompare, StickPolicy P, typename ValueTraits, typename SentinelTraits>
struct MultiQueueImpl;

template <typename PriorityQueue, typename KeyCompare, typename ValueTraits, typename SentinelTraits>
struct MultiQueueImpl<PriorityQueue, KeyCompare, StickPolicy::None, ValueTraits, SentinelTraits>
    : public MultiQueueImplBase<PriorityQueue, KeyCompare, ValueTraits, SentinelTraits> {
    using base_type = MultiQueueImplBase<PriorityQueue, KeyCompare, ValueTraits, SentinelTraits>;

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

    using config_type = Config;
    using handle_type = Handle;

    MultiQueueImpl(unsigned int num_threads, config_type const &config, typename base_type::key_compare const &comp)
        : base_type(num_threads * config.c, config.seed, comp) {
    }

    handle_type get_handle() noexcept {
        static std::mutex m;
        auto l = std::unique_lock(m);
        auto seed = base_type::rng();
        l.unlock();
        return handle_type{*this, seed};
    }
};

template <typename PriorityQueue, typename KeyCompare, typename ValueTraits, typename SentinelTraits>
struct MultiQueueImpl<PriorityQueue, KeyCompare, StickPolicy::Random, ValueTraits, SentinelTraits>
    : public MultiQueueImplBase<PriorityQueue, KeyCompare, ValueTraits, SentinelTraits> {
    using base_type = MultiQueueImplBase<PriorityQueue, KeyCompare, ValueTraits, SentinelTraits>;

    struct Config {
        std::uint64_t seed = 1;
        std::size_t c = 4;
        unsigned int stickiness;
    };

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

    using config_type = Config;
    using handle_type = Handle;

    unsigned int stickiness;

    MultiQueueImpl(unsigned int num_threads, config_type const &config, typename base_type::key_compare const &comp)
        : base_type(num_threads * config.c, config.seed, comp), stickiness{config.stickiness} {
    }

    handle_type get_handle() noexcept {
        static std::mutex m;
        auto l = std::unique_lock(m);
        auto seed = base_type::rng();
        l.unlock();
        return handle_type{*this, seed};
    }
};

template <typename PriorityQueue, typename KeyCompare, typename ValueTraits, typename SentinelTraits>
struct MultiQueueImpl<PriorityQueue, KeyCompare, StickPolicy::Swapping, ValueTraits, SentinelTraits>
    : public MultiQueueImplBase<PriorityQueue, KeyCompare, ValueTraits, SentinelTraits> {
    using base_type = MultiQueueImplBase<PriorityQueue, KeyCompare, ValueTraits, SentinelTraits>;

    struct Config : typename base_type::Config {
        std::uint64_t seed = 1;
        std::size_t c = 4;
        unsigned int stickiness;
    };

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
              index_{load_index(0), load_index(1)},
              use_count_{impl_.stickiness, impl_.stickiness} {
        }

        std::size_t load_index(unsigned int pq) const noexcept {
            assert(pq <= 1);
            return permutation[permutation_index_[pq]].i.load(std::memory_order_relaxed);
        }

        void swap_assignment(unsigned int pq) noexcept {
            assert(pq <= 1);
            if (!permutation[permutation_index_[pq]].i.compare_exchange_strong(index_[pq], impl_.num_pqs,
                                                                               std::memory_order_relaxed)) {
                // Permutation has changed, no need to swap
                // Only handle itself may invalidate
                assert(index_[pq] != impl_.num_pqs);
                return;
            }
            std::size_t target_index;
            std::size_t target_assigned;
            do {
                target_index = random_index();
                target_assigned = permutation[target_index].i.load(std::memory_order_relaxed);
            } while (target_assigned == impl_.num_pqs ||
                     !permutation[target_index].i.compare_exchange_strong(target_assigned, index_[pq],
                                                                          std::memory_order_relaxed));
            permutation[permutation_index_[pq]].i.store(target_assigned, std::memory_order_relaxed);
            index_[pq] = target_assigned;
        }

        void refresh_pq(unsigned int pq) noexcept {
            if (use_count_[pq] != 0) {
                auto current_index = load_index(pq);
                if (current_index != index_[pq]) {
                    index_[pq] = current_index;
                    use_count_[pq] = impl_.stickiness;
                }
            } else {
                swap_assignment(pq);
                use_count_[pq] = impl_.stickiness;
            }
        }

       public:
        void push(typename base_type::const_reference value) {
            refresh_pq(push_pq);
            if (!impl_.pq_list[index_[push_pq]].try_lock()) {
                do {
                    swap_assignment(push_pq);
                } while (!impl_.pq_list[index_[push_pq]].try_lock());
                use_count_[push_pq] = impl_.stickiness;
            }
            impl_.pq_list[index_[push_pq]].unsafe_push(value);
            impl_.pq_list[index_[push_pq]].unlock();
            --use_count_[push_pq_];
            push_pq_ = 1 - push_pq_;
        }

        bool try_pop(typename base_type::reference retval) {
            refresh_pq(0);
            refresh_pq(1);
            typename base_type::key_type key[2] = {load_index(0), load_index(1)};
            std::size_t select_index;
            do {
                unsigned int select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
                select_index = index_[select_pq];
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
                swap_assignment(select_pq);
                use_count_[select_pq] = impl_.stickiness;
                key[select_pq] = load_index(select_pq);
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

    using config_type = Config;
    using handle_type = Handle;

    struct alignas(L1_CACHE_LINESIZE) AlignedIndex {
        std::atomic_size_t i;
    };

    using Permutation = std::vector<AlignedIndex>;

    Permutation permutation;
    unsigned int stickiness;
    unsigned int handle_count = 0;

    MultiQueueImpl(unsigned int num_threads, config_type const &config, typename base_type::key_compare const &comp)
        : base_type(num_threads * config.c, config.seed, comp),
          permutation(num_threads * config.c),
          stickiness{config.stickiness} {
        for (std::size_t i = 0; i < num_pqs; ++i) {
            permutation[i].i = i;
        }
    }

    handle_type get_handle() noexcept {
        static std::mutex m;
        auto l = std::unique_lock(m);
        auto seed = base_type::rng();
        auto id = handle_count++;
        l.unlock();
        return handle_type{*this, id, seed};
    }
};

// This variant uses a global permutation defined by the parameters a and b, such that i*a + b mod p yields a
// number from [0,p-1] for i in [0,p-1] For this to be a permutation, a and b needs to be coprime. Each handle
// has a unique id, so that i in [3*id,3*id+2] identify the queues associated with this handle. The stickiness
// counter is global and can occasionally
template <typename PriorityQueue, typename KeyCompare, typename ValueTraits, typename SentinelTraits>
struct MultiQueueImpl<PriorityQueue, KeyCompare, StickPolicy::Permutation, ValueTraits, SentinelTraits>
    : public MultiQueueImplBase<PriorityQueue, KeyCompare, ValueTraits, SentinelTraits> {
    using base_type = MultiQueueImplBase<PriorityQueue, KeyCompare, ValueTraits, SentinelTraits>;

    struct Config : typename base_type::Config {
        std::uint64_t seed = 1;
        std::size_t c = 4;
        unsigned int stickiness;
    };

    class Handle {
        friend MultiQueueImpl;

        xoroshiro256starstar rng_;
        MultiQueueImpl &impl_;

        std::size_t permutation_index_[2];
        std::uint64_t current_permutatiton_;
        std::size_t backup_index_[2];
        unsigned int use_count_;
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
              current_permutation_{impl_.permutation.load(std::memory_order_relaxed)},
              backup_index_{get_index(0), get_index(1)},
              use_count_{impl_.stickiness} {
        }

        void refresh_permutation() noexcept {
            if (use_count_ != 0) {
                auto permutation = impl_.perm.load(std::memory_order_relaxed);
                if (permutation != current_permutation_) {
                    current_permutation_ = permutation;
                    backup_index_[0] = get_index(0);
                    backup_index_[1] = get_index(1);
                    use_count_ = impl_.stickiness;
                }
            } else {
                impl_.update_permutation(current_permutation_);
                backup_index_[0] = get_index(0);
                backup_index_[1] = get_index(1);
                use_count_ = impl_.stickiness;
            }
        }

        std::size_t get_index(unsigned int pq) const noexcept {
            std::size_t a = current_permutation_ & Mask;
            std::size_t b = current_permutation_ >> 32;
            assert(a & 1 == 1);
            return (permutation_index_[pq] * a + b) & (impl_.num_pqs - 1);
        }

       public:
        void push(typename base_type::const_reference value) noexcept {
            refresh_permutation();
            std::size_t index = get_index(push_pq);
            if (!impl_.pq_list[index].try_lock()) {
                if (backup_index_[push_pq] != index || !impl_.pq_list[backup_index_[push_pq]].try_lock()) {
                    do {
                        backup_index_[push_pq] = random_index();
                    } while (!impl_.pq_list[backup_index_[push_pq]].try_lock());
                    index = backup_index_[push_pq];
                }
            }
            impl_.pq_list[index].unsafe_push(value);
            impl_.pq_list[index].unlock();
            --use_count_;
            push_pq_ = 1 - push_pq_;
        }

        bool try_pop(typename base_type::reference retval) noexcept {
            refresh_permutation();
            typename base_type::size_type index[2] = {get_index(0), get_index(1)};
            typename base_type::key_type key[2] = {impl_.pq_list[index[0]].concurrent_top_key(),
                                                   impl_.pq_list[index[1]].concurrent_top_key()};
            std::size_t select_index;
            do {
                unsigned int select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
                select_index = index[select_pq];
                if (key[select_pq] == SentinelTraits::sentinel()) {
                    use_count_ = 0;
                    return false;
                }
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].empty()) {
                        if (select_index != get_index(select_pq)) {
                            backup_index_[select_pq] = select_index;
                        }
                        break;
                    }
                    impl_.pq_list[select_index].unlock();
                }
                if (index[select_index] == backup_index_[select_index]) {
                    index[select_index] = random_index();
                } else {
                    index[select_index] = backup_index_[select_index];
                    backup_index_[select_index] = random_index();
                }
                key[select_index] = impl_.pq_list[index[select_index]].concurrent_top_key();
            } while (true);
            retval = impl_.pq_list[select_index].unsafe_top();
            impl_.pq_list[select_index].unsafe_pop();
            impl_.pq_list[select_index].unlock();
            --use_count_;
            return true;
        }

        [[nodiscard]] bool is_empty(typename base_type::size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using config_type = Config;
    using handle_type = Handle;

    static constexpr std::uint64_t Mask = 0xffffffff;

    unsigned int stickiness;
    alignas(L1_CACHE_LINESIZE) std::atomic_uint64_t permutation;

    static std::size_t next_power_of_two(std::size_t n) {
        return std::size_t{1} << static_cast<unsigned int>(std::ceil(std::log2(n)));
    }

    MultiQueueImpl(unsigned int num_threads, config_type const &config, typename base_type::key_compare const &comp)
        : base_type(next_power_of_two(num_threads * config.c), config.seed, comp),
          stickiness{config.stickiness},
          permutation{1} {
    }

    template <typename Generator>
    void update_permutation(std::uint64_t &old, Generator &g) {
        std::uint64_t new_permutation = g() | 1;
        if (permutation.compare_exchange_strong(old, new_permutation, std::memory_order_relaxed)) {
            old = new_permutation;
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

}  // namespace multiqueue
#endif
