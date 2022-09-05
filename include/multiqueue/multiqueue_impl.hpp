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
#include "multiqueue/stick_policy.hpp"

#include "multiqueue/external/fastrange.h"
#include "multiqueue/external/xoroshiro256starstar.hpp"

#include <cmath>
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

namespace multiqueue {

template <typename PriorityQueue, typename KeyCompare>
struct MultiQueueImplBase {
    using pq_type = PriorityQueue;
    using key_type = typename pq_type::key_type;
    using value_type = typename pq_type::value_type;
    using key_compare = KeyCompare;
    using value_compare = typename pq_type::value_compare;
    using size_type = std::size_t;
    using reference = value_type &;
    using const_reference = value_type const &;
    using sentinel_traits = typename pq_type::sentinel_traits;

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
        if constexpr (!sentinel_traits::is_implicit) {
            if (rhs == sentinel_traits::sentinel()) {
                return false;
            }
            if (lhs == sentinel_traits::sentinel()) {
                return true;
            }
        }
        return comp(lhs, rhs);
    }

    value_compare value_comp() const {
        return value_compare{comp};
    }
};

template <typename PriorityQueue, typename KeyCompare, StickPolicy P>
struct MultiQueueImpl;

template <typename PriorityQueue, typename KeyCompare>
struct MultiQueueImpl<PriorityQueue, KeyCompare, StickPolicy::None>
    : public MultiQueueImplBase<PriorityQueue, KeyCompare> {
    using base_type = MultiQueueImplBase<PriorityQueue, KeyCompare>;

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
            return fastrange64(rng_(), impl_.num_pqs);
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
            do {
                typename base_type::size_type index[2] = {random_index(), random_index()};
                typename base_type::key_type key[2] = {impl_.pq_list[index[0]].concurrent_top_key(),
                                                       impl_.pq_list[index[1]].concurrent_top_key()};
                unsigned int select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
                if (key[select_pq] == base_type::sentinel_traits::sentinel()) {
                    return false;
                }
                std::size_t select_index = index[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
            } while (true);
        }

        [[nodiscard]] bool is_empty(typename base_type::size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using config_type = Config;
    using handle_type = Handle;

    MultiQueueImpl(unsigned int num_threads, config_type const &config, typename base_type::key_compare const &c)
        : base_type(num_threads * config.c, config.seed, c) {
    }

    handle_type get_handle() noexcept {
        static std::mutex m;
        auto l = std::unique_lock(m);
        auto seed = base_type::rng();
        l.unlock();
        return handle_type{*this, seed};
    }
};

template <typename PriorityQueue, typename KeyCompare>
struct MultiQueueImpl<PriorityQueue, KeyCompare, StickPolicy::RandomStrict>
    : public MultiQueueImplBase<PriorityQueue, KeyCompare> {
    using base_type = MultiQueueImplBase<PriorityQueue, KeyCompare>;

    struct Config {
        std::uint64_t seed = 1;
        std::size_t c = 4;
        unsigned int stickiness;
    };

    class Handle {
        friend MultiQueueImpl;

        xoroshiro256starstar rng_;
        MultiQueueImpl &impl_;
        std::size_t stick_index_[2];
        unsigned int use_count_;
        unsigned int push_pq_ = 0;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit Handle(MultiQueueImpl &impl, std::uint64_t seed) noexcept
            : rng_{seed}, impl_{impl}, stick_index_{random_index(), random_index()}, use_count_{impl_.stickiness} {
        }

        std::size_t random_index() noexcept {
            return fastrange64(rng_(), impl_.num_pqs);
        }

       public:
        void push(typename base_type::const_reference value) noexcept {
            if (use_count_ == 0 || !impl_.pq_list[stick_index_[push_pq_]].try_lock()) {
                do {
                    stick_index_[0] = random_index();
                } while (!impl_.pq_list[stick_index_[0]].try_lock());
                stick_index_[1] = random_index();
                use_count_ = impl_.stickiness;
                push_pq_ = 0;
            }
            impl_.pq_list[stick_index_[push_pq_]].unsafe_push(value);
            impl_.pq_list[stick_index_[push_pq_]].unlock();
            assert(use_count_ > 0);
            --use_count_;
            push_pq_ = 1 - push_pq_;
        }

        bool try_pop(typename base_type::reference retval) noexcept {
            if (use_count_ == 0) {
                stick_index_[0] = random_index();
                stick_index_[1] = random_index();
                use_count_ = impl_.stickiness;
            }
            assert(use_count_ > 0);

            do {
                typename base_type::key_type key[2] = {impl_.pq_list[stick_index_[0]].concurrent_top_key(),
                                                       impl_.pq_list[stick_index_[1]].concurrent_top_key()};
                unsigned int select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
                if (key[select_pq] == base_type::sentinel_traits::sentinel()) {
                    // Both pqs are empty
                    use_count_ = 0;
                    return false;
                }
                std::size_t select_index = stick_index_[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        --use_count_;
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
                stick_index_[0] = random_index();
                stick_index_[1] = random_index();
                use_count_ = impl_.stickiness;
            } while (true);
        }

        [[nodiscard]] bool is_empty(typename base_type::size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using config_type = Config;
    using handle_type = Handle;

    unsigned int stickiness;

    MultiQueueImpl(unsigned int num_threads, config_type const &config, typename base_type::key_compare const &c)
        : base_type(num_threads * config.c, config.seed, c), stickiness{config.stickiness} {
    }

    handle_type get_handle() noexcept {
        static std::mutex m;
        auto l = std::unique_lock(m);
        auto seed = base_type::rng();
        l.unlock();
        return handle_type{*this, seed};
    }
};

template <typename PriorityQueue, typename KeyCompare>
struct MultiQueueImpl<PriorityQueue, KeyCompare, StickPolicy::Random>
    : public MultiQueueImplBase<PriorityQueue, KeyCompare> {
    using base_type = MultiQueueImplBase<PriorityQueue, KeyCompare>;

    struct Config {
        std::uint64_t seed = 1;
        std::size_t c = 4;
        unsigned int stickiness;
    };

    class Handle {
        friend MultiQueueImpl;

        xoroshiro256starstar rng_;
        MultiQueueImpl &impl_;
        std::size_t stick_index_[2];
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
              stick_index_{random_index(), random_index()},
              use_count_{impl_.stickiness, impl_.stickiness} {
        }

        std::size_t random_index() noexcept {
            return fastrange64(rng_(), impl_.num_pqs);
        }

       public:
        void push(typename base_type::const_reference value) noexcept {
            if (use_count_[push_pq_] == 0 || !impl_.pq_list[stick_index_[push_pq_]].try_lock()) {
                do {
                    stick_index_[push_pq_] = random_index();
                } while (!impl_.pq_list[stick_index_[push_pq_]].try_lock());
                use_count_[push_pq_] = impl_.stickiness;
            }
            impl_.pq_list[stick_index_[push_pq_]].unsafe_push(value);
            impl_.pq_list[stick_index_[push_pq_]].unlock();
            assert(use_count_[push_pq_] > 0);
            --use_count_[push_pq_];
            push_pq_ = 1 - push_pq_;
        }

        bool try_pop(typename base_type::reference retval) noexcept {
            if (use_count_[0] == 0) {
                stick_index_[0] = random_index();
                use_count_[0] = impl_.stickiness;
            }
            if (use_count_[1] == 0) {
                stick_index_[1] = random_index();
                use_count_[1] = impl_.stickiness;
            }
            assert(use_count_[0] > 0 && use_count_[1] > 0);
            std::size_t pq_index[2] = {stick_index_[0], stick_index_[1]};
            typename base_type::key_type key[2] = {impl_.pq_list[pq_index[0]].concurrent_top_key(),
                                                   impl_.pq_list[pq_index[1]].concurrent_top_key()};
            unsigned int select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
            if (key[select_pq] == base_type::sentinel_traits::sentinel()) {
                // Both pqs are empty
                use_count_[0] = 0;
                use_count_[1] = 0;
                return false;
            }
            std::size_t select_index = pq_index[select_pq];
            if (impl_.pq_list[select_index].try_lock()) {
                if (!impl_.pq_list[select_index].unsafe_empty()) {
                    retval = impl_.pq_list[select_index].unsafe_top();
                    impl_.pq_list[select_index].unsafe_pop();
                    impl_.pq_list[select_index].unlock();
                    --use_count_[0];
                    --use_count_[1];
                    return true;
                }
                impl_.pq_list[select_index].unlock();
            }
            unsigned int replace_pq = select_pq;
            --use_count_[1 - replace_pq];
            do {
                pq_index[0] = random_index();
                pq_index[1] = random_index();
                key[0] = impl_.pq_list[pq_index[0]].concurrent_top_key();
                key[1] = impl_.pq_list[pq_index[1]].concurrent_top_key();
                select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
                if (key[select_pq] == base_type::sentinel_traits::sentinel()) {
                    // Both random pqs are empty
                    use_count_[replace_pq] = 0;
                    return false;
                }
                select_index = pq_index[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        stick_index_[replace_pq] = select_index;
                        use_count_[replace_pq] = impl_.stickiness - 1;
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
            } while (true);
        }

        [[nodiscard]] bool is_empty(typename base_type::size_type pos) noexcept {
            assert(pos < impl_.num_pqs);
            return impl_.pq_list[pos].concurrent_empty();
        }
    };

    using config_type = Config;
    using handle_type = Handle;

    unsigned int stickiness;

    MultiQueueImpl(unsigned int num_threads, config_type const &config, typename base_type::key_compare const &c)
        : base_type(num_threads * config.c, config.seed, c), stickiness{config.stickiness} {
    }

    handle_type get_handle() noexcept {
        static std::mutex m;
        auto l = std::unique_lock(m);
        auto seed = base_type::rng();
        l.unlock();
        return handle_type{*this, seed};
    }
};

template <typename PriorityQueue, typename KeyCompare>
struct MultiQueueImpl<PriorityQueue, KeyCompare, StickPolicy::Swapping>
    : public MultiQueueImplBase<PriorityQueue, KeyCompare> {
    using base_type = MultiQueueImplBase<PriorityQueue, KeyCompare>;

    struct Config {
        std::uint64_t seed = 1;
        std::size_t c = 4;
        unsigned int stickiness;
    };

    class Handle {
        friend MultiQueueImpl;

        xoroshiro256starstar rng_;
        MultiQueueImpl &impl_;
        std::size_t permutation_index_;
        std::size_t stick_index_[2];
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
              permutation_index_{id * 2},
              stick_index_{load_index(0), load_index(1)},
              use_count_{impl_.stickiness, impl_.stickiness} {
        }

        std::size_t random_index() noexcept {
            return fastrange64(rng_(), impl_.num_pqs);
        }

        std::size_t load_index(unsigned int pq) const noexcept {
            assert(pq <= 1);
            return permutation[permutation_index_ + pq].i.load(std::memory_order_relaxed);
        }

        // returns true if assignment changed
        bool try_swap_assignment(unsigned int pq, std::size_t index, std::size_t expected) noexcept {
            assert(pq <= 1);
            assert(index < impl_.num_pqs);
            assert(expected < impl_.num_pqs);
            std::size_t current_assigned =
                permutation[permutation_index_ + pq].i.exchange(impl_.num_pqs, std::memory_order_relaxed);
            assert(current_assigned < impl_.num_pqs);
            if (permutation[index].i.compare_exchange_strong(expected, current_assigned, std::memory_order_relaxed)) {
                current_assigned = expected;
            }
            permutation[permutation_index_ + pq].i.store(current_assigned, std::memory_order_relaxed);
            if (current_assigned == stick_index_[pq]) {
                return false;
            }
            stick_index_[pq] = current_assigned;
            return true;
        }

        void swap_assignment(unsigned int pq) noexcept {
            assert(pq <= 1);
            if (!permutation[permutation_index_ + pq].i.compare_exchange_strong(stick_index_[pq], impl_.num_pqs,
                                                                                std::memory_order_relaxed)) {
                // Permutation has changed, no need to swap
                // Only handle itself may invalidate
                assert(stick_index_[pq] != impl_.num_pqs);
                return;
            }
            std::size_t target_index;
            std::size_t target_assigned;
            do {
                target_index = random_index();
                target_assigned = permutation[target_index].i.load(std::memory_order_relaxed);
            } while (target_assigned == impl_.num_pqs ||
                     !permutation[target_index].i.compare_exchange_strong(target_assigned, stick_index_[pq],
                                                                          std::memory_order_relaxed));
            permutation[permutation_index_ + pq].i.store(target_assigned, std::memory_order_relaxed);
            stick_index_[pq] = target_assigned;
        }

        void refresh_pq(unsigned int pq) noexcept {
            if (use_count_[pq] == 0) {
                swap_assignment(pq);
            } else {
                auto current_index = load_index(pq);
                if (current_index == stick_index_[pq]) {
                    return;
                }
                stick_index_[pq] = current_index;
            }
            use_count_[pq] = impl_.stickiness;
        }

       public:
        void push(typename base_type::const_reference value) {
            refresh_pq(push_pq_);
            std::size_t index = permutation_index_ + push_pq_;
            std::size_t pq_index = stick_index_[push_pq_];
            assert(pq_index != impl_.num_pqs);
            while (!impl_.pq_list[pq_index].try_lock()) {
                do {
                    index = random_index();
                    pq_index = permutation[index].i.load(std::memory_order_relaxed);
                } while (pq_index == impl_.num_pqs);
            }
            impl_.pq_list[pq_index].unsafe_push(value);
            impl_.pq_list[pq_index].unlock();
            if (index != permutation_index_ + push_pq_ && try_swap_assignment(push_pq_, index, pq_index)) {
                use_count_[push_pq_] = impl_.stickiness;
            }
            --use_count_[push_pq_];
            push_pq_ = 1 - push_pq_;
        }

        bool try_pop(typename base_type::reference retval) {
            refresh_pq(0);
            refresh_pq(1);
            assert(use_count_[0] > 0 && use_count_[1] > 0);

            std::size_t index[2] = {permutation_index_, permutation_index_ + 1};
            std::size_t pq_index[2] = {stick_index_[0], stick_index_[1]};
            typename base_type::key_type key[2] = {impl_.pq_list[pq_index[0]].concurrent_top_key(),
                                                   impl_.pq_list[pq_index[1]].concurrent_top_key()};
            unsigned int select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
            if (key[select_pq] == base_type::sentinel_traits::sentinel()) {
                // Both pqs are empty
                use_count_[0] = 0;
                use_count_[1] = 0;
                return false;
            }
            std::size_t select_index = stick_index_[select_pq];
            if (impl_.pq_list[select_index].try_lock()) {
                if (!impl_.pq_list[select_index].unsafe_empty()) {
                    retval = impl_.pq_list[select_index].unsafe_top();
                    impl_.pq_list[select_index].unsafe_pop();
                    impl_.pq_list[select_index].unlock();
                    --use_count_[0];
                    --use_count_[1];
                    return true;
                }
                impl_.pq_list[select_index].unlock();
            }
            unsigned int replace_pq = select_pq;
            --use_count_[1 - replace_pq];
            do {
                do {
                    index[0] = random_index();
                    pq_index[0] = permutation[index[0]].i.load(std::memory_order_relaxed);
                } while (pq_index[0] == impl_.num_pqs);
                do {
                    index[1] = random_index();
                    pq_index[1] = permutation[index[1]].i.load(std::memory_order_relaxed);
                } while (pq_index[1] == impl_.num_pqs);
                key[0] = impl_.pq_list[pq_index[0]].concurrent_top_key();
                key[1] = impl_.pq_list[pq_index[1]].concurrent_top_key();
                select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
                if (key[select_pq] == base_type::sentinel_traits::sentinel()) {
                    // Both pqs are empty
                    use_count_[replace_pq] = 0;
                    return false;
                }
                select_index = pq_index[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        if (index[select_pq] != permutation_index_ + replace_pq &&
                            try_swap_assignment(replace_pq, index[select_pq], select_index)) {
                            use_count_[replace_pq] = impl_.stickiness - 1;
                        }
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
            } while (true);
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

    MultiQueueImpl(unsigned int num_threads, config_type const &config, typename base_type::key_compare const &c)
        : base_type(num_threads * config.c, config.seed, c),
          permutation(num_threads * config.c),
          stickiness{config.stickiness} {
        for (std::size_t i = 0; i < this->num_pqs; ++i) {
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
template <typename PriorityQueue, typename KeyCompare>
struct MultiQueueImpl<PriorityQueue, KeyCompare, StickPolicy::Permutation>
    : public MultiQueueImplBase<PriorityQueue, KeyCompare> {
    using base_type = MultiQueueImplBase<PriorityQueue, KeyCompare>;

    struct Config {
        std::uint64_t seed = 1;
        std::size_t c = 4;
        unsigned int stickiness;
    };

    class Handle {
        friend MultiQueueImpl;

        xoroshiro256starstar rng_;
        MultiQueueImpl &impl_;

        std::size_t permutation_index_;
        std::uint64_t current_permutation_;
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
              permutation_index_{id * 2},
              current_permutation_{impl_.permutation.load(std::memory_order_relaxed)},
              use_count_{impl_.stickiness} {
        }

        std::size_t random_index() noexcept {
            return fastrange64(rng_(), impl_.num_pqs);
        }

        void update_permutation() {
            std::uint64_t new_permutation = rng_() | 1;
            if (impl_.permutation.compare_exchange_strong(current_permutation_, new_permutation,
                                                          std::memory_order_relaxed)) {
                current_permutation_ = new_permutation;
            }
        }

        void refresh_permutation() noexcept {
            if (use_count_ == 0) {
                update_permutation();
            } else {
                auto permutation = impl_.perm.load(std::memory_order_relaxed);
                if (permutation == current_permutation_) {
                    return;
                }
                current_permutation_ = permutation;
            }
            use_count_ = impl_.stickiness;
        }

        std::size_t get_index(unsigned int pq) const noexcept {
            std::size_t a = current_permutation_ & Mask;
            std::size_t b = current_permutation_ >> 32;
            assert(a & 1 == 1);
            return ((permutation_index_ + pq) * a + b) & (impl_.num_pqs - 1);
        }

       public:
        void push(typename base_type::const_reference value) noexcept {
            refresh_permutation();
            std::size_t pq_index = get_index(push_pq_);
            while (!impl_.pq_list[pq_index].try_lock()) {
                pq_index = random_index();
            }
            impl_.pq_list[pq_index].unsafe_push(value);
            impl_.pq_list[pq_index].unlock();
            --use_count_;
            push_pq_ = 1 - push_pq_;
        }

        bool try_pop(typename base_type::reference retval) noexcept {
            refresh_permutation();
            assert(use_count_ > 0);
            typename base_type::size_type pq_index[2] = {get_index(0), get_index(1)};

            typename base_type::key_type key[2] = {impl_.pq_list[pq_index[0]].concurrent_top_key(),
                                                   impl_.pq_list[pq_index[1]].concurrent_top_key()};
            unsigned int select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
            if (key[select_pq] == base_type::sentinel_traits::sentinel()) {
                use_count_ = 0;
                return false;
            }
            std::size_t select_index = pq_index[select_pq];
            if (impl_.pq_list[select_index].try_lock()) {
                if (!impl_.pq_list[select_index].unsafe_empty()) {
                    retval = impl_.pq_list[select_index].unsafe_top();
                    impl_.pq_list[select_index].unsafe_pop();
                    impl_.pq_list[select_index].unlock();
                    --use_count_;
                    return true;
                }
                impl_.pq_list[select_index].unlock();
            }
            --use_count_;
            do {
                pq_index[0] = random_index();
                pq_index[1] = random_index();
                key[0] = impl_.pq_list[pq_index[0]].concurrent_top_key();
                key[1] = impl_.pq_list[pq_index[1]].concurrent_top_key();
                select_pq = impl_.compare(key[0], key[1]) ? 1 : 0;
                if (key[select_pq] == base_type::sentinel_traits::sentinel()) {
                    return false;
                }
                select_index = pq_index[select_pq];
                if (impl_.pq_list[select_index].try_lock()) {
                    if (!impl_.pq_list[select_index].unsafe_empty()) {
                        retval = impl_.pq_list[select_index].unsafe_top();
                        impl_.pq_list[select_index].unsafe_pop();
                        impl_.pq_list[select_index].unlock();
                        return true;
                    }
                    impl_.pq_list[select_index].unlock();
                }
            } while (true);
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
    unsigned int handle_count = 0;

    static std::size_t next_power_of_two(std::size_t n) {
        return std::size_t{1} << static_cast<unsigned int>(std::ceil(std::log2(n)));
    }

    MultiQueueImpl(unsigned int num_threads, config_type const &config, typename base_type::key_compare const &c)
        : base_type(next_power_of_two(num_threads * config.c), config.seed, c),
          stickiness{config.stickiness},
          permutation{1} {
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
