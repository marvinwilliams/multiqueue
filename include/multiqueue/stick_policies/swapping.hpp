#pragma once

#include "multiqueue/build_config.hpp"
#include "multiqueue/config.hpp"

#include "pcg_random.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <random>

namespace multiqueue {

template <typename Impl>
struct Swapping {
    using key_type = typename Impl::key_type;
    using size_type = typename Impl::size_type;
    using reference = typename Impl::reference;
    using const_reference = typename Impl::const_reference;

    struct SharedData {
        struct alignas(BuildConfiguration::L1CacheLinesize) AlignedIndex {
            std::atomic<size_type> idx;
        };
        using Permutation = std::vector<AlignedIndex>;

        Permutation permutation;

        explicit SharedData(std::size_t n) : permutation(n) {
            for (std::size_t i = 0; i < n; ++i) {
                permutation[i].idx = i;
            }
        }
    };

    Impl &impl;
    pcg32 rng{};
    std::size_t idx;
    std::array<size_type, 2> stick_index{};
    std::array<int, 2> use_count{};

    explicit Swapping(int id, Impl &i) noexcept : impl{i}, idx{static_cast<std::size_t>(id * 2)} {
        auto seq = std::seed_seq{impl.config().seed, id};
        rng.seed(seq);
    }

    inline size_type random_pq_index() noexcept {
        return std::uniform_int_distribution<size_type>(0, impl.num_pqs() - 1)(rng);
    }

    void swap_assignment(std::size_t pq) noexcept {
        assert(pq <= 1);
        if (!impl.shared_data().permutation[idx + pq].idx.compare_exchange_strong(stick_index[pq], impl.num_pqs(),
                                                                                  std::memory_order_relaxed)) {
            // Permutation has changed, no need to swap
            // Only handle itself may invalidate
            assert(stick_index[pq] < impl.num_pqs());
            return;
        }
        std::size_t target_index = 0;
        size_type target_assigned;
        do {
            target_index = random_pq_index();
            target_assigned = impl.shared_data().permutation[target_index].idx.load(std::memory_order_relaxed);
        } while (target_assigned == impl.num_pqs() ||
                 !impl.shared_data().permutation[target_index].idx.compare_exchange_strong(
                     target_assigned, stick_index[pq], std::memory_order_relaxed));
        impl.shared_data().permutation[idx + pq].idx.store(target_assigned, std::memory_order_relaxed);
        stick_index[pq] = target_assigned;
    }

    void refresh_pq(std::size_t pq) noexcept {
        if (use_count[pq] == 0) {
            swap_assignment(pq);
            use_count[pq] = impl.config().stickiness;
        } else {
            auto current_index = impl.shared_data().permutation[idx + pq].idx.load(std::memory_order_relaxed);
            if (current_index != stick_index[pq]) {
                stick_index[pq] = current_index;
                use_count[pq] = impl.config().stickiness;
            }
        }
    }

    void push(const_reference value) {
        auto i = std::uniform_int_distribution<size_type>{0, 1}(rng);
        refresh_pq(i);
        if (impl.try_push(stick_index[i], value) == Impl::push_result::Success) {
            return;
        }
        do {
            swap_assignment(i);
        } while (impl.try_push(stick_index[i], value) == Impl::push_result::Locked);
        use_count[i] = impl.config().stickiness - 1;
    }

    bool try_pop(reference retval) {
        refresh_pq(0);
        refresh_pq(1);
        do {
            auto result = impl.try_pop_compare(stick_index, retval);
            if (result == Impl::pop_result::Success) {
                --use_count[0];
                --use_count[1];
                return true;
            }
            if (result == Impl::pop_result::Empty) {
                break;
            }
            swap_assignment(0);
            swap_assignment(1);
            use_count[0] = impl.config().stickiness;
            use_count[1] = impl.config().stickiness;
        } while (true);
        use_count[0] = 0;
        use_count[1] = 0;
        return impl.try_pop_any(random_pq_index(), retval);
    }
};

}  // namespace multiqueue
