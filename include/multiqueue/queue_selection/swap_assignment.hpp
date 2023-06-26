#pragma once

#include "multiqueue/build_config.hpp"

#include "pcg_random.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <random>

namespace multiqueue::queue_selection {

template <unsigned NumPopPQs = 2>
class SwapAssignment {
    struct alignas(build_config::L1CacheLinesize) AlignedIndex {
        std::atomic<std::size_t> value;
    };
    using Permutation = std::vector<AlignedIndex>;

    pcg32 rng{};
    std::uniform_int_distribution<std::size_t> pq_dist;
    std::geometric_distribution<int> stick_dist;
    std::array<std::size_t, NumPopPQs> stick_index{};
    std::array<int, NumPopPQs> use_count{};
    unsigned push_pq{};
    Permutation& permutation;
    typename Permutation::iterator assignment{};

    void reset_pq(unsigned pq) noexcept {
        static constexpr std::size_t swapping = std::numeric_limits<std::size_t>::max();
        assert(pq <= NumPopPQs);
        use_count[pq] = stick_dist(rng);
        if (!(assignment + static_cast<std::ptrdiff_t>(pq))
                 ->value.compare_exchange_strong(stick_index[pq], swapping, std::memory_order_relaxed)) {
            // Permutation has changed, no need to swap
            // Only we may set ourself to swapping
            return;
        }
        std::size_t target_index;     // NOLINT(cppcoreguidelines-init-variables)
        std::size_t target_assigned;  // NOLINT(cppcoreguidelines-init-variables)
        do {
            target_index = pq_dist(rng);
            target_assigned = permutation[target_index].value.load(std::memory_order_relaxed);
        } while (target_assigned == swapping ||
                 !permutation[target_index].value.compare_exchange_weak(target_assigned, stick_index[pq],
                                                                        std::memory_order_relaxed));
        (assignment + static_cast<std::ptrdiff_t>(pq))->value.store(target_assigned, std::memory_order_relaxed);
        stick_index[pq] = target_assigned;
    }

    void refresh_pq(unsigned pq) noexcept {
        auto index = (assignment + static_cast<std::ptrdiff_t>(pq))->value.load(std::memory_order_relaxed);
        if (index != stick_index[pq]) {
            stick_index[pq] = index;
            use_count[pq] = stick_dist(rng);
        }
    }

   public:
    struct Config {
        int seed{1};
        int stickiness{16};
    };

   protected:
    struct SharedData {
        std::atomic_int id_count{0};

        Permutation permutation;

        explicit SharedData(std::size_t num_pqs) : permutation(num_pqs) {
            for (std::size_t i = 0; i < num_pqs; ++i) {
                permutation[i].value = i;
            }
        }
    };
    explicit SwapAssignment(std::size_t num_pqs, Config const& c, SharedData& sd) noexcept
        : pq_dist(0, num_pqs - 1), stick_dist(1.0 / c.stickiness), permutation{sd.permutation} {
        auto id = sd.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{c.seed, id};
        rng.seed(seq);
        assignment = std::begin(permutation) + static_cast<std::ptrdiff_t>(NumPopPQs) * id;
        for (unsigned i = 0; i < NumPopPQs; ++i) {
            stick_index[i] = (assignment + static_cast<std::ptrdiff_t>(i))->value.load(std::memory_order_relaxed);
            use_count[i] = stick_dist(rng);
        }
    }

    std::size_t get_push_pq() noexcept {
        refresh_pq(push_pq);
        return stick_index[push_pq];
    }

    void reset_push_pq() noexcept {
        reset_pq(push_pq);
    }

    void use_push_pq() noexcept {
        if (use_count[push_pq] == 0) {
            reset_pq(push_pq);
        } else {
            --use_count[push_pq];
        }
        push_pq = (push_pq + 1) % NumPopPQs;
    }

    auto const& get_pop_pqs() noexcept {
        for (unsigned i = 0; i < NumPopPQs; ++i) {
            refresh_pq(i);
        }
        return stick_index;
    }

    void reset_pop_pqs() noexcept {
        for (unsigned i = 0; i < NumPopPQs; ++i) {
            reset_pq(i);
        }
    }

    void use_pop_pqs() noexcept {
        for (unsigned i = 0; i < NumPopPQs; ++i) {
            if (use_count[i] == 0) {
                reset_pq(i);
            } else {
                --use_count[i];
            }
        }
    }
};

}  // namespace multiqueue::queue_selection
