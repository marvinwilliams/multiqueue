#pragma once

#include "multiqueue/build_config.hpp"
#include "multiqueue/config.hpp"

#include "pcg_random.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <random>

namespace multiqueue::queue_selection {

class SwapAssignment {
   protected:
    struct alignas(build_config::L1CacheLinesize) AlignedIndex {
        std::atomic<std::size_t> value;
    };
    using Permutation = std::vector<AlignedIndex>;
    struct SharedData {
        std::atomic_int id_count{0};

        Permutation permutation;

        explicit SharedData(std::size_t num_pqs) : permutation(num_pqs) {
            for (std::size_t i = 0; i < num_pqs; ++i) {
                permutation[i].value = i;
            }
        }
    };

   public:
    struct Config {
        int seed{1};
        int stickiness{16};
    };

   private:
    pcg32 rng{};
    std::uniform_int_distribution<std::size_t> pq_dist;
    std::geometric_distribution<int> stick_dist;
    std::array<std::size_t, 2> stick_index{};
    std::array<int, 2> use_count{};
    int push_pq{};
    Permutation& permutation;
    Permutation::iterator assignment{};

    void reset_pq(int pq) noexcept {
        static constexpr std::size_t swapping = std::numeric_limits<std::size_t>::max();
        assert(pq <= 1);
        use_count[pq] = stick_dist(rng);
        if (!(assignment + pq)->value.compare_exchange_strong(stick_index[pq], swapping, std::memory_order_relaxed)) {
            // Permutation has changed, no need to swap
            // Only we may set ourself to swapping
            return;
        }
        std::size_t target_index;
        std::size_t target_assigned;
        do {
            target_index = pq_dist(rng);
            target_assigned = permutation[target_index].value.load(std::memory_order_relaxed);
        } while (target_assigned == swapping ||
                 !permutation[target_index].value.compare_exchange_weak(target_assigned, stick_index[pq],
                                                                        std::memory_order_relaxed));
        (assignment + pq)->value.store(target_assigned, std::memory_order_relaxed);
        stick_index[pq] = target_assigned;
    }

    void refresh_pq(int pq) noexcept {
        auto index = (assignment + pq)->value.load(std::memory_order_relaxed);
        if (index != stick_index[pq]) {
            stick_index[pq] = index;
            use_count[pq] = stick_dist(rng);
        }
    }

   protected:
    explicit SwapAssignment(std::size_t num_pqs, Config const& c, SharedData& sd) noexcept
        : pq_dist(0, num_pqs - 1), stick_dist(1.0 / c.stickiness), permutation{sd.permutation} {
        auto id = sd.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{c.seed, id};
        rng.seed(seq);
        assignment = permutation.begin() + 2 * id;
        use_count[0] = stick_dist(rng);
        use_count[1] = stick_dist(rng);
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
        push_pq = 1 - push_pq;
    }

    auto const& get_pop_pqs() noexcept {
        refresh_pq(0);
        refresh_pq(1);
        return stick_index;
    }

    void reset_pop_pqs() noexcept {
        reset_pq(0);
        reset_pq(1);
    }

    void use_pop_pqs() noexcept {
        if (use_count[0] == 0) {
            reset_pq(0);
        } else {
            --use_count[0];
        }
        if (use_count[1] == 0) {
            reset_pq(1);
        } else {
            --use_count[1];
        }
    }
};

}  // namespace multiqueue::queue_selection
