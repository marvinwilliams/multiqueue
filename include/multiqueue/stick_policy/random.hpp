#pragma once

#include "pcg_random.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <random>

namespace multiqueue::stick_policy {

template <unsigned NumPopPQs = 2>
class Random {
    pcg32 rng{};
    std::int32_t pq_mask{0};
    std::geometric_distribution<int> stick_dist;
    std::array<std::size_t, NumPopPQs> stick_index{};
    std::array<int, NumPopPQs> use_count{};
    unsigned push_pq{};

    void reset_pq(unsigned pq) noexcept {
        stick_index[pq] = rng() & pq_mask;
        use_count[pq] = stick_dist(rng);
    }

   public:
    struct Config {
        int seed{1};
        int stickiness{16};
    };

   protected:
    struct SharedData {
        std::atomic_int id_count{0};

        explicit SharedData(std::size_t /*num_pqs*/) noexcept {
        }
    };

    explicit Random(std::size_t num_pqs, Config const& c, SharedData& sd) noexcept
        : pq_mask((assert(num_pqs > 0 && (num_pqs & (num_pqs - 1)) == 0), static_cast<std::int32_t>(num_pqs) - 1)), stick_dist(1.0 / c.stickiness) {
        auto id = sd.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{c.seed, id};
        rng.seed(seq);
        for (unsigned i = 0; i < NumPopPQs; ++i) {
            reset_pq(i);
        }
    }

    std::size_t get_push_pq() noexcept {
        return stick_index[push_pq];
    }

    void reset_push_pq() noexcept {
        reset_pq(push_pq);
    }

    void use_push_pq() noexcept {
        if (use_count[push_pq] <= 0) {
            reset_pq(push_pq);
        } else {
            --use_count[push_pq];
        }
        push_pq = (push_pq + 1) % NumPopPQs;
    }

    auto const& get_pop_pqs() noexcept {
        return stick_index;
    }

    void reset_pop_pqs() noexcept {
        for (unsigned i = 0; i < NumPopPQs; ++i) {
            reset_pq(i);
        }
    }

    void use_pop_pqs() noexcept {
        for (unsigned i = 0; i < NumPopPQs; ++i) {
            if (use_count[i] <= 0) {
                reset_pq(i);
            } else {
                --use_count[i];
            }
        }
    }
};

}  // namespace multiqueue::stick_policy