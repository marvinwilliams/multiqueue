#pragma once

#include "pcg_random.hpp"

#include <algorithm>
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
    int use_count{};

    void replace_pq(int pq) noexcept {
        if (NumPopPQs == 2) {
            do {
                stick_index[pq] = rng() & pq_mask;
            } while (stick_index[pq] == stick_index[1 - pq]);
        } else {
            std::size_t index;
            do {
                index = rng() & pq_mask;
            } while (std::find(stick_index.begin(), stick_index.end(), index) != stick_index.end());
            stick_index[pq] = index;
        }
    }

    void refresh_pqs() noexcept {
        for (auto it = stick_index.begin(); it != stick_index.end(); ++it) {
            do {
                *it = rng() & pq_mask;
            } while (std::find(stick_index.begin(), it, *it) != it);
        }
        use_count = stick_dist(rng);
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
        : pq_mask((assert(num_pqs > 0 && (num_pqs & (num_pqs - 1)) == 0), static_cast<std::int32_t>(num_pqs) - 1)),
          stick_dist(1.0 / c.stickiness) {
        auto id = sd.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{c.seed, id};
        rng.seed(seq);
        for (auto it = stick_index.begin(); it != stick_index.end(); ++it) {
            do {
                *it = rng() & pq_mask;
            } while (std::find(stick_index.begin(), it, *it) != it);
        }
        use_count = stick_dist(rng);
    }

    auto const& get_pq_indices() noexcept {
        if (use_count <= 0) {
            refresh_pqs();
        }
        return stick_index;
    }

    void reset_pq(std::size_t index) noexcept {
        replace_pq(index);
    }

    void reset_pqs() noexcept {
        refresh_pqs();
    }

    void used_pqs() noexcept {
        --use_count;
    }
};

}  // namespace multiqueue::stick_policy
