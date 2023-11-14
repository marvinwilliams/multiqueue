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
class Noop {
    pcg32 rng{};
    std::int32_t pq_mask{0};
    std::array<std::size_t, NumPopPQs> stick_index{};

    std::size_t get_random_pq() noexcept {
        return rng() & pq_mask;
    }

   public:
    struct Config {
        int seed{1};
    };

   protected:
    struct SharedData {
        std::atomic_int id_count{0};

        explicit SharedData(std::size_t /*num_pqs*/) noexcept {
        }
    };

    explicit Noop(std::size_t num_pqs, Config const& c, SharedData& sd) noexcept
        : pq_mask((assert(num_pqs > 0 && (num_pqs & (num_pqs - 1)) == 0), static_cast<std::int32_t>(num_pqs) - 1)) {
        auto id = sd.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{c.seed, id};
        rng.seed(seq);
    }

    auto const& get_pq_indices() noexcept {
        for (auto it = stick_index.begin(); it != stick_index.end(); ++it) {
            do {
                *it = rng() & pq_mask;
            } while (std::find(stick_index.begin(), it, *it) != it);
        }
        return stick_index;
    }

    void replace_pq(std::size_t index) noexcept {
        if (NumPopPQs == 2) {
            do {
                stick_index[index] = rng() & pq_mask;
            } while (stick_index[index] == stick_index[1 - index]);
        } else {
            std::size_t i;
            do {
                i = rng() & pq_mask;
            } while (std::find(stick_index.begin(), stick_index.end(), i) != stick_index.end());
            stick_index[index] = i;
        }
    }

    void reset_pqs() noexcept {
    }

    void used_pqs() noexcept {
    }
};

}  // namespace multiqueue::stick_policy
