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

    std::size_t get_push_pq() noexcept {
        return get_random_pq();
    }

    void reset_push_pq() noexcept {
    }

    void use_push_pq() noexcept {
    }

    auto get_pop_pqs() noexcept {
        if constexpr (NumPopPQs == 1) {
            return std::array<std::size_t, 1>{get_random_pq()};
        } else if constexpr (NumPopPQs == 2) {
            std::array<std::size_t, NumPopPQs> ret{get_random_pq(), get_random_pq()};
            while (ret[0] == ret[1]) {
                ret[1] = get_random_pq();
            }
            return ret;
        }
        std::array<std::size_t, NumPopPQs> ret{};
        for (auto it = ret.begin(); it != ret.end(); std::advance(it)) {
            do {
                *it = rng() & pq_mask;
            } while (std::find(ret.begin(), it, *it) != it);
        }
        return ret;
    }

    void reset_pop_pqs() noexcept {
    }

    void use_pop_pqs() noexcept {
    }
};

}  // namespace multiqueue::stick_policy
