#pragma once

#include "multiqueue/build_config.hpp"
#include "pcg_random.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <random>

namespace multiqueue::queue_selection {

template <unsigned NumPopPQs = build_config::DefaultNumPopPQs>
class Random {
    pcg32 rng{};
    std::vector<std::size_t> pqs{};
    std::uniform_int_distribution<std::size_t> pq_dist;

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

    explicit Random(std::size_t num_pqs, Config const& c, SharedData& sd) noexcept
        : pqs(num_pqs), pq_dist(0, num_pqs - 1) {
        auto id = sd.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{c.seed, id};
        rng.seed(seq);
        std::iota(pqs.begin(), pqs.end(), 0);
    }

    std::size_t get_push_pq() noexcept {
        return pq_dist(rng);
    }

    void reset_push_pq() noexcept {
    }

    void use_push_pq() noexcept {
    }

    auto get_pop_pqs() noexcept {
        if constexpr (NumPopPQs == 1) {
            return std::array{pq_dist(rng)};
        }
        if constexpr (NumPopPQs == 2) {
            auto ret = std::array{pq_dist(rng), pq_dist(rng)};
            while (ret[0] == ret[1]) {
                ret[1] = pq_dist(rng);
            }
            return ret;
        }
        std::array<std::size_t, NumPopPQs> ret{};
        for (unsigned i = 0; i < NumPopPQs; ++i) {
            auto r = pq_dist(rng);
            std::swap(pqs[i], pqs[r]);
            ret[i] = pqs[i];
        }
        return ret;
    }

    void reset_pop_pqs() noexcept {
    }

    void use_pop_pqs() noexcept {
    }
};

}  // namespace multiqueue::queue_selection
