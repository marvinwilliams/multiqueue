#pragma once

#include "pcg_random.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <random>

namespace multiqueue::queue_selection {

class Random {
   public:
    struct Config {
        int seed{1};
    };
    struct SharedData {
        std::atomic_int id_count{0};

        explicit SharedData(std::size_t /*num_pqs*/) noexcept {
        }
    };

   private:
    pcg32 rng{};
    std::uniform_int_distribution<std::size_t> pq_dist;

   protected:
    explicit Random(std::size_t num_pqs, Config const& c, SharedData& sd) noexcept : pq_dist(0, num_pqs - 1) {
        auto id = sd.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{c.seed, id};
        rng.seed(seq);
    }

    std::size_t get_push_pq() noexcept {
        return pq_dist(rng);
    }

    void reset_push_pq() noexcept {
    }

    void use_push_pq() noexcept {
    }

    auto get_pop_pqs() noexcept {
        return std::array{pq_dist(rng), pq_dist(rng)};
    }

    void reset_pop_pqs() noexcept {
    }

    void use_pop_pqs() noexcept {
    }
};

}  // namespace multiqueue
