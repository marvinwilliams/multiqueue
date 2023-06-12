#pragma once

#include "pcg_random.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <random>

namespace multiqueue::queue_selection {

class StickRandom {
   public:
    struct Config {
        int seed{1};
        int stickiness{16};
    };
    struct SharedData {
        std::atomic_int id_count{0};

        explicit SharedData(std::size_t /*num_pqs*/) noexcept {
        }
    };

   private:
    pcg32 rng{};
    std::uniform_int_distribution<std::size_t> pq_dist;
    std::geometric_distribution<int> stick_dist;
    std::array<std::size_t, 2> stick_index{};
    std::array<int, 2> use_count{};
    int push_pq{};

    void reset_pq(int pq) noexcept {
        stick_index[pq] = pq_dist(rng);
        use_count[pq] = stick_dist(rng);
    }

   protected:
    explicit StickRandom(std::size_t num_pqs, Config const& c, SharedData& sd) noexcept
        : pq_dist(0, num_pqs - 1), stick_dist(1.0 / c.stickiness) {
        auto id = sd.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{c.seed, id};
        rng.seed(seq);
        reset_pq(0);
        reset_pq(1);
    }

    std::size_t get_push_pq() noexcept {
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
