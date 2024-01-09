#pragma once

#include "pcg_random.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <optional>
#include <random>

namespace multiqueue::mode {

template <int num_pop_candidates = 2>
class StickRandom {
    static_assert(num_pop_candidates > 0);
    pcg32 rng{};
    std::size_t pop_index{};
    std::size_t push_index{};
    int pop_count{};
    int push_count{};
    std::geometric_distribution<int> stick_dist;

    std::array<std::size_t, static_cast<std::size_t>(num_pop_candidates)> generate_pop_candidates(
        std::size_t num_pqs) noexcept {
        std::array<std::size_t, static_cast<std::size_t>(num_pop_candidates)> indices{};
        indices[0] = rng() & (num_pqs - 1);
        for (auto it = std::next(indices.begin()); it != indices.end(); ++it) {
            do {
                *it = rng() & (num_pqs - 1);
            } while (std::find(indices.begin(), it, *it) != it);
        }
        return indices;
    }

    template <typename Context>
    void reset_pop_index(Context const& ctx) noexcept {
        auto candidates = generate_pop_candidates(ctx.num_pqs());
        pop_index = candidates[0];
        auto best_key = ctx.pq_guards()[pop_index].top_key();
        for (int i = 1; i < num_pop_candidates; ++i) {
            auto key = ctx.pq_guards()[candidates[i]].top_key();
            if (ctx.compare(best_key, key)) {
                pop_index = candidates[i];
                best_key = key;
            }
        }
        pop_count = stick_dist(rng);
    }

   public:
    struct Config {
        int seed{1};
        int pop_tries{1};
        int stickiness{16};
    };

    struct SharedData {
        std::atomic_int id_count{0};

        explicit SharedData(std::size_t /*num_pqs*/) noexcept {
        }
    };

   protected:
    explicit StickRandom(Config const& config, SharedData& shared_data) noexcept : stick_dist(1.0 / config.stickiness) {
        auto id = shared_data.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{config.seed, id};
        rng.seed(seq);
    }

    template <typename Context>
    std::optional<typename Context::value_type> try_pop(Context& ctx) {
        if (pop_count <= 0 || !ctx.pq_guards()[pop_index].try_lock()) {
            do {
                reset_pop_index(ctx);
            } while (!ctx.pq_guards()[pop_index].try_lock());
            pop_count = stick_dist(rng);
        }
        auto& guard = ctx.pq_guards()[pop_index];
        if (guard.get_pq().empty()) {
            guard.unlock();
            pop_count = 0;
            return std::nullopt;
        }
        auto retval = guard.get_pq().top();
        guard.get_pq().pop();
        guard.popped();
        guard.unlock();
        --pop_count;
        return retval;
    }

    template <typename Context>
    void push(Context& ctx, typename Context::value_type const& v) {
        if (push_count <= 0 || !ctx.pq_guards()[push_index].try_lock()) {
            do {
                push_index = rng() & (ctx.num_pqs() - 1);
            } while (!ctx.pq_guards()[push_index].try_lock());
            push_count = stick_dist(rng);
        }
        ctx.pq_guards()[push_index].get_pq().push(v);
        ctx.pq_guards()[push_index].pushed();
        ctx.pq_guards()[push_index].unlock();
        --push_count;
    }
};

}  // namespace multiqueue::mode
