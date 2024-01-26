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
    std::array<std::size_t, static_cast<std::size_t>(num_pop_candidates)> pop_index{};
    int count{};

    void refresh_pop_index(std::size_t num_pqs) noexcept {
        pop_index[0] = rng() & (num_pqs - 1);
        for (auto it = std::next(pop_index.begin()); it != pop_index.end(); ++it) {
            do {
                *it = rng() & (num_pqs - 1);
            } while (std::find(pop_index.begin(), it, *it) != it);
        }
    }

   protected:
    explicit StickRandom(Config const& config, SharedData& shared_data) noexcept {
        auto id = shared_data.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{config.seed, id};
        rng.seed(seq);
    }

    template <typename Context>
    std::optional<typename Context::value_type> try_pop(Context& ctx) {
        if (count == 0) {
            refresh_pop_index(ctx.num_pqs());
            count = ctx.config().stickiness;
        }
        while (true) {
            std::size_t best = pop_index[0];
            auto best_key = ctx.pq_guards()[best].top_key();
            for (std::size_t i = 1; i < static_cast<std::size_t>(num_pop_candidates); ++i) {
                auto key = ctx.pq_guards()[pop_index[i]].top_key();
                if (ctx.compare(best_key, key)) {
                    best = pop_index[i];
                    best_key = key;
                }
            }
            auto& guard = ctx.pq_guards()[best];
            if (guard.try_lock()) {
                if (guard.get_pq().empty()) {
                    guard.unlock();
                    count = 0;
                    return std::nullopt;
                }
                auto v = guard.get_pq().top();
                guard.get_pq().pop();
                guard.popped();
                guard.unlock();
                --count;
                return v;
            }
            refresh_pop_index(ctx.num_pqs());
            count = ctx.config().stickiness;
        }
    }

    template <typename Context>
    void push(Context& ctx, typename Context::value_type const& v) {
        if (count == 0) {
            refresh_pop_index(ctx.num_pqs());
            count = ctx.config().stickiness;
        }
        std::size_t push_index = rng() % num_pop_candidates;
        while (true) {
            auto& guard = ctx.pq_guards()[pop_index[push_index]];
            if (guard.try_lock()) {
                guard.get_pq().push(v);
                guard.pushed();
                guard.unlock();
                --count;
                return;
            }
            std::size_t new_index{};
            do {
                new_index = rng() & (ctx.num_pqs() - 1);
            } while (std::find(pop_index.begin(), pop_index.end(), new_index) != pop_index.end());
            pop_index[push_index] = new_index;
        }
    }
};

}  // namespace multiqueue::mode
