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

template <int num_pop_candidates = 2, bool pop_stale = true>
class Random {
    static_assert(num_pop_candidates > 0);

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
    pcg32 rng_{};

    std::array<std::size_t, static_cast<std::size_t>(num_pop_candidates)> generate_indices(
        std::size_t num_pqs) noexcept {
        std::array<std::size_t, static_cast<std::size_t>(num_pop_candidates)> indices{};
        indices[0] = rng_() & (num_pqs - 1);
        for (auto it = std::next(indices.begin()); it != indices.end(); ++it) {
            do {
                *it = rng_() & (num_pqs - 1);
            } while (std::find(indices.begin(), it, *it) != it);
        }
        return indices;
    }

   protected:
    explicit Random(Config const& config, SharedData& shared_data) noexcept {
        auto id = shared_data.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{config.seed, id};
        rng_.seed(seq);
    }

    template <typename Context>
    std::optional<typename Context::value_type> try_pop(Context& ctx) {
        while (true) {
            auto indices = generate_indices(ctx.num_pqs());
            auto best_pq = indices[0];
            auto best_key = ctx.pq_guards()[best_pq].top_key();
            for (std::size_t i = 1; i < static_cast<std::size_t>(num_pop_candidates); ++i) {
                auto key = ctx.pq_guards()[indices[i]].top_key();
                if (ctx.compare(best_key, key)) {
                    best_pq = indices[i];
                    best_key = key;
                }
            }
            auto& guard = ctx.pq_guards()[best_pq];
            if (!guard.try_lock()) {
                continue;
            }
            if (guard.get_pq().empty()) {
                guard.unlock();
                return std::nullopt;
            }
            if (!pop_stale && Context::get_key(guard.get_pq().top()) != best_key) {
                guard.unlock();
                continue;
            }
            auto v = guard.get_pq().top();
            guard.get_pq().pop();
            guard.popped();
            guard.unlock();
            return v;
        }
    }

    template <typename Context>
    void push(Context& ctx, typename Context::value_type const& v) {
        std::size_t i{};
        do {
            i = rng_() & (ctx.num_pqs() - 1);
        } while (!ctx.pq_guards()[i].try_lock());
        ctx.pq_guards()[i].get_pq().push(v);
        ctx.pq_guards()[i].pushed();
        ctx.pq_guards()[i].unlock();
    }
};

}  // namespace multiqueue::mode
