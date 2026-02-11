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
class RandomMoveWorse {
    static_assert(num_pop_candidates == 2);

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
        indices[0] = std::uniform_int_distribution<std::size_t>{0, num_pqs - 1}(rng_);
        for (auto it = std::next(indices.begin()); it != indices.end(); ++it) {
            do {
                *it = std::uniform_int_distribution<std::size_t>{0, num_pqs - 1}(rng_);
            } while (std::find(indices.begin(), it, *it) != it);
        }
        return indices;
    }

   protected:
    explicit RandomMoveWorse(Config const& config, SharedData& shared_data) noexcept {
        auto id = shared_data.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{config.seed, id};
        rng_.seed(seq);
    }

    template <typename Context>
    std::optional<typename Context::value_type> try_pop(Context& ctx) {
        while (true) {
            auto indices = generate_indices(ctx.num_pqs());
            typename Context::key_type keys[2] = {ctx.pq_guards()[indices[0]].top_key(),
                                                  ctx.pq_guards()[indices[1]].top_key()};
            auto best_pq = static_cast<std::size_t>(ctx.compare(keys[0], keys[1]));
            auto& guard = ctx.pq_guards()[indices[best_pq]];
            if (!guard.try_lock()) {
                continue;
            }
            if (guard.get_pq().empty()) {
                guard.unlock();
                return std::nullopt;
            }
            if (!pop_stale && Context::get_key(guard.get_pq().top()) != keys[best_pq]) {
                guard.unlock();
                continue;
            }
            auto v = guard.get_pq().top();
            guard.get_pq().pop();
            if (!guard.get_pq().empty() && ctx.compare(keys[1 - best_pq], Context::get_key(guard.get_pq().top()))) {
                auto tmp = guard.get_pq().top();
                guard.get_pq().pop();
                guard.popped();
                guard.unlock();
                auto& guard_other = ctx.pq_guards()[indices[1 - best_pq]];
                if (guard_other.try_lock()) {
                    guard_other.get_pq().push(tmp);
                    guard_other.pushed();
                    guard_other.unlock();
                } else {
                    push(ctx, tmp);
                }
            } else {
                guard.popped();
                guard.unlock();
            }
            return v;
        }
    }

    template <typename Context>
    void push(Context& ctx, typename Context::value_type const& v) {
        std::size_t i{};
        do {
            i = std::uniform_int_distribution<std::size_t>{0, ctx.num_pqs() - 1}(rng_);
        } while (!ctx.pq_guards()[i].try_lock());
        ctx.pq_guards()[i].get_pq().push(v);
        ctx.pq_guards()[i].pushed();
        ctx.pq_guards()[i].unlock();
    }
};

}  // namespace multiqueue::mode
