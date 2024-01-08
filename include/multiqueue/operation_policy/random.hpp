#pragma once

#include "pcg_random.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <optional>
#include <random>

namespace multiqueue::operation_policy {

template <int num_pop_candidates = 2, bool pop_stale = true>
class Random {
    static_assert(num_pop_candidates > 0);
    pcg32 rng{};

    std::array<std::size_t, static_cast<std::size_t>(num_pop_candidates)> generate_indices(
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

   public:
    struct Config {
        int seed{1};
        int pop_tries{1};
    };

    struct SharedData {
        Config config;
        std::atomic_int id_count{0};

        explicit SharedData(Config const& c, std::size_t /*num_pqs*/) noexcept : config(c) {
        }
    };

   protected:
    template <typename Context>
    explicit Random(Context& ctx) noexcept {
        auto id = ctx.operation_policy_data().id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{ctx.operation_policy_data().config.seed, id};
        rng.seed(seq);
    }

    template <typename Context>
    std::optional<typename Context::value_type> try_pop(Context& ctx) {
        int tries = 0;
        while (tries < ctx.operation_policy_data().config.pop_tries) {
            auto indices = generate_indices(ctx.num_pqs());
            auto best_pq = indices[0];
            auto best_key = ctx.pq_list()[best_pq].top_key();
            if constexpr (num_pop_candidates == 2) {
                auto key = ctx.pq_list()[indices[1]].top_key();
                if (ctx.compare(best_key, key)) {
                    best_pq = indices[1];
                    best_key = key;
                } else if (Context::is_sentinel(best_key)) {
                    ++tries;
                    continue;
                }
            } else {
                for (int i = 1; i < num_pop_candidates; ++i) {
                    auto key = ctx.pq_list()[indices[i]].top_key();
                    if (ctx.compare(best_key, key)) {
                        best_pq = indices[i];
                        best_key = key;
                    }
                }
                if (Context::is_sentinel(best_key)) {
                    ++tries;
                    continue;
                }
            }
            auto& pq = ctx.pq_list()[best_pq];
            if (!pq.try_lock()) {
                continue;
            }
            auto current_top_key = pq.top_key();
            if ((!pop_stale && current_top_key != best_key) || Context::is_sentinel(current_top_key)) {
                // Top got empty (or changed) before locking
                pq.unlock();
                continue;
            }
            auto retval = pq.get_pq().top();
            pq.get_pq().pop();
            pq.update_top_key();
            pq.unlock();
            return retval;
        }
        return std::nullopt;
    }

    template <typename Context>
    void push(Context& ctx, typename Context::value_type const& v) {
        while (true) {
            auto& pq = ctx.pq_list()[rng() & (ctx.num_pqs() - 1)];
            if (pq.try_lock()) {
                pq.get_pq().push(v);
                pq.update_top_key();
                pq.unlock();
                return;
            }
        }
    }
};

}  // namespace multiqueue::operation_policy
