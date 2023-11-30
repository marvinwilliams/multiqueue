#pragma once

#include "multiqueue/third_party/pcg/pcg_random.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <optional>
#include <random>

namespace multiqueue::operation_policy {

template <int NumPopCandidates = 2, int NumPopTries = 1, bool PopStale = true>
class Random {
    pcg32 rng{};

    std::array<std::size_t, static_cast<std::size_t>(NumPopCandidates)> generate_indices(std::size_t num_pqs) noexcept {
        std::array<std::size_t, static_cast<std::size_t>(NumPopCandidates)> indices{};
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
        while (tries < NumPopTries) {
            auto indices = generate_indices(ctx.num_pqs());
            auto best_pq = indices[0];
            auto best_key = ctx.pq_list()[best_pq].top_key();
            if constexpr (NumPopCandidates == 2) {
                auto key = ctx.pq_list()[indices[1]].top_key();
                if (ctx.compare(best_key, key)) {
                    best_pq = indices[1];
                    best_key = key;
                } else if (Context::is_sentinel(best_key)) {
                    ++tries;
                    continue;
                }
            } else {
                for (int i = 1; i < NumPopCandidates; ++i) {
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
            assert((Context::is_sentinel(current_top_key && pq.get_pq().empty())) ||
                   (current_top_key == pq.get_pq().top()));
            if ((!PopStale && current_top_key != best_key) || Context::is_sentinel(current_top_key)) {
                // Top got empty (or changed) before locking
                pq.unlock();
                continue;
            }
            assert((current_top_key == pq.get_pq().top()));
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
                if (ctx.compare(pq.top_key(), Context::get_key(v))) {
                    pq.update_top_key();
                }
                pq.unlock();
                return;
            }
        }
    }
};

}  // namespace multiqueue::operation_policy
