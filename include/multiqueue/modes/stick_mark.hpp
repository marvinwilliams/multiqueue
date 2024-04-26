#pragma once

#include "pcg_random.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <optional>
#include <random>

namespace multiqueue::mode {

template <int num_pop_candidates = 2>
class StickMark {
    static_assert(num_pop_candidates > 0);

   public:
    struct Config {
        int seed{1};
        int stickiness{16};
    };

    struct SharedData {
        std::atomic_uint id_count{0};

        explicit SharedData(std::size_t /*num_pqs*/) noexcept {
        }
    };

   private:
    std::uint32_t id_{};
    pcg32 rng_{};
    std::array<std::size_t, static_cast<std::size_t>(num_pop_candidates)> pop_index_{};
    int count_{};

    void refresh_pop_index(std::size_t num_pqs) noexcept {
        for (auto it = pop_index_.begin(); it != pop_index_.end(); ++it) {
            do {
                *it = std::uniform_int_distribution<std::size_t>{0, num_pqs - 1}(rng_);
            } while (std::find(pop_index_.begin(), it, *it) != it);
        }
    }

   protected:
    explicit StickMark(Config const& config, SharedData& shared_data) noexcept
        : id_(shared_data.id_count.fetch_add(1, std::memory_order_relaxed)) {
        auto seq = std::seed_seq{config.seed, id_};
        rng_.seed(seq);
    }

    template <typename Context>
    std::optional<typename Context::value_type> try_pop(Context& ctx) {
        if (count_ == 0) {
            refresh_pop_index(ctx.num_pqs());
            count_ = ctx.config().stickiness;
        }
        while (true) {
            std::size_t best = pop_index_[0];
            auto best_key = ctx.pq_guards()[best].top_key();
            for (std::size_t i = 1; i < static_cast<std::size_t>(num_pop_candidates); ++i) {
                auto key = ctx.pq_guards()[pop_index_[i]].top_key();
                if (ctx.compare(best_key, key)) {
                    best = pop_index_[i];
                    best_key = key;
                }
            }
            auto& guard = ctx.pq_guards()[best];
            if (guard.try_lock(count_ == ctx.config().stickiness, id_)) {
                if (guard.get_pq().empty()) {
                    guard.unlock(id_);
                    count_ = 0;
                    return std::nullopt;
                }
                auto v = guard.get_pq().top();
                guard.get_pq().pop();
                guard.popped();
                guard.unlock(id_);
                --count_;
                return v;
            }
            refresh_pop_index(ctx.num_pqs());
            count_ = ctx.config().stickiness;
        }
    }

    template <typename Context>
    void push(Context& ctx, typename Context::value_type const& v) {
        if (count_ == 0) {
            refresh_pop_index(ctx.num_pqs());
            count_ = ctx.config().stickiness;
        }
        std::size_t push_index = rng_() % num_pop_candidates;
        while (true) {
            auto& guard = ctx.pq_guards()[pop_index_[push_index]];
            if (guard.try_lock(count_ == ctx.config().stickiness, id_)) {
                guard.get_pq().push(v);
                guard.pushed();
                guard.unlock(id_);
                --count_;
                return;
            }
            refresh_pop_index(ctx.num_pqs());
            count_ = ctx.config().stickiness;
        }
    }
};

}  // namespace multiqueue::mode
