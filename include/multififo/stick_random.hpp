#pragma once

#include "pcg_random.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <optional>
#include <random>

namespace multififo::mode {

template <int num_pop_candidates = 2>
class StickRandom {
    static_assert(num_pop_candidates > 0);

   private:
    pcg32 rng_{};
    std::array<std::size_t, static_cast<std::size_t>(num_pop_candidates)> pop_index_{};
    int count_{};

    void refresh_pop_index(std::size_t num_queues) noexcept {
        for (auto it = pop_index_.begin(); it != pop_index_.end(); ++it) {
            do {
                *it = std::uniform_int_distribution<std::size_t>{0, num_queues - 1}(rng_);
            } while (std::find(pop_index_.begin(), it, *it) != it);
        }
    }

   protected:
    explicit StickRandom(int seed, int id) noexcept {
        auto seq = std::seed_seq{seed, id};
        rng_.seed(seq);
    }

    template <typename Context>
    std::optional<typename Context::value_type> try_pop(Context& ctx) {
        if (count_ == 0) {
            refresh_pop_index(ctx.num_queues());
            count_ = ctx.stickiness();
        }
        while (true) {
            std::size_t best = pop_index_[0];
            auto best_tick = ctx.queue_guards()[best].top_tick();
            for (std::size_t i = 1; i < static_cast<std::size_t>(num_pop_candidates); ++i) {
                auto tick = ctx.queue_guards()[pop_index_[i]].top_tick();
                if (tick < best_tick) {
                    best = pop_index_[i];
                    best_tick = tick;
                }
            }
            auto& guard = ctx.queue_guards()[best];
            if (guard.try_lock()) {
                if (guard.get_queue().empty()) {
                    guard.unlock();
                    count_ = 0;
                    return std::nullopt;
                }
                auto v = guard.get_queue().top().value;
                guard.get_queue().pop();
                guard.popped();
                guard.unlock();
                --count_;
                return v;
            }
            refresh_pop_index(ctx.num_queues());
            count_ = ctx.stickiness();
        }
    }

    template <typename Context>
    bool try_push(Context& ctx, typename Context::value_type const& v) {
        if (count_ == 0) {
            refresh_pop_index(ctx.num_queues());
            count_ = ctx.stickiness();
        }
        std::size_t push_index = rng_() % num_pop_candidates;
        while (true) {
            auto& guard = ctx.queue_guards()[pop_index_[push_index]];
            if (guard.try_lock()) {
                if (guard.get_queue().full()) {
                    guard.unlock();
                    count_ = 0;
                    return false;
                }
                auto tick = static_cast<std::uint64_t>(Context::clock_type::now().time_since_epoch().count());
                guard.get_queue().push({tick, v});
                guard.pushed();
                guard.unlock();
                --count_;
                return true;
            }
            refresh_pop_index(ctx.num_queues());
            count_ = ctx.stickiness();
        }
    }
};

}  // namespace multififo::mode
