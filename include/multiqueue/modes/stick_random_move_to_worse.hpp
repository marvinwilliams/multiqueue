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
class StickRandomMTW {
    static_assert(num_pop_candidates == 2);

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
    explicit StickRandomMTW(Config const& config, SharedData& shared_data) noexcept {
        auto id = shared_data.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{config.seed, id};
        rng_.seed(seq);
    }

    template <typename Context>
    std::optional<typename Context::value_type> try_pop(Context& ctx) {
        if (count_ == 0) {
            refresh_pop_index(ctx.num_pqs());
            count_ = ctx.config().stickiness;
        }
        while (true) {
            typename Context::key_type keys[2] = {ctx.pq_guards()[pop_index_[0]].top_key(),
                                                  ctx.pq_guards()[pop_index_[1]].top_key()};
            auto best_pq = static_cast<std::size_t>(ctx.compare(keys[0], keys[1]));
            auto& guard = ctx.pq_guards()[pop_index_[best_pq]];
            if (!guard.try_lock()) {
                refresh_pop_index(ctx.num_pqs());
                count_ = ctx.config().stickiness;
                continue;
            }
            if (guard.get_pq().empty()) {
                guard.unlock();
                count_ = 0;
                return std::nullopt;
            }
            auto v = guard.get_pq().top();
            guard.get_pq().pop();
            if (!guard.get_pq().empty() && ctx.compare(keys[1 - best_pq], Context::get_key(guard.get_pq().top()))) {
                auto tmp = guard.get_pq().top();
                guard.get_pq().pop();
                guard.popped();
                guard.unlock();
                auto& guard_other = ctx.pq_guards()[pop_index_[1 - best_pq]];
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
            --count_;
            return v;
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
            if (guard.try_lock()) {
                guard.get_pq().push(v);
                guard.pushed();
                guard.unlock();
                --count_;
                return;
            }
            refresh_pop_index(ctx.num_pqs());
            count_ = ctx.config().stickiness;
        }
    }
};

}  // namespace multiqueue::mode
