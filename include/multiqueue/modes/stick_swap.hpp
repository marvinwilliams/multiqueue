#pragma once

#include "multiqueue/build_config.hpp"

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
class StickSwap {
    static_assert(num_pop_candidates > 0);

   public:
    struct alignas(build_config::l1_cache_line_size) AlignedIndex {
        std::atomic<std::size_t> value;
    };

    using permutation_type = std::vector<AlignedIndex>;

    struct Config {
        int seed{1};
        int stickiness{16};
    };

    struct SharedData {
        permutation_type permutation;
        std::atomic_int id_count{0};

        explicit SharedData(std::size_t num_pqs) : permutation(num_pqs) {
            for (std::size_t i = 0; i < num_pqs; ++i) {
                permutation[i].value = i;
            }
        }
    };

   private:
    pcg32 rng_{};
    int stick_count_{};
    std::size_t offset_{};

    void swap_assignment(permutation_type& perm, std::size_t index) noexcept {
        static constexpr std::size_t swapping = std::numeric_limits<std::size_t>::max();
        assert(index < num_pop_candidates);
        std::size_t old_target = perm[offset_ + index].value.exchange(swapping, std::memory_order_relaxed);
        std::size_t perm_index{};
        std::size_t new_target{};
        do {
            perm_index = rng_() & (perm.size() - 1);
            new_target = perm[perm_index].value.load(std::memory_order_relaxed);
        } while (new_target == swapping ||
                 !perm[perm_index].value.compare_exchange_weak(new_target, old_target, std::memory_order_relaxed));
        perm[offset_ + index].value.store(new_target, std::memory_order_relaxed);
    }

    template <typename Context>
    std::size_t best_pop_index(Context const& ctx) noexcept {
        std::size_t best = ctx.shared_data().permutation[offset_].value.load(std::memory_order_relaxed);
        auto best_key = ctx.pq_guards()[best].top_key();
        for (std::size_t i = 1; i < static_cast<std::size_t>(num_pop_candidates); ++i) {
            std::size_t target = ctx.shared_data().permutation[offset_ + i].value.load(std::memory_order_relaxed);
            auto key = ctx.pq_guards()[target].top_key();
            if (ctx.compare(best_key, key)) {
                best = target;
                best_key = key;
            }
        }
        return best;
    }

   protected:
    explicit StickSwap(Config const& config, SharedData& shared_data) noexcept {
        auto id = shared_data.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{config.seed, id};
        rng_.seed(seq);
        offset_ = static_cast<std::size_t>(id * num_pop_candidates);
    }

    template <typename Context>
    std::optional<typename Context::value_type> try_pop(Context& ctx) {
        if (stick_count_ == 0) {
            for (std::size_t i = 0; i < static_cast<std::size_t>(num_pop_candidates); ++i) {
                swap_assignment(ctx.shared_data().permutation, i);
            }
            stick_count_ = ctx.config().stickiness;
        }
        while (true) {
            auto& guard = ctx.pq_guards()[best_pop_index(ctx)];
            if (guard.try_lock()) {
                if (guard.get_pq().empty()) {
                    guard.unlock();
                    stick_count_ = 0;
                    return std::nullopt;
                }
                auto v = guard.get_pq().top();
                guard.get_pq().pop();
                guard.popped();
                guard.unlock();
                --stick_count_;
                return v;
            }
            for (std::size_t i = 0; i < static_cast<std::size_t>(num_pop_candidates); ++i) {
                swap_assignment(ctx.shared_data().permutation, i);
            }
            stick_count_ = ctx.config().stickiness;
        }
    }

    template <typename Context>
    void push(Context& ctx, typename Context::value_type const& v) {
        if (stick_count_ == 0) {
            for (std::size_t i = 0; i < static_cast<std::size_t>(num_pop_candidates); ++i) {
                swap_assignment(ctx.shared_data().permutation, i);
            }
            stick_count_ = ctx.config().stickiness;
        }
        std::size_t push_index = rng_() % num_pop_candidates;
        while (true) {
            auto target = ctx.shared_data().permutation[offset_ + push_index].value.load(std::memory_order_relaxed);
            auto& guard = ctx.pq_guards()[target];
            if (guard.try_lock()) {
                guard.get_pq().push(v);
                guard.pushed();
                guard.unlock();
                --stick_count_;
                return;
            }
            for (std::size_t i = 0; i < static_cast<std::size_t>(num_pop_candidates); ++i) {
                swap_assignment(ctx.shared_data().permutation, i);
            }
            stick_count_ = ctx.config().stickiness;
        }
    }
};

}  // namespace multiqueue::mode
