#pragma once

#include "build_config.hpp"

#include "pcg_random.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <random>

namespace multiqueue::stick_policy {

template <unsigned NumPopPQs = 2>
class Swap {
    struct alignas(build_config::L1CacheLinesize) AlignedIndex {
        std::atomic<std::size_t> value;
    };
    using Permutation = std::vector<AlignedIndex>;

    pcg32 rng{};
    std::int32_t pq_mask{0};
    std::geometric_distribution<int> stick_dist;
    std::array<std::size_t, NumPopPQs> stick_index{};
    int use_count{};
    Permutation& permutation;
    std::size_t offset{};

    void refresh_pq(unsigned index) noexcept {
        auto i = permutation[offset + index].value.load(std::memory_order_relaxed);
        if (i != stick_index[index]) {
            stick_index[index] = i;
        }
    }

   public:
    struct Config {
        int seed{1};
        int stickiness{16};
    };

   protected:
    struct SharedData {
        std::atomic_int id_count{0};

        Permutation permutation;

        explicit SharedData(std::size_t num_pqs) : permutation(num_pqs) {
            for (std::size_t i = 0; i < num_pqs; ++i) {
                permutation[i].value = i;
            }
        }
    };

    explicit Swap(std::size_t num_pqs, Config const& c, SharedData& sd) noexcept
        : pq_mask((assert(num_pqs > 0 && (num_pqs & (num_pqs - 1)) == 0), static_cast<std::int32_t>(num_pqs) - 1)),
          stick_dist(1.0 / c.stickiness),
          permutation{sd.permutation} {
        auto id = sd.id_count.fetch_add(1, std::memory_order_relaxed);
        auto seq = std::seed_seq{c.seed, id};
        rng.seed(seq);
        offset = static_cast<std::size_t>(NumPopPQs * id);
        for (unsigned i = 0; i < NumPopPQs; ++i) {
            stick_index[i] = permutation[offset + i].value.load(std::memory_order_relaxed);
        }
        use_count = stick_dist(rng);
    }

    auto const& get_pq_indices() noexcept {
        if (use_count <= 0) {
            for (unsigned i = 0; i < NumPopPQs; ++i) {
                replace_pq(i);
            }
            use_count = stick_dist(rng);
        } else {
            for (unsigned i = 0; i < NumPopPQs; ++i) {
                refresh_pq(i);
            }
        }
        return stick_index;
    }

    void replace_pq(std::size_t index) noexcept {
        static constexpr std::size_t swapping = std::numeric_limits<std::size_t>::max();
        assert(index <= NumPopPQs);
        if (!permutation[offset + index].value.compare_exchange_strong(stick_index[index], swapping,
                                                                       std::memory_order_relaxed)) {
            // Permutation has changed, no need to swap
            // Only we may set ourself to swapping
            return;
        }
        std::size_t target_index;     // NOLINT(cppcoreguidelines-init-variables)
        std::size_t target_assigned;  // NOLINT(cppcoreguidelines-init-variables)
        do {
            target_index = rng() & pq_mask;
            target_assigned = permutation[target_index].value.load(std::memory_order_relaxed);
        } while (target_assigned == swapping ||
                 !permutation[target_index].value.compare_exchange_weak(target_assigned, stick_index[index],
                                                                        std::memory_order_relaxed));
        permutation[offset + index].value.store(target_assigned, std::memory_order_relaxed);
        stick_index[index] = target_assigned;
    }

    void reset_pqs() noexcept {
        use_count = 0;
    }

    void used_pqs() noexcept {
        --use_count;
    }
};

}  // namespace multiqueue::stick_policy
