#pragma once

#include "multiqueue/build_config.hpp"
#include "multiqueue/config.hpp"

#include "pcg_random.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <random>

namespace multiqueue::queue_selection {

// This variant uses a global permutation defined by the parameters a and b, such that i*a + b mod p yields a
// number from [0,p-1] for i in [0,p-1] For this to be a permutation, a and b needs to be coprime. Each handle
// has a unique id, so that i in [3*id,3*id+2] identify the queues associated with this handle. The stickiness
// counter is global and can occasionally
class GlobalPermutation {
    static constexpr int Shift = 32;
    static constexpr std::uint64_t Mask = (1ULL << Shift) - 1;

   protected:
    struct SharedData {
        std::atomic_uint64_t id_count{0};
        alignas(build_config::L1CacheLinesize) std::atomic_uint64_t permutation{1};

        explicit SharedData(std::size_t /*num_pqs*/) : permutation(1) {
        }
    };

   public:
    struct Config {
        std::uint64_t seed{1};
        int stickiness{16};
    };

   private:
    pcg32 rng{};
    std::uniform_int_distribution<std::size_t> pq_dist;
    std::geometric_distribution<int> stick_dist;
    int use_count{};
    int push_pq{};
    std::atomic_uint64_t& permutation;
    std::uint64_t local_permutation;
    std::uint64_t index{};
    bool use_random_push_pq{false};
    bool use_random_pop_pqs{false};

    void reset_permutation() noexcept {
        std::uint64_t new_permutation{rng() | 1};  // lower half must be uneven
        if (permutation.compare_exchange_strong(local_permutation, new_permutation, std::memory_order_relaxed)) {
            local_permutation = new_permutation;
        }
        use_count = stick_dist(rng);
        use_random_push_pq = false;
        use_random_pop_pqs = false;
    }

    std::size_t get_index(int pq) const noexcept {
        std::size_t a = local_permutation & Mask;
        std::size_t b = (local_permutation >> Shift) & Mask;
        assert((a & 1) == 1);
        return ((index + pq) * a + b) & (pq_dist.max());
    }

    void refresh_permutation() noexcept {
        auto const p = permutation.load(std::memory_order_relaxed);
        if (p != local_permutation) {
            local_permutation = p;
            use_count = stick_dist(rng);
            use_random_push_pq = false;
            use_random_pop_pqs = false;
        }
    }

   protected:
    explicit GlobalPermutation(std::size_t num_pqs, Config const& c, SharedData& sd) noexcept
        : pq_dist(0, num_pqs - 1),
          stick_dist(0.5 / c.stickiness),
          permutation{sd.permutation},
          local_permutation{sd.permutation.load(std::memory_order_relaxed)},
          index(sd.id_count.fetch_add(1, std::memory_order_relaxed)) {
        assert((num_pqs & num_pqs - 1) == 0);
        auto seq = std::seed_seq{c.seed, index};
        rng.seed(seq);
        use_count = stick_dist(rng);
    }

    std::size_t get_push_pq() noexcept {
        if (use_random_push_pq) {
            return pq_dist(rng);
        }
        refresh_permutation();
        return get_index(push_pq);
    }

    void reset_push_pq() noexcept {
        use_random_push_pq = true;
    }

    void use_push_pq() noexcept {
        use_random_push_pq = false;
        if (use_count <= 0) {
            reset_permutation();
        } else {
            --use_count;
        }
        push_pq = 1 - push_pq;
    }

    auto get_pop_pqs() noexcept {
        if (use_random_pop_pqs) {
            return std::array{pq_dist(rng), pq_dist(rng)};
        }
        refresh_permutation();
        return std::array{get_index(0), get_index(1)};
    }

    void reset_pop_pqs() noexcept {
        use_random_pop_pqs = true;
    }

    void use_pop_pqs() noexcept {
        use_random_pop_pqs = false;
        if (use_count <= 0) {
            reset_permutation();
        } else {
            use_count -= 2;
        }
    }
};

}  // namespace multiqueue::queue_selection
