#pragma once

#include "multiqueue/config.hpp"

#include "pcg_random.hpp"

#include <array>
#include <atomic>
#include <cassert>
#include <random>

namespace multiqueue {

// This variant uses a global permutation defined by the parameters a and b, such that i*a + b mod p yields a
// number from [0,p-1] for i in [0,p-1] For this to be a permutation, a and b needs to be coprime. Each handle
// has a unique id, so that i in [3*id,3*id+2] identify the queues associated with this handle. The stickiness
// counter is global and can occasionally
template <typename Impl>
struct GlobalPermutation {
    using key_type = typename Impl::key_type;
    using size_type = typename Impl::size_type;
    using reference = typename Impl::reference;
    using const_reference = typename Impl::const_reference;

    static constexpr int Shift = 32;
    static constexpr std::uint64_t Mask = (1ULL << Shift) - 1;

    struct SharedData {
        alignas(BuildConfiguration::L1CacheLinesize) std::atomic_uint64_t permutation{1};

        explicit SharedData(size_type /* unused */) {
        }
    };

    Impl &impl;
    pcg32 rng{};
    size_type idx;
    std::array<size_type, 2> stick_index{};
    std::uint64_t permutation{0};
    int use_count{};
    std::uint8_t push_pq{};

    explicit GlobalPermutation(int id, Impl &i) noexcept : impl{i}, idx{static_cast<size_type>(id * 2)} {
        auto seq = std::seed_seq{impl.config().seed, id};
        rng.seed(seq);
    }

    size_type random_pq_index() noexcept {
        return std::uniform_int_distribution<size_type>(0, impl.num_pqs() - 1)(rng);
    }

    void update_permutation() noexcept {
        auto current_permutation = impl.shared_data().permutation.load(std::memory_order_relaxed);
        if (current_permutation == permutation) {
            return;
        }
        permutation = current_permutation;
        use_count = 2 * impl.config().stickiness;
        set_index();
    }

    void set_index() noexcept {
        size_type a = permutation & Mask;
        size_type b = (permutation >> Shift) & Mask;
        assert((a & 1) == 1);
        stick_index[0] = (idx * a + b) & (impl.num_pqs() - 1);
        stick_index[1] = ((idx + 1) * a + b) & (impl.num_pqs() - 1);
    }

    void refresh_permutation() {
        update_permutation();
        if (use_count <= 0) {
            std::uint64_t new_permutation{rng() | 1};  // lower half must be uneven
            if (impl.shared_data().permutation.compare_exchange_strong(permutation, new_permutation, std::memory_order_relaxed)) {
                permutation = new_permutation;
            }
            use_count = 2 * impl.config().stickiness;
            set_index();
        }
    }

   public:
    void push(const_reference value) {
        refresh_permutation();
        push_pq = 1 - push_pq;
        --use_count;
        if (impl.try_push(stick_index[push_pq], value) == Impl::push_result::Success) {
            return;
        }
        while (impl.try_push(random_pq_index(), value) != Impl::push_result::Success) {
        }
    }

    bool try_pop(reference retval) {
        refresh_permutation();
        use_count -= 2;
        auto i = stick_index;
        do {
            auto result = impl.try_pop_compare(i, retval);
            if (result == Impl::pop_result::Success) {
                return true;
            }
            if (result == Impl::pop_result::Empty) {
                break;
            }
            i[0] = random_pq_index();
            i[1] = random_pq_index();
        } while (true);
        return impl.try_pop_any(random_pq_index(), retval);
    }
};

}  // namespace multiqueue
