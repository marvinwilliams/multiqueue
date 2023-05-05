#pragma once

#include "multiqueue/build_config.hpp"
#include "multiqueue/config.hpp"

#include "pcg_random.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <random>

namespace multiqueue {

template <typename Impl>
struct NoSticking {
    using key_type = typename Impl::key_type;
    using size_type = typename Impl::size_type;
    using reference = typename Impl::reference;
    using const_reference = typename Impl::const_reference;

    struct SharedData {
        explicit SharedData(size_type /*unused*/) {
        }
    };

    Impl &impl;
    pcg32 rng{};

    explicit NoSticking(int id, Impl &i) noexcept : impl{i} {
        auto seq = std::seed_seq{impl.config().seed, id};
        rng.seed(seq);
    }

    size_type random_pq_index() noexcept {
        return std::uniform_int_distribution<size_type>(0, impl.num_pqs() - 1)(rng);
    }

    void push(const_reference value) {
        auto idx = random_pq_index();
        while (impl.try_push(idx, value) == Impl::push_result::Locked) {
            idx = random_pq_index();
        }
    }

    template <std::size_t N = BuildConfiguration::DefaultNumCompare>
    bool try_pop(reference retval) {
        do {
            std::array<size_type, N> idx;
            std::generate(std::begin(idx), std::end(idx), [this]() { return random_pq_index(); });
            if constexpr (N == 2) {
                while (idx[0] == idx[1]) {
                    idx[1] = random_pq_index();
                }
            }
            auto result = impl.try_pop_compare(idx, retval);
            while (result == Impl::pop_result::Invalid) {
                // If the result is invalid, another thread accessed the pq.
                // However, since there is no sticking, just try again
                result = impl.try_pop_compare(idx, retval);
            }
            if (result == Impl::pop_result::Success) {
                return true;
            }
            if (result == Impl::pop_result::Empty) {
                return impl.try_pop_any(random_pq_index(), retval);
            }
        } while (true);
    }
};

}  // namespace multiqueue
