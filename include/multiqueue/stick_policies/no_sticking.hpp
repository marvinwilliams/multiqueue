#pragma once

#include "multiqueue/config.hpp"

#include "pcg_random.hpp"

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
        explicit SharedData(unsigned int /*unused*/) {
        }
    };

    Impl &impl;
    pcg32 rng;

    inline size_type random_pq_index() noexcept {
        return std::uniform_int_distribution<size_type>(0, impl.num_pqs() - 1)(rng);
    }

    explicit NoSticking(unsigned int id, Impl &i) noexcept : impl{i} {
        auto seq = std::seed_seq{impl.config().seed, id};
        rng.seed(seq);
    }

    void push(const_reference value) {
        std::array<size_type, 2> idx{random_pq_index(), random_pq_index()};
        while (impl.try_push_compare(idx, value) == Impl::push_result::Locked) {
            idx[0] = random_pq_index();
            idx[1] = random_pq_index();
        }
    }

    bool try_pop(reference retval) {
        std::array<size_type, 2> idx;
        do {
            idx[0] = random_pq_index();
            do {
                idx[1] = random_pq_index();
            } while (idx[0] == idx[1]);
            auto result = impl.try_pop_compare(idx, retval);
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
