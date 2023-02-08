#pragma once

#include "multiqueue/config.hpp"
#include "multiqueue/stick_policy.hpp"

#include "multiqueue/third_party/pcg_random.hpp"

#include <array>
#include <cassert>
#include <random>

namespace multiqueue {

template <typename Impl>
struct Random {
    using key_type = typename Impl::key_type;
    using size_type = typename Impl::size_type;
    using reference = typename Impl::reference;
    using const_reference = typename Impl::const_reference;

    struct shared_data {};

    Impl &impl;
    pcg32 rng;
    std::array<size_type, 2> stick_index{0, 0};
    int use_count{0};

    inline size_type random_pq_index() noexcept {
        return std::uniform_int_distribution<size_type>(0, impl.num_pqs - 1)(rng);
    }

    explicit Random(unsigned int id, Impl &impl) noexcept : impl{impl}, rng(std::seed_seq{impl.config.seed, id}) {
    }

    void push(const_reference value) {
        if (use_count != 0 && impl.try_push_compare(stick_index, value) == PushResult::Success) {
            --use_count;
            return;
        }
        do {
            stick_index[0] = random_pq_index(rng);
            stick_index[1] = random_pq_index(rng);
        } while (impl.try_push_compare(stick_index, value) == PushResult::Locked);
        use_count = impl.config().stickiness - 1;
    }

    bool try_pop(reference retval) {
        if (use_count != 0) {
            auto result = impl.try_pop_compare(stick_index, retval);
            if (result == PopResult::Success) {
                --use_count;
                return true;
            }
            if (result == PopResult::Empty) {
                use_count = 0;
                return impl.try_pop_any(random_pq_index(rng));
            }
        }
        do {
            stick_index[0] = random_pq_index(rng);
            stick_index[1] = random_pq_index(rng);
            auto result = impl.try_pop_compare(stick_index, retval);
            if (result == PopResult::Success) {
                --use_count;
                return true;
            }
            if (result == PopResult::Empty) {
                use_count = 0;
                return impl.try_pop_any(random_pq_index(rng));
            }
        } while (true);
    }
};

}  // namespace multiqueue
