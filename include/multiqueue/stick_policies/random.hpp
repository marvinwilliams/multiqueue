#pragma once

#include "multiqueue/config.hpp"

#include "pcg_random.hpp"

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

    struct SharedData {
        explicit SharedData(size_type /*unused*/) {
        }
    };

    Impl &impl;
    pcg32 rng{};
    std::array<size_type, 2> stick_index{};
    std::array<int, 2> use_count{};
    std::uint8_t push_pq{};

    explicit Random(int id, Impl &i) noexcept : impl{i} {
        auto seq = std::seed_seq{impl.config().seed, id};
        rng.seed(seq);
    }

    size_type random_pq_index() noexcept {
        return std::uniform_int_distribution<size_type>(0, impl.num_pqs() - 1)(rng);
    }

    void push(const_reference value) {
        push_pq = 1 - push_pq;
        if (use_count[push_pq] != 0) {
            if (impl.try_push(stick_index[push_pq], value) == Impl::push_result::Success) {
                --use_count[push_pq];
                return;
            }
        }
        do {
            stick_index[push_pq] = random_pq_index();
        } while (impl.try_push(stick_index[push_pq], value) != Impl::push_result::Success);
        use_count[push_pq] = impl.config().stickiness - 1;
    }

    bool try_pop(reference retval) {
        if (use_count[0] == 0) {
            stick_index[0] = random_pq_index();
            use_count[0] = impl.config().stickiness;
        }
        if (use_count[1] == 0) {
            stick_index[1] = random_pq_index();
            use_count[1] = impl.config().stickiness;
        }
        do {
            auto result = impl.try_pop_compare(stick_index, retval);
            if (result == Impl::pop_result::Success) {
                --use_count[0];
                --use_count[1];
                return true;
            }
            if (result == Impl::pop_result::Empty) {
                use_count[0] = 0;
                use_count[1] = 0;
                return impl.try_pop_any(random_pq_index(), retval);
            }
            stick_index[0] = random_pq_index();
            stick_index[1] = random_pq_index();
            use_count[0] = impl.config().stickiness;
            use_count[1] = impl.config().stickiness;
        } while (true);
    }
};

}  // namespace multiqueue
