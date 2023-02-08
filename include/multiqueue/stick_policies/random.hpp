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
        explicit SharedData(unsigned int /*unused*/) {
        }
    };

    Impl &impl;
    pcg32 rng;
    std::array<size_type, 2> stick_index{0, 0};
    int use_count{0};

    inline size_type random_pq_index() noexcept {
        return std::uniform_int_distribution<size_type>(0, impl.num_pqs() - 1)(rng);
    }

    explicit Random(unsigned int id, Impl &i) noexcept : impl{i}, rng(std::seed_seq{impl.config().seed, id}) {
    }

    void push(const_reference value) {
        if (use_count != 0 && impl.try_push_compare(stick_index, value) == Impl::push_result::Success) {
            --use_count;
            return;
        }
        do {
            stick_index[0] = random_pq_index();
            stick_index[1] = random_pq_index();
        } while (impl.try_push_compare(stick_index, value) == Impl::push_result::Locked);
        use_count = impl.config().stickiness - 1;
    }

    bool try_pop(reference retval) {
        if (use_count != 0) {
            auto result = impl.try_pop_compare(stick_index, retval);
            if (result == Impl::pop_result::Success) {
                --use_count;
                return true;
            }
            if (result == Impl::pop_result::Empty) {
                use_count = 0;
                return impl.try_pop_any(random_pq_index(), retval);
            }
        }
        do {
            stick_index[0] = random_pq_index();
            stick_index[1] = random_pq_index();
            auto result = impl.try_pop_compare(stick_index, retval);
            if (result == Impl::pop_result::Success) {
                --use_count;
                return true;
            }
            if (result == Impl::pop_result::Empty) {
                use_count = 0;
                return impl.try_pop_any(random_pq_index(), retval);
            }
        } while (true);
    }
};

}  // namespace multiqueue
