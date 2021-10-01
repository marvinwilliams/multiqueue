/**
******************************************************************************
* @file:   random.hpp
*
* @author: Marvin Williams
* @date:   2021/09/27 10:15
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef SELECTION_STRATEGY_RANDOM_HPP_INCLUDED
#define SELECTION_STRATEGY_RANDOM_HPP_INCLUDED

#include "multiqueue/fastrange.h"
#include "multiqueue/xoroshiro128plus.hpp"

#include <cstdint>
#include <sstream>
#include <string>

namespace multiqueue {
namespace selection_strategy {

template <typename Multiqueue>
struct random {
    using key_type = typename Multiqueue::key_type;
    using size_type = typename Multiqueue::size_type;
    using spq_t = typename Multiqueue::spq_t;

    struct shared_data_t {};

    struct thread_data_t {
        xoroshiro128plus gen;

        explicit thread_data_t(Multiqueue&) {}

        void seed(std::uint64_t seed) noexcept {
            gen.seed(seed);
        }
    };

    inline static spq_t *get_locked_insert_spq(Multiqueue &mq, thread_data_t &thread_data) {
        spq_t *s;
        do {
            s = mq.spqs_ + fastrange64(thread_data.gen(), mq.num_spqs_);
        } while (!s->try_lock());
        return s;
    }

    inline static spq_t *get_locked_delete_spq(Multiqueue &mq, thread_data_t &thread_data) {
        spq_t *first;
        spq_t *second;
        key_type first_key;
        key_type second_key;

        do {
            first = mq.spqs_ + fastrange64(thread_data.gen(), mq.num_spqs_);
            second = mq.spqs_ + fastrange64(thread_data.gen(), mq.num_spqs_);
            first_key = first->get_min_key();
            second_key = second->get_min_key();

            if (first_key != Multiqueue::empty_key || second != Multiqueue::empty_key) {
                // empty_key is larger than all valid keys
                if (second_key < first_key) {
                    first = second;
                    first_key = second_key;
                }
                if (first->try_lock(first_key)) {
                    return first;
                }
            } else {
                break;
            }
        } while (true);
        return nullptr;
    }

    static std::string description(shared_data_t const &) {
        return "random";
    }
};

}  // namespace selection_strategy
}  // namespace multiqueue
#endif  //! SELECTION_STRATEGY_RANDOM_HPP_INCLUDED
