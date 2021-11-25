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

#include "multiqueue/external/fastrange.h"
#include "multiqueue/external/xoroshiro128plus.hpp"

#include <cstdint>
#include <sstream>
#include <string>

namespace multiqueue {
namespace selection_strategy {

struct random {
    struct shared_data_t {
        template <typename Configuration>
        shared_data_t(Configuration const &) {
        }

        std::string description(shared_data_t const &) const {
            return "random";
        }
    };

    struct thread_data_t {
        xoroshiro128plus gen;

        explicit thread_data_t(std::uint64_t seed) : gen(seed) {
        }
    };

    template <typename Multiqueue>
    static inline auto lock_push_pq(Multiqueue &mq, thread_data_t &thread_data) {
        auto pq = mq.pq_list_ + fastrange64(thread_data.gen(), mq.num_pqs_);
        while (!pq->try_lock()) {
            pq = mq.pq_list_ + fastrange64(thread_data.gen(), mq.num_pqs_);
        }
        return pq;
    }

    template <typename Multiqueue>
    static inline auto lock_delete_pq(Multiqueue &mq, thread_data_t &thread_data) -> typename Multiqueue::pq_type * {
        auto first = mq.pq_list_ + fastrange64(thread_data.gen(), mq.num_pqs_);
        auto second = mq.pq_list_ + fastrange64(thread_data.gen(), mq.num_pqs_);
        auto first_key = first->concurrent_top_key();
        auto second_key = second->concurrent_top_key();
        do {
            if (first_key != Multiqueue::sentinel &&
                (second_key == Multiqueue::sentinel || mq.comp_(first_key, second_key))) {
                if (first->try_lock_if_key(first_key)) {
                    return first;
                } else {
                    first = mq.pq_list_ + fastrange64(thread_data.gen(), mq.num_pqs_);
                    first_key = first->concurrent_top_key();
                }
            } else if (second_key != Multiqueue::sentinel) {
                if (second->try_lock_if_key(second_key)) {
                    return second;
                } else {
                    second = mq.pq_list_ + fastrange64(thread_data.gen(), mq.num_pqs_);
                    second_key = second->concurrent_top_key();
                }
            } else {
                // Both keys are sentinels
                break;
            }
        } while (true);
        return nullptr;
    }
};

}  // namespace selection_strategy
}  // namespace multiqueue
#endif  //! SELECTION_STRATEGY_RANDOM_HPP_INCLUDED
