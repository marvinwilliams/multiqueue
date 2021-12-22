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

#include <cstdint>
#include <sstream>
#include <string>

namespace multiqueue::selection_strategy {

struct random {
    struct Parameters {
    };

    template <typename MultiQueue>
    struct Strategy {
        using pq_type = typename MultiQueue::pq_type;

        struct handle_data_t {
            handle_data_t(unsigned int) {
            }
        };

        MultiQueue &mq;

        Strategy(MultiQueue &mq_ref, Parameters const &) noexcept : mq{mq_ref} {
        }

        static std::string description() {
            return "random";
        }

        template <typename Generator>
        pq_type *lock_push_pq(handle_data_t &, Generator &g) {
            auto pq = mq.pq_list_.get() + fastrange64(g(), mq.num_pqs());
            while (!pq->try_lock()) {
                pq = mq.pq_list_.get() + fastrange64(g(), mq.num_pqs());
            }
            return pq;
        }

        template <typename Generator>
        pq_type *lock_delete_pq(handle_data_t &, Generator &g) {
            auto first = mq.pq_list_.get() + fastrange64(g(), mq.num_pqs());
            auto second = mq.pq_list_.get() + fastrange64(g(), mq.num_pqs());
            auto first_key = first->top_key();
            auto second_key = second->top_key();
            do {
                if (mq.comp_(first_key, second_key)) {
                    if (first_key == mq.get_sentinel()) {
                        break;
                    }
                    if (first->try_lock_assume_key(first_key)) {
                        return first;
                    }
                    first = mq.pq_list_.get() + fastrange64(g(), mq.num_pqs());
                    first_key = first->top_key();
                } else {
                    if (second_key == mq.get_sentinel()) {
                        break;
                    }
                    if (second->try_lock_assume_key(second_key)) {
                        return second;
                    }
                    second = mq.pq_list_.get() + fastrange64(g(), mq.num_pqs());
                    second_key = second->top_key();
                }
            } while (true);
            return nullptr;
        }
    };
};

}  // namespace multiqueue::selection_strategy

#endif  //! SELECTION_STRATEGY_RANDOM_HPP_INCLUDED
