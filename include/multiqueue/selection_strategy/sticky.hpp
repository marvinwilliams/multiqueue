/**
******************************************************************************
* @file:   sticky.hpp
*
* @author: Marvin Williams
* @date:   2021/09/27 10:15
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef SELECTION_STRATEGY_STICKY_HPP_INCLUDED
#define SELECTION_STRATEGY_STICKY_HPP_INCLUDED

#include "multiqueue/external/fastrange.h"

#include <cstdint>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace multiqueue::selection_strategy {

struct sticky {
    struct Parameters {
        unsigned int stickiness = 4;
    };

    template <typename MultiQueue>
    struct Strategy {
        using pq_type = typename MultiQueue::pq_type;

        struct handle_data_t {
            unsigned int push_count = 0;
            unsigned int delete_count[2] = {0, 0};
            std::size_t push_index;
            std::size_t delete_index[2];
        };

        MultiQueue &mq;
        unsigned int stickiness;

        Strategy(MultiQueue &mq_ref, Parameters const &param) : mq{mq_ref}, stickiness{param.stickiness} {
            if (stickiness == 0) {
                throw std::invalid_argument("stickiness cannot be 0");
            }
        }

        std::string description() const {
            std::stringstream ss;
            ss << "sticky\n";
            ss << "\tStickiness: " << stickiness;
            return ss.str();
        }

        template <typename Generator>
        pq_type *lock_push_pq(handle_data_t &handle_data, Generator &g) {
            if (handle_data.push_count == 0) {
                handle_data.push_index = fastrange64(g(), mq.num_pqs());
                handle_data.push_count = stickiness;
            }

            if (!mq.pq_list_[handle_data.push_index].try_lock()) {
                handle_data.push_count = stickiness;
                do {
                    handle_data.push_index = fastrange64(g(), mq.num_pqs());
                } while (!mq.pq_list_[handle_data.push_index].try_lock());
            }
            --handle_data.push_count;
            return &mq.pq_list_[handle_data.push_index];
        }

        template <typename Generator>
        pq_type *lock_delete_pq(handle_data_t &handle_data, Generator &g) {
            if (handle_data.delete_count[0] == 0) {
                handle_data.delete_index[0] = fastrange64(g(), mq.num_pqs());
                handle_data.delete_count[0] = stickiness;
            }
            if (handle_data.delete_count[1] == 0) {
                handle_data.delete_index[1] = fastrange64(g(), mq.num_pqs());
                handle_data.delete_count[1] = stickiness;
            }
            auto first_key = mq.pq_list_[handle_data.delete_index[0]].top_key();
            auto second_key = mq.pq_list_[handle_data.delete_index[1]].top_key();
            do {
                if (mq.comp_(first_key, second_key)) {
                    if (first_key == mq.get_sentinel()) {
                        break;
                    }
                    if (mq.pq_list_[handle_data.delete_index[0]].try_lock_assume_key(first_key)) {
                        --handle_data.delete_count[0];
                        --handle_data.delete_count[1];
                        return &mq.pq_list_[handle_data.delete_index[0]];
                    }
                    handle_data.delete_index[0] = fastrange64(g(), mq.num_pqs());
                    first_key = mq.pq_list_[handle_data.delete_index[0]].top_key();
                    handle_data.delete_count[0] = stickiness;
                } else {
                    if (second_key == mq.get_sentinel()) {
                        break;
                    }
                    if (mq.pq_list_[handle_data.delete_index[1]].try_lock_assume_key(second_key)) {
                        --handle_data.delete_count[0];
                        --handle_data.delete_count[1];
                        return &mq.pq_list_[handle_data.delete_index[1]];
                    }
                    handle_data.delete_index[1] = fastrange64(g(), mq.num_pqs());
                    second_key = mq.pq_list_[handle_data.delete_index[1]].top_key();
                    handle_data.delete_count[1] = stickiness;
                }
            } while (true);
            handle_data.delete_count[0] = 0;
            handle_data.delete_count[1] = 0;
            return nullptr;
        }
    };
};

}  // namespace multiqueue::selection_strategy

#endif  //! SELECTION_STRATEGY_STICKY_HPP_INCLUDED
