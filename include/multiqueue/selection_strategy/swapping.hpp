/**
******************************************************************************
* @file:   swapping.hpp
*
* @author: Marvin Williams
* @date:   2021/09/27 10:15
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef SELECTION_STRATEGY_SWAPPING_HPP_INCLUDED
#define SELECTION_STRATEGY_SWAPPING_HPP_INCLUDED

#include "multiqueue/external/fastrange.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

namespace multiqueue::selection_strategy {

struct swapping {
    struct Parameters {
        unsigned int stickiness = 64;
        static constexpr unsigned int c = 3;
    };

    template <typename MultiQueue>
    struct Strategy {
        using pq_type = typename MultiQueue::pq_type;

        struct handle_data_t {
            unsigned int id;
            unsigned int push_count = 0;
            unsigned int delete_count = 0;
            handle_data_t(unsigned int index) : id{index} {
            }
        };

        struct alignas(L1_CACHE_LINESIZE) Assignment {
            std::atomic_size_t index;
        };

        MultiQueue &mq;
        unsigned int stickiness;
        std::vector<Assignment> assignment;

        Strategy(MultiQueue &mq_ref, Parameters const &param)
            : mq{mq_ref}, stickiness{param.stickiness}, assignment(mq.num_pqs()) {
            assert(stickiness > 0);
            for (std::size_t i = 0; i < mq.num_pqs(); ++i) {
                assignment[i].index = i;
            }
        }

        std::string description() const {
            std::stringstream ss;
            ss << "swapping\n";
            ss << "\tStickiness: " << stickiness;
            return ss.str();
        }

        template <typename Generator>
        void swap_assignment(unsigned int pq, Generator &g) {
            // Cannot be invalid, only thread itself invalidates
            std::size_t old_index = assignment[pq].index.exchange(mq.num_pqs(), std::memory_order_relaxed);
            std::size_t other_pq = fastrange64(g(), mq.num_pqs());
            std::size_t other_index = assignment[other_pq].index.load(std::memory_order_relaxed);
            while (other_index == mq.num_pqs() ||
                   !assignment[other_pq].index.compare_exchange_strong(other_index, old_index,
                                                                       std::memory_order_relaxed)) {
                other_pq = fastrange64(g(), mq.num_pqs());
                other_index = assignment[other_pq].index.load(std::memory_order_relaxed);
            }
            assignment[pq].index.store(other_index, std::memory_order_relaxed);
        }

        template <typename Generator>
        pq_type *lock_push_pq(handle_data_t &handle_data, Generator &g) {
            if (handle_data.push_count == stickiness) {
                swap_assignment(handle_data.id * 3, g);
                handle_data.push_count = 0;
            }
            auto index = assignment[handle_data.id * 3].index.load(std::memory_order_relaxed);
            if (!mq.pq_list_[index].try_lock()) {
                do {
                    index = fastrange64(g(), mq.num_pqs());
                } while (!mq.pq_list_[index].try_lock());
            }
            ++handle_data.push_count;
            return &mq.pq_list_[index];
        }

        template <typename Generator>
        pq_type *lock_delete_pq(handle_data_t &handle_data, Generator &g) {
            if (handle_data.delete_count == stickiness) {
                swap_assignment(handle_data.id * 3 + 1, g);
                swap_assignment(handle_data.id * 3 + 2, g);
                handle_data.delete_count = 0;
            }
            auto first_index = assignment[3 * handle_data.id + 1].index.load(std::memory_order_relaxed);
            auto second_index = assignment[3 * handle_data.id + 2].index.load(std::memory_order_relaxed);
            auto first_key = mq.pq_list_[first_index].top_key();
            auto second_key = mq.pq_list_[second_index].top_key();
            do {
                if (mq.comp_(first_key, second_key)) {
                    if (first_key == mq.get_sentinel()) {
                        break;
                    }
                    if (mq.pq_list_[first_index].try_lock_assume_key(first_key)) {
                        ++handle_data.delete_count;
                        return &mq.pq_list_[first_index];
                    }
                } else {
                    if (second_key == mq.get_sentinel()) {
                        break;
                    }
                    if (mq.pq_list_[second_index].try_lock_assume_key(second_key)) {
                        ++handle_data.delete_count;
                        return &mq.pq_list_[second_index];
                    }
                }
                first_index = fastrange64(g(), mq.num_pqs());
                second_index = fastrange64(g(), mq.num_pqs());
                first_key = mq.pq_list_[first_index].top_key();
                second_key = mq.pq_list_[second_index].top_key();
            } while (true);
            handle_data.delete_count = stickiness;
            return nullptr;
        }
    };
};

}  // namespace multiqueue::selection_strategy

#endif  //! SELECTION_STRATEGY_SWAPPING_HPP_INCLUDED
