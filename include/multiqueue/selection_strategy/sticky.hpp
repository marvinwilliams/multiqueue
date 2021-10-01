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

#include "multiqueue/fastrange.h"
#include "multiqueue/xoroshiro128plus.hpp"

#include <cstdint>
#include <sstream>
#include <string>
#include <utility>

namespace multiqueue {
namespace selection_strategy {

template <typename Multiqueue>
struct sticky {
    using key_type = typename Multiqueue::key_type;
    using size_type = typename Multiqueue::size_type;
    using spq_t = typename Multiqueue::spq_t;

    struct shared_data_t {
        unsigned int stickiness;
    };

    struct thread_data_t {
        xoroshiro128plus gen;
        unsigned int insert_count = 0;
        unsigned int delete_count = 0;
        spq_t *insert_spq;
        spq_t *delete_spq[2];

        explicit thread_data_t(Multiqueue&) {}

        void seed(std::uint64_t seed) noexcept {
            gen.seed(seed);
        }
    };

    inline static spq_t *get_locked_insert_spq(Multiqueue &mq, thread_data_t &thread_data) {
        if (thread_data.insert_count > 0 && thread_data.insert_spq->try_lock()) {
            --thread_data.insert_count;
            return thread_data.insert_spq;
        }
        do {
            thread_data.insert_spq = mq.spqs_ + fastrange64(thread_data.gen(), mq.num_spqs_);
        } while (!thread_data.insert_spq->try_lock());
        thread_data.insert_count = mq.selection_strategy_data_.stickiness - 1;

        return thread_data.insert_spq;
    }

    inline static spq_t *get_locked_delete_spq(Multiqueue &mq, thread_data_t &thread_data) {
        key_type first_key;
        key_type second_key;
        if (thread_data.delete_count > 0) {
            first_key = thread_data.delete_spq[0]->get_min_key();
            second_key = thread_data.delete_spq[1]->get_min_key();
            if (first_key != Multiqueue::empty_key || second_key != Multiqueue::empty_key) {
                if (second_key < first_key) {
                    std::swap(first_key, second_key);
                    std::swap(thread_data.delete_spq[0], thread_data.delete_spq[1]);
                }
                // Now the min element are in first position and can't be empty
                if (thread_data.delete_spq[0]->try_lock(first_key)) {
                    --thread_data.delete_count;
                    if (second_key == Multiqueue::empty_key && thread_data.delete_count > 0) {
                        thread_data.delete_spq[1] = mq.spqs_ + fastrange64(thread_data.gen(), mq.num_spqs_);
                    }
                    return thread_data.delete_spq[0];
                }
            } else {
                thread_data.delete_count = 0;
                return nullptr;
            }
        }
        // If we get here, either the count is 0 or locking failed, so choose new SPQs randomly
        do {
            thread_data.delete_spq[0] = mq.spqs_ + fastrange64(thread_data.gen(), mq.num_spqs_);
            thread_data.delete_spq[1] = mq.spqs_ + fastrange64(thread_data.gen(), mq.num_spqs_);
            first_key = thread_data.delete_spq[0]->get_min_key();
            second_key = thread_data.delete_spq[1]->get_min_key();
            if (first_key != Multiqueue::empty_key || second_key != Multiqueue::empty_key) {
                if (second_key < first_key) {
                    std::swap(first_key, second_key);
                    std::swap(thread_data.delete_spq[0], thread_data.delete_spq[1]);
                }
                if (thread_data.delete_spq[0]->try_lock(first_key)) {
                    thread_data.delete_count = mq.selection_strategy_data_.stickiness - 1;
                    if (second_key == Multiqueue::empty_key) {
                        thread_data.delete_spq[1] = mq.spqs_ + fastrange64(thread_data.gen(), mq.num_spqs_);
                    }
                    return thread_data.delete_spq[0];
                }
            } else {
                break;
            }
        } while (true);
        thread_data.delete_count = 0;
        return nullptr;
    }

    static std::string description(shared_data_t const &data) {
        std::stringstream ss;
        ss << "sticky\n";
        ss << "\tStickiness: " << data.stickiness << '\n';
        return ss.str();
    }
};

}  // namespace selection_strategy
}  // namespace multiqueue
#endif  //! SELECTION_STRATEGY_STICKY_HPP_INCLUDED
