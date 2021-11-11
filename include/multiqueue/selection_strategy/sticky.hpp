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
#include "multiqueue/external/xoroshiro128plus.hpp"

#include <cstdint>
#include <sstream>
#include <string>
#include <utility>

namespace multiqueue {
namespace selection_strategy {

struct sticky {
    struct shared_data_t {
        unsigned int stickiness;
        template <typename Configuration>
        shared_data_t(Configuration const &config) : stickiness{config.stickiness} {
        }

        std::string description() const {
            std::stringstream ss;
            ss << "sticky\n";
            ss << "\tStickiness: " << stickiness << '\n';
            return ss.str();
        }
    };

    struct thread_data_t {
        xoroshiro128plus gen;
        unsigned int push_count = 0;
        unsigned int delete_count[2] = {0, 0};
        void *push_pq;
        void *delete_pq[2];

        explicit thread_data_t(std::uint64_t seed) noexcept : gen(seed) {
        }
    };

    template <typename Multiqueue>
    static inline auto lock_push_pq(Multiqueue &mq, thread_data_t &thread_data) {
        if (thread_data.push_count == 0) {
            thread_data.push_pq = mq.pq_list_ + fastrange64(thread_data.gen(), mq.num_spqs_);
            thread_data.push_count = mq.shared_data_.stickiness;
        }

        if (static_cast<typename Multiqueue::pq_type *>(thread_data.push_pq)->try_lock()) {
            --thread_data.push_count;
            return static_cast<typename Multiqueue::pq_type *>(thread_data.push_pq);
        }

        do {
            thread_data.push_pq = mq.pq_list_ + fastrange64(thread_data.gen(), mq.num_spqs_);
        } while (!static_cast<typename Multiqueue::pq_type *>(thread_data.push_pq)->try_lock());
        thread_data.push_count = mq.shared_data_.stickiness - 1;
        return static_cast<typename Multiqueue::pq_type *>(thread_data.push_pq);
    }

    template <typename Multiqueue>
    inline static auto lock_delete_pq(Multiqueue &mq, thread_data_t &thread_data) {
        if (thread_data.delete_count[0] == 0) {
            thread_data.delete_pq[0] = mq.pq_list_ + fastrange64(thread_data.gen(), mq.num_spqs_);
            thread_data.delete_count[0] = mq.shared_data_.stickiness;
        }
        if (thread_data.delete_count[1] == 0) {
            thread_data.delete_pq[1] = mq.pq_list_ + fastrange64(thread_data.gen(), mq.num_spqs_);
            thread_data.delete_count[1] = mq.shared_data_.stickiness;
        }
        auto first_key = static_cast<typename Multiqueue::pq_type *>(thread_data.delete_pq[0])->concurrent_top_key();
        auto second_key = static_cast<typename Multiqueue::pq_type *>(thread_data.delete_pq[1])->concurrent_top_key();

        do {
            if (first_key != Multiqueue::sentinel &&
                (second_key == Multiqueue::sentinel || mq.comp_(first_key, second_key))) {
                if (static_cast<typename Multiqueue::pq_type *>(thread_data.delete_pq[0])->try_lock_if_key(first_key)) {
                    --thread_data.delete_count[0];
                    --thread_data.delete_count[1];
                    return static_cast<typename Multiqueue::pq_type *>(thread_data.delete_pq[0]);
                } else {
                    thread_data.delete_pq[0] = mq.spqs_ + fastrange64(thread_data.gen(), mq.num_spqs_);
                    first_key =
                        static_cast<typename Multiqueue::pq_type *>(thread_data.delete_pq[0])->concurrent_top_key();
                    thread_data.delete_count[0] = mq.shared_data_.stickiness;
                }
            } else if (second_key != Multiqueue::sentinel) {
                if (static_cast<typename Multiqueue::pq_type *>(thread_data.delete_pq[1])
                        ->try_lock_if_key(second_key)) {
                    --thread_data.delete_count[0];
                    --thread_data.delete_count[1];
                    return static_cast<typename Multiqueue::pq_type *>(thread_data.delete_pq[1]);
                } else {
                    thread_data.delete_pq[1] = mq.spqs_ + fastrange64(thread_data.gen(), mq.num_spqs_);
                    second_key =
                        static_cast<typename Multiqueue::pq_type *>(thread_data.delete_pq[1])->concurrent_top_key();
                    thread_data.delete_count[1] = mq.shared_data_.stickiness;
                }
            } else {
                // Both keys are sentinels
                thread_data.delete_count[0] = 0;
                thread_data.delete_count[1] = 0;
                break;
            }

        } while (true);
        return nullptr;
    }
};

}  // namespace selection_strategy
}  // namespace multiqueue
#endif  //! SELECTION_STRATEGY_STICKY_HPP_INCLUDED
