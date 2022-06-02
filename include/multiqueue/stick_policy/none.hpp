/**
******************************************************************************
* @file:   none.hpp
*
* @author: Marvin Williams
* @date:   2021/09/27 10:15
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef STICK_POLICY_NONE_HPP_INCLUDED
#define STICK_POLICY_NONE_HPP_INCLUDED

#include "multiqueue/external/fastrange.h"
#include "multiqueue/external/xoroshiro256starstar.hpp"

#include <cstddef>
#include <sstream>
#include <string>

namespace multiqueue::stick_policy {

class None {
   public:
    struct Config {};

    struct ThreadData {
        xoroshiro256starstar rng;
        ThreadData(std::uint64_t seed, unsigned int /* id */) noexcept : rng{seed} {
        }
    };

    struct GlobalData {
        GlobalData(std::size_t /* num_pqs */, Config const &) noexcept {
        }
    };

    static std::string description() {
        return "none";
    }

   private:
    static std::size_t get_random_index(xoroshiro256starstar &rng, std::size_t max) {
        return fastrange64(rng(), max);
    }

   public:
    template <typename MultiQueue>
    static typename MultiQueue::guarded_pq_type *get_pop_pq_1(MultiQueue &mq, ThreadData &data) {
        return &mq.pq_list_[get_random_index(data.rng, mq.num_pqs_)];
    }

    template <typename MultiQueue>
    static typename MultiQueue::guarded_pq_type *get_pop_pq_2(MultiQueue &mq, ThreadData &data) {
        return &mq.pq_list_[get_random_index(data.rng, mq.num_pqs_)];
    }

    static void pop_pq_1_used_callback(ThreadData &) noexcept {
    }

    static void pop_pq_2_used_callback(ThreadData &) noexcept {
    }

    static void pop_pq_1_failed_callback(ThreadData &) noexcept {
    }

    static void pop_pq_2_failed_callback(ThreadData &) noexcept {
    }

    static void push_pq_used_callback(ThreadData &) noexcept {
    }

    static void push_pq_failed_callback(ThreadData &) noexcept {
    }
};

}  // namespace multiqueue::stick_policy

#endif  //! STICK_POLICY_NONE_HPP_INCLUDED
