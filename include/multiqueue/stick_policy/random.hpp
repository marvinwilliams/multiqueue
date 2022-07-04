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
#ifndef STICK_POLICY_RANDOM_HPP_INCLUDED
#define STICK_POLICY_RANDOM_HPP_INCLUDED

#include "multiqueue/external/fastrange.h"
#include "multiqueue/external/xoroshiro256starstar.hpp"

#include <cassert>
#include <cstdint>
#include <sstream>
#include <string>
#include <utility>

namespace multiqueue::stick_policy {

class Random {
   public:
    struct Config {
        unsigned int stickiness = 8;
    };

    struct ThreadData {
        unsigned int push_count = 0;
        unsigned int pop_count[2] = {0, 0};
        std::size_t push_index;
        std::size_t pop_index[2];
        xoroshiro256starstar rng;

        ThreadData(unsigned int /* id */, std::uint64_t seed) noexcept : rng{seed} {
        }
    };

    struct GlobalData {
        unsigned int stickiness;

        GlobalData(std::size_t /* num_pqs */, Config const &config) noexcept {
            stickiness = config.stickiness;
        }
    };

   private:
    static std::size_t get_random_index(xoroshiro256starstar &rng, std::size_t max) noexcept {
        return fastrange64(rng(), max);
    }

   public:
    static std::size_t get_pop_pq(std::size_t num_pqs, unsigned int num, ThreadData &thread_data,
                                  GlobalData &global_data) noexcept {
        assert(num < 2);
        if (thread_data.pop_count[num] == 0) {
            thread_data.pop_index[num] = get_random_index(thread_data.rng, num_pqs);
            thread_data.pop_count[num] = global_data.stickiness;
        }

        return thread_data.pop_index[num];
    }

    static void pop_failed_callback(unsigned int num, ThreadData &thread_data) noexcept {
        assert(num < 2);
        thread_data.pop_count[num] = 0;
    }

    static void pop_callback(ThreadData &thread_data) noexcept {
        assert(thread_data.pop_count[0] > 0 && thread_data.pop_count[1] > 0);
        --thread_data.pop_count[0];
        --thread_data.pop_count[1];
    }

    static std::size_t get_push_pq(std::size_t num_pqs, ThreadData &thread_data, GlobalData &global_data) noexcept {
        if (thread_data.push_count == 0) {
            thread_data.push_index = get_random_index(thread_data.rng, num_pqs);
            thread_data.push_count = global_data.stickiness;
        }

        return thread_data.push_index;
    }

    static void push_failed_callback(ThreadData &thread_data) noexcept {
        thread_data.push_count = 0;
    }

    static void push_callback(ThreadData &thread_data) noexcept {
        assert(thread_data.push_count > 0);
        --thread_data.push_count;
    }
};

}  // namespace multiqueue::stick_policy

#endif  //! STICK_POLICY_RANDOM_HPP_INCLUDED
