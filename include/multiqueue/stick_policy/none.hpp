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

        ThreadData(unsigned int /* id */, std::uint64_t seed) noexcept : rng{seed} {
        }
    };

    struct GlobalData {
        GlobalData(std::size_t /* num_pqs */, Config const &) noexcept {
        }
    };

   private:
    static std::size_t get_random_index(xoroshiro256starstar &rng, std::size_t max) noexcept {
        return fastrange64(rng(), max);
    }

   public:
    template <std::size_t /* I */>
    static std::size_t get_pop_pq(std::size_t num_pqs, ThreadData &thread_data, GlobalData &) noexcept {
        return get_random_index(thread_data.rng, num_pqs);
    }

    template <std::size_t /* I */>
    static void pop_failed_callback(ThreadData &) noexcept {
    }

    static void pop_callback(ThreadData &) noexcept {
    }

    static std::size_t get_push_pq(std::size_t num_pqs, ThreadData &thread_data, GlobalData &) noexcept {
        return get_random_index(thread_data.rng, num_pqs);
    }

    static void push_failed_callback(ThreadData &) noexcept {
    }

    static void push_callback(ThreadData &) noexcept {
    }
};

}  // namespace multiqueue::stick_policy

#endif  //! STICK_POLICY_NONE_HPP_INCLUDED
