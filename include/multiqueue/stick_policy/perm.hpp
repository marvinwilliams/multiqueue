/**
******************************************************************************
* @file:   perm.hpp
*
* @author: Marvin Williams
* @date:   2021/09/27 10:15
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef STICK_POLICY_PERM_HPP_INCLUDED
#define STICK_POLICY_PERM_HPP_INCLUDED

#include "multiqueue/external/fastrange.h"
#include "multiqueue/external/xoroshiro256starstar.hpp"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#ifndef L1_CACHE_LINESIZE
#error Need to define L1_CACHE_LINESIZE
#endif

namespace multiqueue::stick_policy {

class Permuting {
   public:
    struct Config {
        unsigned int stickiness = 64;
    };

    struct ThreadData {
        bool last_failed;
        unsigned int index;
        unsigned int push_count = 0;
        unsigned int pop_count = 0;
        xoroshiro256starstar rng;

        ThreadData(unsigned int id, std::uint64_t seed) : last_failed{false}, index{id}, rng{seed} {
        }
    };

   private:
    static constexpr std::uint64_t Mask = ((std::uint64_t{1} << 32) - 1);

    static constexpr std::size_t get_index(std::uint64_t p, std::size_t i, std::size_t num_pqs) noexcept {
        return (i * (p >> 32) + (p & Mask)) % num_pqs;
    }

    static constexpr std::uint64_t to_perm(std::uint64_t a, std::uint64_t b) noexcept {
        return (a << 32) | (b & Mask);
    }

    static std::size_t get_random_index(xoroshiro256starstar &rng, std::size_t max) noexcept {
        return fastrange64(rng(), max);
    }

   public:
    struct GlobalData {
        std::uint64_t pq_mask;
        unsigned int stickiness;
        // Upper half a, lower half b
        // i*a + b mod z
        std::vector<std::uint64_t> valid_factors;
        std::atomic_uint64_t perm;

        GlobalData(std::size_t num_pqs, Config const &config) : stickiness{config.stickiness}, perm{to_perm(1, 0)} {
            assert(stickiness > 0);
            for (std::uint64_t i = 1; i < num_pqs; ++i) {
                if (std::gcd(i, num_pqs) == 1) {
                    valid_factors.push_back(i);
                }
            }
        }

        std::uint64_t update_permutation(xoroshiro256starstar &rng) {
            std::uint64_t a = valid_factors[fastrange64(rng(), valid_factors.size())];
            std::uint64_t p = to_perm(a, rng());
            perm.store(p, std::memory_order_relaxed);
            return p;
        }
    };

    static std::size_t get_pop_pq(std::size_t num_pqs, unsigned int num, ThreadData &thread_data,
                                  GlobalData &global_data) noexcept {
        assert(num < 2);
        if (thread_data.last_failed) {
            thread_data.last_failed = false;
            return get_random_index(thread_data.rng, num_pqs);
        }
        std::uint64_t p;
        if (thread_data.index == 0 && thread_data.pop_count == 0) {
            p = global_data.update_permutation(thread_data.rng);
            thread_data.pop_count = global_data.stickiness;
        } else {
            p = global_data.perm.load(std::memory_order_relaxed);
        }
        return get_index(p, thread_data.index * 3 + num, num_pqs);
    }

    static void pop_failed_callback(unsigned int /* num */, ThreadData &thread_data) noexcept {
        thread_data.last_failed = true;
    }

    static void pop_callback(ThreadData &thread_data) noexcept {
        if (thread_data.index == 0) {
            --thread_data.pop_count;
        }
    }

    static std::size_t get_push_pq(std::size_t num_pqs, ThreadData &thread_data, GlobalData &global_data) noexcept {
        if (thread_data.last_failed) {
            thread_data.last_failed = false;
            return get_random_index(thread_data.rng, num_pqs);
        }
        std::uint64_t p;
        if (thread_data.index == 0 && thread_data.push_count == 0) {
            p = global_data.update_permutation(thread_data.rng);
            thread_data.push_count = global_data.stickiness;
        } else {
            p = global_data.perm.load(std::memory_order_relaxed);
        }
        return get_index(p, thread_data.index * 3 + 2, num_pqs);
    }

    static void push_failed_callback(ThreadData &thread_data) noexcept {
        thread_data.last_failed = true;
    }

    static void push_callback(ThreadData &thread_data) noexcept {
        if (thread_data.index == 0) {
            --thread_data.push_count;
        }
    }
};

}  // namespace multiqueue::stick_policy

#endif  //! STICK_POLICY_PERM_HPP_INCLUDED
