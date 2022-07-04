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
#ifndef STICK_POLICY_SWAPPING_HPP_INCLUDED
#define STICK_POLICY_SWAPPING_HPP_INCLUDED

#include "multiqueue/external/fastrange.h"
#include "multiqueue/external/xoroshiro256starstar.hpp"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

#ifndef L1_CACHE_LINESIZE
#error Need to define L1_CACHE_LINESIZE
#endif

namespace multiqueue::stick_policy {

class Swapping {
   public:
    struct Config {
        unsigned int stickiness = 64;
    };

    struct ThreadData {
        unsigned int index;
        unsigned int push_count = 0;
        unsigned int pop_count[2] = {0, 0};
        xoroshiro256starstar rng;

        ThreadData(unsigned int id, std::uint64_t seed) : index{id}, rng{seed} {
        }
    };

    struct alignas(L1_CACHE_LINESIZE) AlignedIndex {
        std::atomic_size_t index;
    };

    struct GlobalData {
        using Assignment = std::vector<AlignedIndex>;

        unsigned int stickiness;
        Assignment assignment;

        GlobalData(std::size_t num_pqs, Config const &config) : stickiness{config.stickiness}, assignment(num_pqs) {
            assert(stickiness > 0);
            for (std::size_t i = 0; i < assignment.size(); ++i) {
                assignment[i].index = i;
            }
        }
    };

   private:
    static std::size_t get_random_index(xoroshiro256starstar &rng, std::size_t max) noexcept {
        return fastrange64(rng(), max);
    }

    static std::size_t swap_assignment(GlobalData::Assignment &assignment, unsigned int index,
                                       xoroshiro256starstar &rng) {
        assert(index < assignment.size());
        // Cannot be invalid, only thread itself invalidates
        std::size_t old_pq = assignment[index].index.exchange(assignment.size(), std::memory_order_relaxed);
        assert(old_pq < assignment.size());
        std::size_t other_index;
        std::size_t other_pq;
        do {
            other_index = get_random_index(rng, assignment.size());
            other_pq = assignment[other_index].index.load(std::memory_order_relaxed);
        } while (other_pq == assignment.size() ||
                 !assignment[other_index].index.compare_exchange_strong(other_pq, old_pq, std::memory_order_relaxed));
        assignment[index].index.store(other_pq, std::memory_order_relaxed);
        return other_pq;
    }

   public:
    static std::size_t get_pop_pq(std::size_t /* num_pqs */, unsigned int num, ThreadData &thread_data,
                                  GlobalData &global_data) noexcept {
        assert(num < 2);
        if (thread_data.pop_count[num] == 0) {
            thread_data.pop_count[num] = global_data.stickiness;
            return swap_assignment(global_data.assignment, thread_data.index * 3 + num, thread_data.rng);
        }
        return global_data.assignment[thread_data.index * 3 + num].index.load(std::memory_order_relaxed);
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

    static std::size_t get_push_pq(std::size_t /* num_pqs */, ThreadData &thread_data,
                                   GlobalData &global_data) noexcept {
        if (thread_data.push_count == 0) {
            thread_data.push_count = global_data.stickiness;
            return swap_assignment(global_data.assignment, thread_data.index * 3 + 2, thread_data.rng);
        }
        return global_data.assignment[thread_data.index * 3 + 2].index.load(std::memory_order_relaxed);
    }

    static void push_failed_callback(ThreadData &data) noexcept {
        data.push_count = 0;
    }

    static void push_callback(ThreadData &data) noexcept {
        assert(data.push_count > 0);
        --data.push_count;
    }
};

}  // namespace multiqueue::stick_policy

#endif  //! STICK_POLICY_SWAPPING_HPP_INCLUDED
