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
#include "multiqueue/external/xoroshiro256starstar.hpp"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

namespace multiqueue::selection_strategy {

class Swapping {
   public:
    struct Parameters {
        unsigned int stickiness = 64;
    };

    struct handle_data_t {
        xoroshiro256starstar rng;
        unsigned int id;
        unsigned int push_count = 0;
        unsigned int delete_count = 0;

        handle_data_t(std::uint64_t seed, unsigned int id) : rng{seed}, id{id} {
        }
    };

    struct alignas(L1_CACHE_LINESIZE) AlignedIndex {
        std::atomic_size_t index;
    };

    unsigned int stickiness;
    std::vector<AlignedIndex> assignment;

    Swapping(std::size_t num_pqs, Parameters const &params) : stickiness{params.stickiness}, assignment(num_pqs) {
        assert(stickiness > 0);
        for (std::size_t i = 0; i < assignment.size(); ++i) {
            assignment[i].index.store(i, std::memory_order_relaxed);
        }
    }

    std::string description() const {
        std::stringstream ss;
        ss << "swapping\n";
        ss << "\tStickiness: " << stickiness;
        return ss.str();
    }

    std::size_t swap_assignment(unsigned int pq, handle_data_t &handle_data) {
        assert(pq < assignment.size());
        // Cannot be invalid, only thread itself invalidates
        std::size_t old_index = assignment[pq].index.exchange(assignment.size(), std::memory_order_relaxed);
        do {
            std::size_t other_pq = fastrange64(handle_data.rng(), assignment.size());
            std::size_t other_index = assignment[other_pq].index.load(std::memory_order_relaxed);
            if (other_index != assignment.size() &&
                assignment[other_pq].index.compare_exchange_strong(other_index, old_index, std::memory_order_relaxed)) {
                assignment[pq].index.store(other_index, std::memory_order_relaxed);
                return other_index;
            }
        } while (true);
    }

    std::pair<std::size_t, std::size_t> get_delete_pqs(handle_data_t &handle_data) {
        if (handle_data.delete_count == stickiness) {
            std::size_t first_index = swap_assignment(handle_data.id * 3 + 1, handle_data);
            std::size_t second_index = swap_assignment(handle_data.id * 3 + 2, handle_data);
            handle_data.delete_count = 0;
            assert(first_index < assignment.size() && second_index < assignment.size());
            return {first_index, second_index};
        }
        std::size_t first_index = assignment[3 * handle_data.id + 1].index.load(std::memory_order_relaxed);
        std::size_t second_index = assignment[3 * handle_data.id + 2].index.load(std::memory_order_relaxed);
        assert(first_index < assignment.size() && second_index < assignment.size());
        return {first_index, second_index};
    }

    void delete_pq_used(bool no_fail, handle_data_t &handle_data) noexcept {
        if (no_fail) {
            ++handle_data.delete_count;
        } else {
            handle_data.delete_count = 1;
        }
    }

    std::pair<std::size_t, std::size_t> get_fallback_delete_pqs(handle_data_t &handle_data) {
        std::size_t first_index = swap_assignment(handle_data.id * 3 + 1, handle_data);
        std::size_t second_index = swap_assignment(handle_data.id * 3 + 2, handle_data);
        assert(first_index < assignment.size() && second_index < assignment.size());
        return {first_index, second_index};
    }

    std::size_t get_push_pq(handle_data_t &handle_data) noexcept {
        if (handle_data.push_count == stickiness) {
            std::size_t index = swap_assignment(handle_data.id * 3, handle_data);
            handle_data.delete_count = 0;
            assert(index < assignment.size());
            return index;
        }
        std::size_t index = assignment[3 * handle_data.id].index.load(std::memory_order_relaxed);
        assert(index < assignment.size());
        return index;
    }

    void push_pq_used(bool no_fail, handle_data_t &handle_data) noexcept {
        if (no_fail) {
            ++handle_data.push_count;
        } else {
            handle_data.push_count = 1;
        }
    }

    std::size_t get_fallback_push_pq(handle_data_t &handle_data) noexcept {
        std::size_t index = swap_assignment(handle_data.id * 3, handle_data);
        assert(index < assignment.size());
        return index;
    }
};

}  // namespace multiqueue::selection_strategy

#endif  //! SELECTION_STRATEGY_SWAPPING_HPP_INCLUDED
