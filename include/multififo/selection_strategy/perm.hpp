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
#ifndef SELECTION_STRATEGY_PERM_HPP_INCLUDED
#define SELECTION_STRATEGY_PERM_HPP_INCLUDED

#include "multififo/external/fastrange.h"
#include "multififo/external/xoroshiro256starstar.hpp"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <utility>
#include <vector>


namespace multififo::selection_strategy {

class Permuting {
   public:
    struct Parameters {
        unsigned int stickiness = 64;
    };

    struct handle_data_t {
        xoroshiro256starstar rng;
        unsigned int id;

        handle_data_t(std::uint64_t seed, unsigned int index) : rng{seed}, id{index} {
        }
    };

   private:
    struct alignas(L1_CACHE_LINESIZE) Permutation {
        // Upper half a, lower half b
        // i*a + b mod z
        static constexpr std::uint64_t mask = ((std::uint64_t(1) << 32) - 1);
        std::atomic_uint64_t n;
    };

    std::size_t log_num_pqs_;
    unsigned int stickiness;
    unsigned int count;
    Permutation perm;

   public:
    Permuting(std::size_t num_pqs, Parameters const &params)
        : stickiness{params.stickiness}, count{stickiness}, perm{to_perm(1, 0)} {
        assert(stickiness > 0);
        log_num_pqs_ = 0;
        num_pqs >>= 1;
        while (num_pqs != 0) {
            num_pqs >>= 1;
            ++log_num_pqs_;
        }
    }

    std::string description() const {
        std::stringstream ss;
        ss << "permutation\n";
        ss << "\tStickiness: " << stickiness;
        return ss.str();
    }

    static constexpr std::size_t get_perm(std::uint64_t p, std::size_t i, std::size_t log) noexcept {
        return (i * (p >> 32) + (Permutation::mask & p)) & ((static_cast<std::size_t>(1) << log) - 1);
    }

    static constexpr std::uint64_t to_perm(std::uint64_t a, std::uint64_t b) noexcept {
        return (a << 32 | (Permutation::mask & b));
    }

    std::uint64_t update_permutation(handle_data_t &handle_data) {
        std::uint64_t a = 2 * handle_data.rng() + 1;
        std::uint64_t b = handle_data.rng();
        std::uint64_t p = to_perm(a, b);
        perm.n.store(p, std::memory_order_relaxed);
        return p;
    }

    std::pair<std::size_t, std::size_t> get_delete_indices(handle_data_t &handle_data) {
        if (handle_data.id == 0 && count == 0) {
            std::uint64_t p = update_permutation(handle_data);
            count = stickiness;
            return {get_perm(p, 3 * handle_data.id + 1, log_num_pqs_),
                    get_perm(p, 3 * handle_data.id + 2, log_num_pqs_)};
        }
        std::uint64_t p = perm.n.load(std::memory_order_relaxed);
        return {get_perm(p, 3 * handle_data.id + 1, log_num_pqs_), get_perm(p, 3 * handle_data.id + 2, log_num_pqs_)};
    }

    void delete_index_used(bool /* no_fail */, handle_data_t &handle_data) noexcept {
        if (handle_data.id == 0) {
            --count;
        }
    }

    std::pair<std::size_t, std::size_t> get_fallback_delete_indices(handle_data_t &handle_data) noexcept {
        return {fastrange64(handle_data.rng(), 1 << log_num_pqs_), fastrange64(handle_data.rng(), 1 << log_num_pqs_)};
    }

    std::size_t get_push_index(handle_data_t &handle_data) noexcept {
        std::uint64_t p;
        if (handle_data.id == 0 && count == 0) {
            p = update_permutation(handle_data);
            count = stickiness;
        } else {
            p = perm.n.load(std::memory_order_relaxed);
        }
        return get_perm(p, 3 * handle_data.id, log_num_pqs_);
    }

    void push_index_used(bool /* no_fail */, handle_data_t &handle_data) noexcept {
        if (handle_data.id == 0) {
            --count;
        }
    }

    std::size_t get_fallback_push_index(handle_data_t &handle_data) noexcept {
        return fastrange64(handle_data.rng(), 1 << log_num_pqs_);
    }
};

}  // namespace multififo::selection_strategy

#endif  //! SELECTION_STRATEGY_PERM_HPP_INCLUDED
