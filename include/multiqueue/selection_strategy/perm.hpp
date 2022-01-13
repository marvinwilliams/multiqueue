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

namespace multiqueue::selection_strategy {

class Permuting {
   public:
    struct Parameters {
        unsigned int stickiness = 64;
    };

    struct handle_data_t {
        xoroshiro256starstar rng;
        unsigned int id;

        handle_data_t(std::uint64_t seed, unsigned int id) : rng{seed}, id{id} {
        }
    };

   private:
    struct alignas(L1_CACHE_LINESIZE) Permutation {
        // Upper half a, lower half b
        // i*a + b mod z
        static constexpr std::uint64_t mask = ((std::uint64_t(1) << 32) - 1);
        std::atomic_uint64_t n;
    };

    std::size_t num_pqs_;
    unsigned int stickiness;
    unsigned int count;
    Permutation perm;
    std::vector<std::uint64_t> coprimes;

   public:
    Permuting(std::size_t num_pqs, Parameters const &params)
        : num_pqs_{num_pqs}, stickiness{params.stickiness}, count{stickiness}, perm{to_perm(1, 0)} {
        assert(stickiness > 0);
        for (std::size_t i = 1; i < num_pqs; ++i) {
            if (std::gcd(i, num_pqs_) == 1) {
                coprimes.push_back(static_cast<std::uint64_t>(i));
            }
        }
    }

    std::string description() const {
        std::stringstream ss;
        ss << "permutation\n";
        ss << "\tStickiness: " << stickiness;
        return ss.str();
    }

    static constexpr std::size_t get_perm(std::uint64_t p, std::size_t i, std::size_t num) noexcept {
        return (i * (p >> 32) + (Permutation::mask & p)) % num;
    }

    static constexpr std::uint64_t to_perm(std::uint64_t a, std::uint64_t b) noexcept {
        return (a << 32 | (Permutation::mask & b));
    }

    std::uint64_t update_permutation(handle_data_t &handle_data) {
        std::uint64_t a = coprimes[handle_data.rng(), coprimes.size()];
        std::uint64_t b = fastrange64(handle_data.rng(), num_pqs_);
        std::uint64_t p = to_perm(a, b);
        perm.n.store(p, std::memory_order_relaxed);
        return p;
    }

    std::pair<std::size_t, std::size_t> get_delete_pqs(handle_data_t &handle_data) {
        if (handle_data.id == 0 && count == 0) {
            std::uint64_t p = update_permutation(handle_data);
            count = stickiness;
            return {get_perm(p, 3 * handle_data.id + 1, num_pqs_), get_perm(p, 3 * handle_data.id + 2, num_pqs_)};
        }
        std::uint64_t p = perm.n.load(std::memory_order_relaxed);
        return {get_perm(p, 3 * handle_data.id + 1, num_pqs_), get_perm(p, 3 * handle_data.id + 2, num_pqs_)};
    }

    void delete_pq_used(bool /* no_fail */, handle_data_t &handle_data) noexcept {
        if (handle_data.id == 0) {
            --count;
        }
    }

    std::pair<std::size_t, std::size_t> get_fallback_delete_pqs(handle_data_t &handle_data) noexcept {
        return {fastrange64(handle_data.rng(), num_pqs_), fastrange64(handle_data.rng(), num_pqs_)};
    }

    std::size_t get_push_pq(handle_data_t &handle_data) noexcept {
        std::uint64_t p;
        if (handle_data.id == 0 && count == 0) {
            p = update_permutation(handle_data);
            count = stickiness;
        } else {
            p = perm.n.load(std::memory_order_relaxed);
        }
        return get_perm(p, 3 * handle_data.id, num_pqs_);
    }

    void push_pq_used(bool no_fail, handle_data_t &handle_data) noexcept {
        if (handle_data.id == 0) {
            --count;
        }
    }

    std::size_t get_fallback_push_pq(handle_data_t &handle_data) noexcept {
        return fastrange64(handle_data.rng(), num_pqs_);
    }
};

}  // namespace multiqueue::selection_strategy

#endif  //! SELECTION_STRATEGY_PERM_HPP_INCLUDED
