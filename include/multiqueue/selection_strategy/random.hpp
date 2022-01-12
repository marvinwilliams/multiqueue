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
#ifndef SELECTION_STRATEGY_RANDOM_HPP_INCLUDED
#define SELECTION_STRATEGY_RANDOM_HPP_INCLUDED

#include "multiqueue/external/fastrange.h"
#include "multiqueue/external/xoroshiro256starstar.hpp"

#include <cstddef>
#include <sstream>
#include <string>

namespace multiqueue::selection_strategy {

class Random {
   public:
    struct Parameters {};

    struct handle_data_t {
        xoroshiro256starstar rng;
        handle_data_t(std::uint64_t seed, unsigned int /* id */) noexcept : rng{seed} {
        }
    };

   private:
    std::size_t num_pqs_;

   public:
    Random(std::size_t num_pqs, Parameters const & /* params */) noexcept : num_pqs_{num_pqs} {
    }

    static std::string description() {
        return "random";
    }

    std::pair<std::size_t, std::size_t> get_delete_pqs(handle_data_t &handle_data) {
        return {fastrange64(handle_data.rng(), num_pqs_), fastrange64(handle_data.rng(), num_pqs_)};
    }

    void delete_pq_used(bool /* no_fail */, handle_data_t & /* handle_data */) noexcept {
    }

    std::pair<std::size_t, std::size_t> get_fallback_delete_pqs(handle_data_t &handle_data) {
        return {fastrange64(handle_data.rng(), num_pqs_), fastrange64(handle_data.rng(), num_pqs_)};
    }

    std::size_t get_push_pq(handle_data_t &handle_data) noexcept {
        return fastrange64(handle_data.rng(), num_pqs_);
    }

    void push_pq_used(bool /* no_fail */, handle_data_t & /* handle_data */) noexcept {
    }

    std::size_t get_fallback_push_pq(handle_data_t &handle_data) noexcept {
        return fastrange64(handle_data.rng(), num_pqs_);
    }
};

}  // namespace multiqueue::selection_strategy

#endif  //! SELECTION_STRATEGY_RANDOM_HPP_INCLUDED
