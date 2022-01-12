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
#include "multiqueue/external/xoroshiro256starstar.hpp"

#include <cassert>
#include <cstdint>
#include <sstream>
#include <string>
#include <utility>

namespace multiqueue::selection_strategy {

class Sticky {
   public:
    struct Parameters {
        unsigned int stickiness = 8;
    };

    struct handle_data_t {
        xoroshiro256starstar rng;
        unsigned int push_count = 0;
        unsigned int delete_count = 0;
        std::size_t push_index;
        std::pair<std::size_t, std::size_t> delete_index;

        handle_data_t(std::uint64_t seed, unsigned int /* id */) noexcept : rng{seed} {
        }
    };

   private:
    std::size_t num_pqs_;
    unsigned int stickiness_;

   public:
    Sticky(std::size_t num_pqs, Parameters const &params) noexcept : num_pqs_{num_pqs}, stickiness_{params.stickiness} {
        assert(stickiness > 0);
    }

    std::string description() const {
        std::stringstream ss;
        ss << "sticky\n";
        ss << "\tStickiness: " << stickiness_;
        return ss.str();
    }

    std::pair<std::size_t, std::size_t> get_delete_pqs(handle_data_t &handle_data) {
        if (handle_data.delete_count == 0) {
            handle_data.delete_index = {fastrange64(handle_data.rng(), num_pqs_),
                                        fastrange64(handle_data.rng(), num_pqs_)};
            handle_data.delete_count = stickiness_;
        }

        return handle_data.delete_index;
    }

    void delete_pq_used(bool no_fail, handle_data_t &handle_data) noexcept {
        if (no_fail) {
            --handle_data.delete_count;
        } else {
            handle_data.delete_count = stickiness_ - 1;
        }
    }

    void delete_pq_empty(handle_data_t & /* handle_data */) noexcept {
    }

    std::pair<std::size_t, std::size_t> get_fallback_delete_pqs(handle_data_t &handle_data) noexcept {
        return handle_data.delete_index = {fastrange64(handle_data.rng(), num_pqs_),
                                           fastrange64(handle_data.rng(), num_pqs_)};
    }

    std::size_t get_push_pqs(handle_data_t &handle_data) noexcept {
        if (handle_data.push_count == 0) {
            handle_data.push_index = fastrange64(handle_data.rng(), num_pqs_);
            handle_data.push_count = stickiness_;
        }

        return handle_data.push_index;
    }

    void push_pq_used(bool no_fail, handle_data_t &handle_data) noexcept {
        if (no_fail) {
            --handle_data.push_count;
        } else {
            handle_data.push_count = stickiness_ - 1;
        }
    }

    std::size_t get_fallback_push_pq(handle_data_t &handle_data) noexcept {
        return handle_data.push_index = fastrange64(handle_data.rng(), num_pqs_);
    }
};

}  // namespace multiqueue::selection_strategy

#endif  //! SELECTION_STRATEGY_STICKY_HPP_INCLUDED
