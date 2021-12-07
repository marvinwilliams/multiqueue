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
#include <utility>
#ifndef SELECTION_STRATEGY_PERM_HPP_INCLUDED
#define SELECTION_STRATEGY_PERM_HPP_INCLUDED

#include "multiqueue/external/fastrange.h"

#include <atomic>
#include <cassert>
#include <cstdint>
#include <numeric>
#include <sstream>
#include <string>
#include <vector>

namespace multiqueue::selection_strategy {

struct perm {
    struct Parameters {
        unsigned int stickiness = 64;
        std::size_t c = 4;
    };

    template <typename MultiQueue>
    struct Strategy {
        using pq_type = typename MultiQueue::pq_type;

        struct handle_data_t {
            unsigned int id;
            unsigned int count = 0;
            std::uint64_t current_perm = to_perm(1, 0);
            handle_data_t(unsigned int index) : id{index} {
            }
        };

        struct alignas(L1_CACHE_LINESIZE) Permutation {
            // Upper half a, lower half b
            // i*a + b mod z
            std::atomic_uint64_t n;
        };

        MultiQueue &mq;
        unsigned int stickiness;
        Permutation perm;

        Strategy(MultiQueue &mq_ref, Parameters const &param)
            : mq{mq_ref}, stickiness{param.stickiness}, perm{to_perm(1, 0)} {
            assert(stickiness > 0);
        }

        std::string description() const {
            std::stringstream ss;
            ss << "swapping\n";
            ss << "\tStickiness: " << stickiness;
            return ss.str();
        }

        static constexpr std::size_t get_perm(std::uint64_t p, std::size_t i, std::size_t num) noexcept {
            return (i * (p >> 32) + (((std::uint64_t(1) << 32) - 1) & p)) % num;
        }

        static constexpr std::uint64_t to_perm(std::uint64_t a, std::uint64_t b) noexcept {
            return (a << 32 | b);
        }

        template <typename Generator>
        std::uint64_t update_permutation(std::uint64_t current, Generator &g) {
            std::uint64_t a = fastrange64(g(), mq.num_pqs() - 1) + 1;
            while (std::gcd(a, mq.num_pqs()) != 1) {
                a = fastrange64(g(), mq.num_pqs() - 1) + 1;
            }
            std::uint64_t b = fastrange64(g(), mq.num_pqs());
            auto p = to_perm(a, b);
            return perm.n.compare_exchange_strong(current, p, std::memory_order_relaxed) ? p : current;
        }

        template <typename Generator>
        pq_type *lock_push_pq(handle_data_t &handle_data, Generator &g) {
            auto p = perm.n.load(std::memory_order_relaxed);
            if (handle_data.current_perm != p) {
                handle_data.current_perm = p;
                handle_data.count = 0;
            } else if (handle_data.count == stickiness) {
                handle_data.current_perm = update_permutation(p, g);
                handle_data.count = 0;
            }
            auto index = get_perm(handle_data.current_perm, handle_data.id * 3, mq.num_pqs());
            if (!mq.pq_list_[index].try_lock()) {
                do {
                    index = fastrange64(g(), mq.num_pqs());
                } while (!mq.pq_list_[index].try_lock());
            }
            ++handle_data.count;
            return &mq.pq_list_[index];
        }

        template <typename Generator>
        pq_type *lock_delete_pq(handle_data_t &handle_data, Generator &g) {
            auto p = perm.n.load(std::memory_order_relaxed);
            if (handle_data.current_perm != p) {
                handle_data.current_perm = p;
                handle_data.count = 0;
            } else if (handle_data.count == stickiness) {
                handle_data.current_perm = update_permutation(p, g);
                handle_data.count = 0;
            }
            auto first_index = get_perm(handle_data.current_perm, 3 * handle_data.id + 1, mq.num_pqs());
            auto second_index = get_perm(handle_data.current_perm, 3 * handle_data.id + 2, mq.num_pqs());
            auto first_key = mq.pq_list_[first_index].top_key();
            auto second_key = mq.pq_list_[second_index].top_key();
            do {
                if (mq.comp_(first_key, second_key)) {
                    if (first_key == mq.get_sentinel()) {
                        break;
                    }
                    if (mq.pq_list_[first_index].try_lock_assume_key(first_key)) {
                        ++handle_data.count;
                        return &mq.pq_list_[first_index];
                    }
                } else {
                    if (second_key == mq.get_sentinel()) {
                        break;
                    }
                    if (mq.pq_list_[second_index].try_lock_assume_key(second_key)) {
                        ++handle_data.count;
                        return &mq.pq_list_[second_index];
                    }
                }
                first_index = fastrange64(g(), mq.num_pqs());
                second_index = fastrange64(g(), mq.num_pqs());
                first_key = mq.pq_list_[first_index].top_key();
                second_key = mq.pq_list_[second_index].top_key();
            } while (true);
            handle_data.count = stickiness;
            return nullptr;
        }
    };
};

}  // namespace multiqueue::selection_strategy

#endif  //! SELECTION_STRATEGY_PERM_HPP_INCLUDED
