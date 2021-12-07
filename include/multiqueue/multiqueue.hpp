/**
******************************************************************************
* @file:   multiqueue.hpp
*
* @author: Marvin Williams
* @date:   2021/03/29 17:19
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef MULTIQUEUE_HPP_INCLUDED
#define MULTIQUEUE_HPP_INCLUDED

#ifndef L1_CACHE_LINESIZE
#define L1_CACHE_LINESIZE 64
#endif

#ifndef PAGESIZE
#define PAGESIZE 4096
#endif

#include "multiqueue/external/xoroshiro256starstar.hpp"
#include "multiqueue/guarded_pq.hpp"
#include "multiqueue/selection_strategy/sticky.hpp"

#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

#ifdef MQ_ELEMENT_DISTRIBUTION
#include <algorithm>
#include <utility>
#include <vector>
#endif

namespace multiqueue {

template <typename... Configs>
struct MultiqueueParameters : Configs::Parameters... {
    std::uint64_t seed = 1;
};

template <typename Key, typename T, typename Compare, template <typename, typename> typename PriorityQueue,
          typename StrategyType = selection_strategy::sticky, bool ImplicitLock = false,
          typename Allocator = std::allocator<Key>>
class Multiqueue {
   private:
    using pq_type = GuardedPQ<Key, T, Compare, PriorityQueue, ImplicitLock>;

   public:
    using key_type = typename pq_type::key_type;
    using value_type = typename pq_type::value_type;
    using key_compare = typename pq_type::key_compare;
    using reference = typename pq_type::reference;
    using const_reference = typename pq_type::const_reference;
    using size_type = typename pq_type::size_type;
    using allocator_type = Allocator;
    using param_type = MultiqueueParameters<StrategyType>;

   private:
    using Strategy = typename StrategyType::template Strategy<Multiqueue>;
    friend Strategy;

   public:
    class alignas(2 * L1_CACHE_LINESIZE) Handle {
        friend Multiqueue;
        using data_t = typename Strategy::handle_data_t;

        std::reference_wrapper<Multiqueue> mq_;
        xoroshiro256starstar rng_;
        data_t data_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit Handle(Multiqueue &mq, unsigned int id, std::uint64_t seed) noexcept : mq_{mq}, rng_{seed}, data_{id} {
        }

       public:
        bool try_extract_top(reference retval) noexcept {
            auto pq = mq_.get().strategy_.lock_delete_pq(data_, rng_);
            if (!pq) {
                return false;
            }
            pq->extract_top(retval);
            pq->unlock();
            return true;
        }

        void push(const_reference value) noexcept {
            auto pq = mq_.get().strategy_.lock_push_pq(data_, rng_);
            assert(pq);
            pq->push(value);
            pq->unlock();
        }

        void push(value_type &&value) noexcept {
            auto pq = mq_.get().strategy_.lock_push_pq(data_, rng_);
            assert(pq);
            pq->push(std::move(value));
            pq->unlock();
        }

        bool try_extract_from(size_type pos, value_type &retval) noexcept {
            assert(pos < mq_.get().num_pqs());
            auto &pq = mq_.get().pq_list_[pos];
            if (pq.try_lock()) {
                if (!pq.empty()) {
                    pq.extract_top(retval);
                    pq.unlock();
                    return true;
                }
                pq.unlock();
            }
            return false;
        }
    };

   private:
    using pq_alloc_type = typename std::allocator_traits<allocator_type>::template rebind_alloc<pq_type>;
    using pq_alloc_traits = typename std::allocator_traits<pq_alloc_type>;

    struct Deleter {
        size_type num_pqs;
        [[no_unique_address]] pq_alloc_type alloc;
        Deleter(allocator_type a) : num_pqs{0}, alloc{a} {
        }
        Deleter(size_type n, allocator_type a) : num_pqs{n}, alloc{a} {
        }
        void operator()(pq_type *pq_list) noexcept {
            for (pq_type *s = pq_list; s != pq_list + num_pqs; ++s) {
                pq_alloc_traits::destroy(alloc, s);
            }
            pq_alloc_traits::deallocate(alloc, pq_list, num_pqs);
        }
    };

   private:
    // False sharing is avoided by class alignment, but the members do not need to reside in individual cache lines, as
    // they are not written concurrently
    std::unique_ptr<pq_type[], Deleter> pq_list_;
    key_type sentinel_;
    [[no_unique_address]] key_compare comp_;
    std::unique_ptr<std::uint64_t[]> handle_seeds_;
    std::atomic_uint handle_index_ = 0;

    // strategy data in separate cache line, as it might be written to
    [[no_unique_address]] alignas(2 * L1_CACHE_LINESIZE) Strategy strategy_;

    void abort_on_data_misalignment() {
        for (pq_type *s = pq_list_.get(); s != pq_list_.get() + num_pqs(); ++s) {
            if (reinterpret_cast<std::uintptr_t>(s) % (2 * L1_CACHE_LINESIZE) != 0) {
                std::abort();
            }
        }
    }

   public:
    explicit Multiqueue(unsigned int num_threads, param_type const &param, key_compare const &comp = key_compare(),
                        key_type sentinel = pq_type::max_key, allocator_type const &alloc = allocator_type())
        : pq_list_(nullptr, Deleter(num_threads * param.c, alloc)),
          sentinel_{sentinel},
          comp_{comp},
          strategy_(*this, param) {
        assert(num_threads > 0);
        assert(param.c > 0);
        size_type const num_pqs = num_threads * param.c;
        pq_type *pq_list = pq_alloc_traits::allocate(pq_list_.get_deleter().alloc, num_pqs);
        for (pq_type *s = pq_list; s != pq_list + num_pqs; ++s) {
            pq_alloc_traits::construct(pq_list_.get_deleter().alloc, s, sentinel, comp_);
        }
        pq_list_.get_deleter().num_pqs = 0;
        pq_list_.reset(pq_list);
        pq_list_.get_deleter().num_pqs = num_pqs;
#ifdef MQ_ABORT_MISALIGNMENT
        abort_on_data_misalignment();
#endif
        handle_seeds_ = std::make_unique<std::uint64_t[]>(num_threads);
        std::generate(handle_seeds_.get(), handle_seeds_.get() + num_threads, xoroshiro256starstar{param.seed});
    }

    explicit Multiqueue(unsigned int num_threads, MultiqueueParameters<StrategyType> const &param,
                        key_compare const &comp, allocator_type const &alloc)
        : Multiqueue(num_threads, param, comp, pq_type::max_key, alloc) {
    }

    Handle get_handle() noexcept {
        unsigned int index = handle_index_.fetch_add(1, std::memory_order_relaxed);
        return Handle(*this, index, handle_seeds_[index]);
    }

    constexpr key_type const &get_sentinel() noexcept {
        return sentinel_;
    }

    void reserve(size_type cap) {
        std::size_t const cap_per_pq = (2 * cap) / num_pqs();
        for (auto &pq : pq_list_) {
            pq.reserve(cap_per_pq);
        };
    }

#ifdef MQ_ELEMENT_DISTRIBUTION
    std::vector<std::size_t> get_distribution() const {
        std::vector<std::size_t> distribution(num_pqs());
        std::transform(pq_list_.get(), pq_list_.get() + num_pqs(), distribution.begin(),
                       [](auto const &pq) { return pq.unsafe_size(); });
        return distribution;
    }

    std::vector<std::size_t> get_top_distribution(std::size_t k) {
        std::vector<std::pair<value_type, std::size_t>> removed_elements;
        removed_elements.reserve(k);
        std::vector<std::size_t> distribution(num_pqs(), 0);
        for (std::size_t i = 0; i < k; ++i) {
            auto min = std::min_element(pq_list_.get(), pq_list_.get() + num_pqs(),
                                        [](auto const &lhs, auto const &rhs) { return lhs.top_key() < rhs.top_key(); });
            if (min->min_key() == sentinel) {
                break;
            }
            assert(!min->empty());
            std::pair<value_type, std::size_t> result;
            min->extract_top(result.first);
            result.second = static_cast<std::size_t>(min - std::begin(pq_list_));
            removed_elements.push_back(result);
            ++distribution[result.second];
        }
        for (auto [val, index] : removed_elements) {
            pq_list_[index].push(std::move(val));
        }
        return distribution;
    }
#endif

    constexpr size_type num_pqs() const noexcept {
        return pq_list_.get_deleter().num_pqs;
    }

    std::string description() const {
        std::stringstream ss;
        ss << "multiqueue\n\t";
        ss << "PQs: " << num_pqs() << "\n\t";
        ss << "Sentinel: " << sentinel_ << "\n\t";
        ss << "Selection strategy: " << strategy_.description() << "\n\t";
        ss << pq_type::description();
        return ss.str();
    }
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
