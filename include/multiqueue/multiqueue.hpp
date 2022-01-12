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

#include "multiqueue/external/fastrange.h"

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
    std::size_t c = 4;
};

template <typename Key, typename T, typename ExtractKey, typename Compare, typename Sentinel,
          typename SelectionStrategy, bool ImplicitLock, typename PriorityQueue, typename Allocator>
class Multiqueue {
   private:
    using guarded_pq_type = GuardedPQ<Key, T, ExtractKey, Compare, Sentinel, ImplicitLock, PriorityQueue>;

   public:
    using key_type = typename guarded_pq_type::key_type;
    using value_type = typename guarded_pq_type::value_type;
    using key_compare = typename guarded_pq_type::key_compare;
    using reference = typename guarded_pq_type::reference;
    using const_reference = typename guarded_pq_type::const_reference;
    using size_type = typename guarded_pq_type::size_type;
    using allocator_type = Allocator;
    using param_type = MultiqueueParameters<SelectionStrategy>;

   private:
    using Selector = typename SelectionStrategy::template Strategy<Multiqueue>;

   public:
    class alignas(2 * L1_CACHE_LINESIZE) Handle {
        friend Multiqueue;
        using data_t = typename Selector::handle_data_t;

        Multiqueue &mq_;
        data_t data_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit Handle(Multiqueue &mq, unsigned int id, std::uint64_t seed) noexcept : mq_{mq}, data_{seed, id} {
        }

       public:
        bool try_extract_top(reference retval) noexcept {
            {
                auto const [first, second] = mq_.selector_.get_delete_pqs(data_);
                auto const [pq, key] = mq_.select_delete_pq(mq_.pq_list.get() + first, mq_.pq_list.get() + second);
                if (!pq) {
                    // Both pqs are empty
                    mq_.selector_.delete_pq_empty(data_);
                    return false;
                }
                if (pq->try_lock_if_key(key)) {
                    pq->extract_top(retval);
                    pq->unlock();
                    mq_.selector_.delete_pq_used(true, data_);
                    return true;
                }
            }
            do {
                auto const [first, second] = mq_.selector_.get_fallback_delete_pqs(data_);
                auto const [pq, key] = mq_.select_delete_pq(mq_.pq_list.get() + first, mq_.pq_list.get() + second);
                if (!pq) {
                    // Both pqs are empty
                    mq_.selector_.delete_pq_empty(data_);
                    return false;
                }
                if (pq->try_lock_if_key(key)) {
                    pq->extract_top(retval);
                    pq->unlock();
                    mq_.selector_.delete_pq_used(false, data_);
                    return true;
                }
            } while (true);
        }

        void push(const_reference value) noexcept {
            {
                auto const pq = mq_.pq_list.get() + mq_.selector_.get_push_pq(data_);
                if (pq->try_lock()) {
                    pq->push(value);
                    pq->unlock();
                    mq_.selector_.push_pq_used(true, data_);
                    return;
                }
            }
            do {
                auto const pq = mq_.pq_list.get() + mq_.selector_.get_fallback_push_pq(data_);
                if (pq->try_lock()) {
                    pq->push(value);
                    pq->unlock();
                    mq_.selector_.push_pq_used(false);
                    return;
                }
            } while (true);
        }

        void push(key_type &&value) noexcept {
            {
                auto const pq = mq_.pq_list.get() + mq_.selector_.get_push_pq(data_);
                if (pq->try_lock()) {
                    pq->push(std::move(value));
                    pq->unlock();
                    mq_.selector_.push_pq_used(true);
                    return;
                }
            }
            do {
                auto const pq = mq_.pq_list.get() + mq_.selector_.get_fallback_push_pq(data_);
                if (pq->try_lock()) {
                    pq->push(std::move(value));
                    pq->unlock();
                    mq_.selector_.push_pq_used(false);
                    return;
                }
            } while (true);
        }

        bool try_extract_from(size_type pos, value_type &retval) noexcept {
            assert(pos < mq_.num_pqs());
            auto &pq = mq_.pq_list_[pos];
            if (pq.try_lock()) {
                if (!pq.unsafe_empty()) {
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
    using pq_alloc_type = typename std::allocator_traits<allocator_type>::template rebind_alloc<guarded_pq_type>;
    using pq_alloc_traits = typename std::allocator_traits<pq_alloc_type>;

    struct PQDeleter {
        Multiqueue const &mq;

        PQDeleter(Multiqueue const &mq_ref) : mq{mq_ref} {
        }

        void operator()(guarded_pq_type *pq_list) noexcept {
            for (guarded_pq_type *s = pq_list; s != pq_list + mq.num_pqs_; ++s) {
                pq_alloc_traits::destroy(mq.alloc_, s);
            }
            pq_alloc_traits::deallocate(mq.alloc_, pq_list, mq.num_pqs_);
        }
    };

   private:
    // False sharing is avoided by class alignment, but the members do not need to reside in individual cache lines, as
    // they are not written concurrently
    std::unique_ptr<guarded_pq_type[], PQDeleter> pq_list_;
    size_type num_pqs_;
    [[no_unique_address]] key_compare comp_;
    [[no_unique_address]] pq_alloc_type alloc_;
    std::unique_ptr<std::uint64_t[]> handle_seeds_;
    std::atomic_uint handle_index_ = 0;

    // strategy data in separate cache line, as it might be written to
    [[no_unique_address]] alignas(2 * L1_CACHE_LINESIZE) Selector selector_;

    void abort_on_data_misalignment() {
        for (guarded_pq_type *s = pq_list_.get(); s != pq_list_.get() + num_pqs(); ++s) {
            if (reinterpret_cast<std::uintptr_t>(s) % (2 * L1_CACHE_LINESIZE) != 0) {
                std::abort();
            }
        }
    }

    std::pair<pq_type *, key_type> select_delete_pq(size_type first, size_type second) noexcept {
        auto const first_key = (pq_list_.get() + first)->top_key();
        auto const second_key = (pq_list_.get() + second)->top_key();
        if (first_key == get_sentinel() && second_key == get_sentinel()) {
            return {nullptr, get_sentinel()};
        }
        if (second_key == get_sentinel() || comp_(first_key, second_key)) {
            return {(pq_list_.get() + first), first_key};
        } else {
            return {(pq_list_.get() + second), second_key};
        }
    }

   public:
    explicit Multiqueue(unsigned int num_threads, param_type const &param, key_compare const &comp = key_compare(),
                        allocator_type const &alloc = allocator_type())
        : pq_list_(nullptr, PQDeleter(this)),
          num_pqs_{num_threads * param.c},
          comp_{comp},
          alloc_{alloc},
          selector_(*this, param) {
        assert(num_threads > 0);
        assert(param.c > 0);
        guarded_pq_type *pq_list = pq_alloc_traits::allocate(alloc_, num_pqs_);
        for (guarded_pq_type *s = pq_list; s != pq_list + num_pqs_; ++s) {
            pq_alloc_traits::construct(alloc_, comp_);
        }
        pq_list_.reset(pq_list);  // Empty unique_ptr does not call deleter
#ifdef MQ_ABORT_MISALIGNMENT
        abort_on_data_misalignment();
#endif
        handle_seeds_ = std::make_unique<std::uint64_t[]>(num_threads);
        std::generate(handle_seeds_.get(), handle_seeds_.get() + num_threads, xoroshiro256starstar{param.seed});
    }

    Handle get_handle() noexcept {
        unsigned int index = handle_index_.fetch_add(1, std::memory_order_relaxed);
        return Handle(*this, index, handle_seeds_[index]);
    }

    static constexpr key_type get_sentinel() noexcept {
        return guarded_pq_type::get_sentinel();
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
            if (min->min_key() == Sentinel()()) {
                break;
            }
            assert(!min->empty());
            std::pair<value_type, std::size_t> result;
            min->extract_top(ExtractKey()(result));
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
        return num_pqs_;
    }

    std::string description() const {
        std::stringstream ss;
        ss << "multiqueue\n\t";
        ss << "PQs: " << num_pqs() << "\n\t";
        ss << "Sentinel: " << Sentinel()() << "\n\t";
        ss << "Selection strategy: " << selector_.description() << "\n\t";
        ss << guarded_pq_type::description();
        return ss.str();
    }
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
