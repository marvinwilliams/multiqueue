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

#include "multiqueue/default_configuration.hpp"
#include "multiqueue/external/xoroshiro128plus.hpp"
#include "multiqueue/guarded_pq.hpp"
#include "multiqueue/heap.hpp"

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

#ifndef L1_CACHE_LINESIZE
#define L1_CACHE_LINESIZE 64
#endif

#ifndef PAGESIZE
#define PAGESIZE 4096
#endif

namespace multiqueue {

template <typename Key, typename T = void, typename Compare = std::less<Key>, typename Allocator = std::allocator<Key>,
          typename Configuration = multiqueue::StickySelectionConfiguration>
class alignas(2 * L1_CACHE_LINESIZE) Multiqueue {
   private:
    using this_t = Multiqueue<Key, T, Compare, Allocator, Configuration>;
    using pq_type = GuardedPQ<Key, T, Compare, Allocator, Configuration>;

   public:
    using key_type = typename pq_type::key_type;
    using value_type = typename pq_type::value_type;
    using key_compare = typename pq_type::key_compare;
    using value_compare = typename pq_type::value_compare;
    using allocator_type = Allocator;
    using reference = typename pq_type::reference;
    using const_reference = typename pq_type::const_reference;
    using size_type = typename pq_type::size_type;
    using configuration = Configuration;

   private:
    using selection_strategy = typename Configuration::selection_strategy;
    friend selection_strategy;
    using shared_data_type = typename selection_strategy::shared_data_t;

   public:
    class alignas(2 * L1_CACHE_LINESIZE) Handle {
        friend this_t;
        using data_t = typename selection_strategy::thread_data_t;

        alignas(2 * L1_CACHE_LINESIZE) data_t data_;
        std::reference_wrapper<Multiqueue> mq_;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        Handle(Handle &&) = default;

       private:
        explicit constexpr Handle(this_t &mq) noexcept : data_{mq.rng_()}, mq_{mq} {
        }

       public:
        bool try_extract_top(reference retval) {
            pq_type *pq = selection_strategy::lock_delete_pq(mq_.get(), data_);
            if (!pq) {
                return false;
            }
            pq->extract_top(retval);
            pq->unlock();
            return true;
        }

        void push(const_reference value) {
            pq_type *pq = selection_strategy::lock_push_pq(mq_.get(), data_);
            assert(pq);
            pq->push(value);
            pq->unlock();
        }

        void push(value_type &&value) {
            pq_type *pq = selection_strategy::lock_push_pq(mq_.get(), data_);
            assert(pq);
            pq->push(std::move(value));
            pq->unlock();
        }

        bool try_extract_from(size_type pos, value_type &retval) {
            assert(pos < mq_.get().num_pqs_);
            key_type key = mq_.get().pq_list_[pos].concurrent_top_key();
            if (key != sentinel && mq_.get().pq_list_[pos].try_lock_if_key(key)) {
                mq_.pq_list_[pos].extract_top(retval);
                return true;
            }
            return false;
        }
    };

   public:
    using pq_alloc_type = typename std::allocator_traits<allocator_type>::template rebind_alloc<pq_type>;
    using pq_alloc_traits = std::allocator_traits<pq_alloc_type>;

    static inline key_type sentinel = Configuration::template sentinel<key_type>::value;

   private:
    // No padding needed, as these members are not written to concurrently (false sharing is avoided by the alignment of
    // the class itself)
    pq_type *pq_list_;
    size_type num_pqs_;
    xoroshiro128plus rng_;
    [[no_unique_address]] key_compare comp_;
    [[no_unique_address]] shared_data_type shared_data_;
    [[no_unique_address]] pq_alloc_type alloc_;

    void abort_on_data_misalignment() {
        for (pq_type *s = pq_list_; s != pq_list_ + num_pqs_; ++s) {
            if (reinterpret_cast<std::uintptr_t>(s) % (2 * L1_CACHE_LINESIZE) != 0) {
                std::abort();
            }
        }
    }

   public:
    explicit Multiqueue(unsigned int num_threads, Configuration const &config = Configuration(),
                        key_compare const &comp = key_compare(), allocator_type const &alloc = allocator_type())
        : num_pqs_{num_threads * config.c}, rng_(config.seed), comp_(comp), shared_data_(), alloc_(alloc) {
        if (num_threads == 0) {
            throw std::invalid_argument("num_threads cannot be 0");
        }
        if (config.c == 0) {
            throw std::invalid_argument("c cannot be 0");
        }
        pq_list_ = pq_alloc_traits::allocate(alloc_, num_pqs_);
        for (pq_type *pq = pq_list_; pq != pq_list_ + num_pqs_; ++pq) {
            pq_alloc_traits::construct(alloc_, pq, comp);
        }
#ifdef MQ_ABORT_MISALIGNMENT
        abort_on_data_misalignment();
#endif
    }

    explicit Multiqueue(unsigned int num_threads, Configuration const &config, allocator_type const &alloc)
        : Multiqueue(num_threads, config, key_compare(), alloc) {
    }

    explicit Multiqueue(unsigned int num_threads, key_compare const &comp,
                        allocator_type const &alloc = allocator_type())
        : Multiqueue(num_threads, Configuration(), comp, alloc) {
    }

    explicit Multiqueue(unsigned int num_threads, allocator_type const &alloc)
        : Multiqueue(num_threads, Configuration(), key_compare(), alloc) {
    }

    ~Multiqueue() noexcept {
        for (pq_type *pq = pq_list_; pq != pq_list_ + num_pqs_; ++pq) {
            pq_alloc_traits::destroy(alloc_, pq);
        }
        pq_alloc_traits::deallocate(alloc_, pq_list_, num_pqs_);
    }

    constexpr Handle get_handle() noexcept {
        return Handle(*this);
    }

    void reserve_per_spq(size_type cap) {
        for (pq_type *pq = pq_list_; pq != pq_list_ + num_pqs_; ++pq) {
            pq->reserve(cap);
        };
    }

#ifdef MQ_ELEMENT_DISTRIBUTION
    std::vector<std::size_t> get_distribution() const {
        std::vector<std::size_t> distribution(num_pqs_);
        std::transform(pq_list_, pq_list_ + num_pqs_, distribution.begin(), [](auto const &spq) { return spq.size(); });
        return distribution;
    }

    std::vector<std::size_t> get_top_distribution(std::size_t k) {
        std::vector<std::pair<value_type, std::size_t>> removed_elements;
        removed_elements.reserve(k);
        std::vector<std::size_t> distribution(num_pqs_, 0);
        for (std::size_t i = 0; i < k; ++i) {
            auto min = std::min_element(pq_list_, pq_list_ + num_pqs_, [](auto const &lhs, auto const &rhs) {
                return lhs.get_min_key() < rhs.get_min_key();
            });
            if (min->get_min_key() == empty_key) {
                break;
            }
            assert(!min->empty());
            std::pair<value_type, std::size_t> result;
            [[maybe_unused]] bool success = min->try_lock();
            assert(success);
            result.first = min->extract_top_and_unlock();
            result.second = static_cast<std::size_t>(min - pq_list_);
            removed_elements.push_back(result);
            ++distribution[result.second];
        }
        for (auto [val, index] : removed_elements) {
            [[maybe_unused]] bool success = pq_list_[index].try_lock();
            assert(success);
            pq_list_[index].push_and_unlock(val);
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
        ss << "# PQs: " << num_pqs_ << "\n\t";
        ss << "Deletion buffer size: " << Configuration::DeletionBufferSize << "\n\t";
        ss << "Insertion buffer size: " << Configuration::InsertionBufferSize << "\n\t";
        ss << "Heap degree: " << Configuration::HeapDegree << "\n\t";
        ss << "Selection strategy: " << shared_data_.description();
        return ss.str();
    }
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
