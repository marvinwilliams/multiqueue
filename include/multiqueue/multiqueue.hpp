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

#include "multiqueue/buffered_pq.hpp"
#include "multiqueue/configurations.hpp"
#include "multiqueue/heap.hpp"
#include "multiqueue/ring_buffer.hpp"
#include "multiqueue/value.hpp"
#include "system_config.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#ifndef L1_CACHE_LINESIZE
#error Need to define L1_CACHE_LINESIZE
#endif

namespace multiqueue {

template <typename Key, typename T, typename Configuration = multiqueue::DefaultConfiguration,
          typename Allocator = std::allocator<Key>>
class Multiqueue {
   private:
    using this_t = Multiqueue<Key, T, Configuration, Allocator>;
    using spq_t = BufferedPQ<Key, T, Configuration>;
    using selection_strategy = typename Configuration::template selection_strategy<this_t>;
    friend selection_strategy;

   public:
    using allocator_type = Allocator;
    using key_type = Key;
    using mapped_type = T;
    using value_type = typename spq_t::value_type;
    using size_type = std::size_t;

    class Handle {
        friend this_t;

       public:
        Handle(Handle const &) = delete;
        Handle &operator=(Handle const &) = delete;
        alignas(2 * L1_CACHE_LINESIZE) typename selection_strategy::thread_data_t data;

       private:
        explicit Handle(this_t &mq) : data{mq} {
        }
    };

    static constexpr key_type min_valid_key = spq_t::min_valid_key;
    static constexpr key_type max_valid_key = spq_t::max_valid_key;

   private:
    using alloc_type = typename allocator_type::template rebind<spq_t>::other;
    using alloc_traits = std::allocator_traits<alloc_type>;

    static constexpr key_type empty_key = spq_t::empty_key;

   private:
    // TODO: No padding needed, as these members are not written to concurrently?
    spq_t *spqs_;
    size_type num_spqs_;
    size_type num_threads_;
    [[no_unique_address]] typename selection_strategy::shared_data_t selection_strategy_data_;
    [[no_unique_address]] alloc_type alloc_;

   public:
    template <typename... Args>
    explicit Multiqueue(unsigned int num_threads, std::size_t c, allocator_type const &alloc, Args &&...args)
        : num_spqs_{num_threads * c},
          num_threads_{num_threads},
          selection_strategy_data_{std::forward<Args>(args)...},
          alloc_(alloc) {
        assert(num_threads >= 1);
        spqs_ = alloc_traits::allocate(alloc_, num_spqs_);
        for (spq_t *s = spqs_; s != spqs_ + num_spqs_; ++s) {
            if (reinterpret_cast<std::uintptr_t>(s) % (2 * L1_CACHE_LINESIZE) != 0) {
                std::abort();
            }
            alloc_traits::construct(alloc_, s);
        }
    }

    template <typename... Args>
    explicit Multiqueue(unsigned int num_threads, std::size_t c, Args &&...args)
        : Multiqueue(num_threads, c, static_cast<allocator_type const &>(allocator_type()),
                     std::forward<Args>(args)...) {
    }

    ~Multiqueue() noexcept {
        for (spq_t *s = spqs_; s != spqs_ + num_spqs_; ++s) {
            alloc_traits::destroy(alloc_, s);
        }
        alloc_traits::deallocate(alloc_, spqs_, num_spqs_);
    }

    inline Handle get_handle() {
        return Handle(*this);
    }

    void push(Handle &handle, value_type value) {
        spq_t *s = selection_strategy::get_locked_insert_spq(*this, handle.data);
        assert(s);
        s->push_and_unlock(value);
    }

    bool try_delete_min(Handle &handle, value_type &retval) {
        spq_t *s = selection_strategy::get_locked_delete_spq(*this, handle.data);
        if (s) {
            retval = s->extract_min_and_unlock();
            return true;
        }
        return false;
    }

    bool try_delete_first_from(size_type begin, size_type end, value_type &retval) {
        for (spq_t *s = spqs_ + begin; s != spqs_ + end; ++s) {
            key_type key = s->get_min_key();
            if (key != empty_key && s->try_lock(key)) {
                retval = s->extract_top_and_unlock();
                return true;
            }
        }
        return false;
    }

    std::vector<std::size_t> get_distribution() const {
        std::vector<std::size_t> distribution(num_spqs_);
        std::transform(spqs_, spqs_ + num_spqs_, distribution.begin(), [](auto const &spq) { return spq.size(); });
        return distribution;
    }

    std::vector<std::size_t> get_top_distribution(std::size_t k) {
        std::vector<std::pair<value_type, std::size_t>> removed_elements;
        removed_elements.reserve(k);
        std::vector<std::size_t> distribution(num_spqs_, 0);
        for (std::size_t i = 0; i < k; ++i) {
            auto min = std::min_element(spqs_, spqs_ + num_spqs_, [](auto const &lhs, auto const &rhs) {
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
            result.second = static_cast<std::size_t>(min - spqs_);
            removed_elements.push_back(result);
            ++distribution[result.second];
        }
        for (auto [val, index] : removed_elements) {
            [[maybe_unused]] bool success = spqs_[index].try_lock();
            assert(success);
            spqs_[index].push_and_unlock(val);
        }
        return distribution;
    }

    inline void reserve_per_spq(std::size_t cap) {
        std::for_each(spqs_, spqs_ + num_spqs_, [cap](auto &spq) { spq.heap_reserve(cap); });
    }

    std::string description() const {
        std::stringstream ss;
        ss << "multiqueue\n\t";
        ss << "C: " << num_spqs_ / num_threads_ << "\n\t";
        ss << "Deletion buffer size: " << Configuration::DeletionBufferSize << "\n\t";
        ss << "Insertion buffer size: " << Configuration::InsertionBufferSize << "\n\t";
        ss << "Heap degree: " << Configuration::HeapDegree << "\n\t";
        ss << "Selection strategy: " << selection_strategy::description(selection_strategy_data_);
        return ss.str();
    }
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
