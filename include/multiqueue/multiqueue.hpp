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

#include "multiqueue/buffered_spq.hpp"
#include "multiqueue/fastrange.h"
#include "multiqueue/heap.hpp"
#include "multiqueue/ring_buffer.hpp"
#include "multiqueue/value.hpp"
#include "multiqueue/xoroshiro128plus.hpp"
#include "system_config.hpp"

#ifdef MULTIQUEUE_HAVE_NUMA
#include <numa.h>
#endif
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <type_traits>

namespace multiqueue {

struct alignas(2 * L1_CACHE_LINESIZE) ThreadData {
    xoroshiro128plus gen;
#ifdef MULTIQUEUE_HAVE_STICKINESS
    unsigned int insert_count = 0;
    unsigned int extract_count = 0;
    size_type insert_index;
    std::array<size_type, 2> extract_index;
#endif
};

template <typename Key, typename T, typename Configuration = configuration::Default,
          typename Allocator = std::allocator<Key>>
class multiqueue {
   private:
    using spq_t = buffered_spq<Key, T, Configuration>;
    static constexpr auto max_key = spq_t::max_key;

   public:
    using allocator_type = Allocator;
    using key_type = Key;
    using mapped_type = T;
    using value_type = spq_t::value;
    using size_type = std::size_t;
    struct Handle {
        friend class multiqueue;

       private:
        size_type id_;

       private:
        explicit Handle(unsigned int id) : id_{id} {
        }
    };

   private:
    using alloc_type = typename allocator_type::template rebind<spq_t>::other;
    using alloc_traits = std::allocator_traits<alloc_type>;

   private:
    spq_t *spq_;
    size_type num_spqs_;
    ThreadData *thread_data_;
    alloc_type alloc_;

   public:
    explicit multiqueue(unsigned int const num_threads, std::uint32_t seed = 1,
                        allocator_type const &alloc = allocator_type())
        : num_spqs_{num_threads * Configuration::C}, alloc_(alloc) {
        assert(num_threads >= 1);
        if constexpr (use_numa<Configuration::NumaFriendly>) {
            numa_set_interleave_mask(numa_all_nodes_ptr);
        }
        spq_ = alloc_traits::allocate(alloc_, num_spqs_);
        for (std::size_t i = 0; i < num_spqs_; ++i) {
            alloc_traits::construct(alloc_, spq_ + i);
#ifdef MULTIQUEUE_ABORT_MISALIGNED
            if (reinterpret_cast<std::uintptr_t>(&spq_[i]) % (2 * L1_CACHE_LINESIZE) != 0) {
                std::abort();
            }
#endif
        }
        if constexpr (use_numa<Configuration::NumaFriendly>) {
            numa_set_interleave_mask(numa_no_nodes_ptr);
        }
        for (std::size_t i = 0; i < num_spqs_; ++i) {
            if constexpr (use_numa<Configuration::NumaFriendly>) {
                // TODO Allocate SPQ on same numa node as its entry in the list
                numa_set_preferred(static_cast<int>(i / (num_spqs_ / (static_cast<std::size_t>(numa_max_node()) + 1))));
                spq_[i].heap.reserve_and_touch(Configuration::ReservePerQueue);
                numa_set_preferred(-1);
            }
            spq_[i].heap.reserve(Configuration::ReservePerQueue);
        }
        thread_data_ = new ThreadData[num_threads]();
        for (std::size_t i = 0; i < num_threads; ++i) {
            std::seed_seq seq{seed + i};
            thread_data_[i].gen.seed(seq);
#ifdef MULTIQUEUE_ABORT_MISALIGNED
            if (reinterpret_cast<std::uintptr_t>(&thread_data_[i]) % (2 * L1_CACHE_LINESIZE) != 0) {
                std::abort();
            }
#endif
        }
    }

    ~multiqueue() noexcept {
        for (size_type i = 0; i < num_spqs_; ++i) {
            alloc_traits::destroy(alloc_, spq_ + i);
        }
        alloc_traits::deallocate(alloc_, spq_, num_spqs_);
        delete[] thread_data_;
    }

    Handle get_handle(unsigned int id) const noexcept {
        return Handle{id};
    }

    inline size_type get_random_index(std::size_t thread_id) noexcept {
        return fastrange64(thread_data_[thread_id].gen(), num_spqs_);
    }

#ifndef MULTIQUEUE_HAVE_STICKINESS
    void push(Handle handle, value_type value) {
        size_type index = get_random_index(handle.id_);
        while (!spq_[index].try_lock()) {
            index = get_random_index(handle.id_);
        }
        spq_[index].push_and_unlock(value);
    }

    bool extract_top(Handle handle, value_type &retval) {
        size_type first_index;
        size_type second_index;
        Key first_key;
        Key second_key;

        do {
            first_index = get_random_index(handle.id_);
            second_index = get_random_index(handle.id_);
            first_key = spq_[first_index].get_min_key();
            second_key = spq_[second_index].get_min_key();
            if (second_key < first_key) {
                first_index = second_index;
                first_key = second_key;
            }
            if (first_key == max_key) {
                return false;
            }
        } while (!spq_[first_index].try_lock(first_key));
        retval = spq_[first_index].extract_min_and_unlock();
        return true;
    }

#else
    void push(Handle handle, value_type value) {
        auto index = thread_data_[handle.id_].insert_index;
        if (thread_data_[handle.id_].insert_count == 0 || !spq_[index].try_lock()) {
            do {
                index = get_random_index(handle.id_);
            } while (!spq_[index].try_lock());
            thread_data_[handle.id_].insert_count = Configuration::Stickiness;
            thread_data_[handle.id_].index = index;
        }
        spq_[index].push_and_unlock(value);
        --thread_data_[handle.id_].insert_count;
    }

    bool extract_top(Handle handle, value_type &retval) {
        Key first_index;
        Key second_index;
        if (thread_data_[handle.id_].extract_count != 0) {
            first_index = thread_data_[handle.id_].extract_index[0];
            second_index = thread_data_[handle.id_].extract_index[1];
        } else {
            first_index = get_random_index(handle.id_);
            second_index = get_random_index(handle.id_);
        }
        Key first_key = spq_[first_index].get_min_key();
        Key second_key = spq_[second_index].get_min_key();

        if (second_key < first_key) {
            first_index = second_index;
            first_key = second_key;
        }

        if (first_key == max_key) {
            thread_data_[handle.id_].extract_count = 0;
            return false;
        }

        if (!spq_[first_index].try_lock(first_key)) {
            thread_data_[handle.id_].extract_count = 0;
            do {
                first_index = thread_data_[handle.id_].get_random_index();
                second_index = thread_data_[handle.id_].get_random_index();
                first_key = spq_[first_index].get_min_key();
                second_key = spq_[second_index].get_min_key();
                if (second_key < first_key) {
                    first_index = second_index;
                    first_key = second_key;
                }

                if (first_key == max_key) {
                    return false;
                }
            } while (!spq_[first_index].try_lock(first_key));
        }

        retval = spq_[first_index].extract_min_and_unlock();
        if (thread_data_[handle.id_].extract_count == 0) {
            thread_data_[handle.id_].extract_count = Configuration::K - 1;
            thread_data_[handle.id_].extract_index = {first_index, second_index};
        } else {
            --thread_data_[handle.id_].extract_count;
        }
        return true;
    }

#endif
    bool extract_from_partition(Handle handle, value_type &retval) {
        for (size_type i = Configuration::C * handle.id_; i < Configuration::C * (handle.id_ + 1); ++i) {
            Key key = spq_[i].get_min_key();
            if (key != max_key && spq_[i].try_lock(key)) {
                retval = spq_[i].extract_top_and_unlock();
                return true;
            }
        }
        return false;
    }

    std::vector<std::size_t> get_distribution() const {
        std::vector<std::size_t> distribution(num_spqs_);
        std::transform(spq_, spq_ + num_spqs_, distribution.begin(), [](auto const &spq) { return spq.size(); });
        return distribution;
    }

    std::vector<std::size_t> get_top_distribution(std::size_t k) {
        std::vector<std::pair<value_type, std::size_t>> removed_elements;
        removed_elements.reserve(k);
        std::vector<std::size_t> distribution(num_spqs_, 0);
        for (std::size_t i = 0; i < k; ++i) {
            auto min = std::min_element(spq_, spq_ + num_spqs_, [](auto const &lhs, auto const &rhs) {
                return lhs.get_min_key() < rhs.get_min_key();
            });
            if (min->get_min_key() == max_key) {
                break;
            }
            assert(!min->empty());
            std::pair<value_type, std::size_t> result;
            [[maybe_unused]] bool success = min->try_lock();
            assert(success);
            result.first = min->extract_top_and_unlock();
            result.second = static_cast<std::size_t>(min - spq_);
            removed_elements.push_back(result);
            ++distribution[result.second];
        }
        for (auto [val, index] : removed_elements) {
            [[maybe_unused]] bool success = spq_[index].try_lock();
            assert(success);
            spq_[index].push_and_unlock(val);
        }
        return distribution;
    }

    static std::string description() {
        std::stringstream ss;
        ss << "int multiqueue\n\t";
        ss << "C: " << Configuration::C << "\n\t";
#ifdef MULTIQUEUE_HAVE_STICKINESS
        ss << "K: " << Configuration::K << "\n\t";
#endif
        ss << "Using deletion buffer with size: " << Configuration::DeletionBufferSize << "\n\t";
        ss << "Using insertion buffer with size: " << Configuration::InsertionBufferSize << "\n\t";
        ss << "Heap degree: " << Configuration::HeapDegree << "\n\t";
        if (Configuration::NumaFriendly) {
            ss << "Numa friendly\n\t";
#ifndef MULTIQUEUE_HAVE_NUMA
            ss << "But numasupport disabled!\n\t";
#endif
        }
#ifdef MULTIQUEUE_ABORT_MISALIGNED
        ss << "Abort on misalignment\n\t";
#endif
        ss << "Preallocation for " << Configuration::ReservePerQueue << " elements per internal pq";
        return ss.str();
    }
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
