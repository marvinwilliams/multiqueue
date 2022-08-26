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

#include "multiqueue/build_config.hpp"

#include "multiqueue/guarded_pq.hpp"
#include "multiqueue/multiqueue_impl.hpp"
#include "multiqueue/sentinel_traits.hpp"
#include "multiqueue/stick_policies.hpp"
#include "multiqueue/value_traits.hpp"

#include <allocator>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>

#ifdef MULTIQUEUE_ELEMENT_DISTRIBUTION
#include <algorithm>
#include <utility>
#include <vector>
#endif

#ifndef L1_CACHE_LINESIZE
#error Need to define L1_CACHE_LINESIZE
#endif

namespace multiqueue {

template <typename Key, typename T, typename KeyCompare = std::less<>, StickPolicy S = BuildConfig::DefaultStickPolicy,
          template <typename, typename> typename PriorityQueue = BuildConfig::DefaultPriorityQueue,
          typename ValueTraits = value_traits<Key, T>, typename SentinelTraits = sentinel_traits<Key, KeyCompare>,
          typename Allocator = std::allocator<typename ValueTraits::value_type>>
class MultiQueue {
   private:
    using impl_type = MultiQueueImpl<Key, T, KeyCompare, PriorityQueue, ValueTraits, SentinelTraits, S>;

   public:
    using key_type = typename impl_type::key_type;
    using mapped_type = typename impl_type::mapped_type;
    using value_type = typename impl_type::value_type;
    using key_compare = typename impl_type::key_compare;
    using value_compare = typename impl_type::value_compare;
    using reference = typename impl_type::reference;
    using const_reference = typename impl_type::const_reference;
    using size_type = typename impl_type::size_type;
    using allocator_type = typename impl_type::allocator_type;
    using config_type = typename impl_type::Config;
    using handle_type = typename impl_type::Handle;

   private:
    // False sharing is avoided by class alignment, but the members do not need to reside in individual cache lines,
    // as they are not written concurrently
    impl_type impl_;

   public:
    explicit MultiQueue(unsigned int num_threads, config_type const &config, key_compare const &comp = key_compare(),
                        allocator_type const &alloc = allocator_type())
        : impl_{num_threads, config, comp, alloc} {
    }

    handle_type get_handle() noexcept {
        return impl_.get_handle();
    }

    bool try_pop(reference retval) noexcept {
        return impl_.try_pop(retval);
    }

    void push(const_reference value) noexcept {
        impl_.push(value);
    }

    constexpr size_type num_pqs() const noexcept {
        return impl_.num_pqs();
    }

    value_compare value_comp() const {
        return impl_.value_comp();
    }
};

}  // namespace multiqueue

#endif  //! MULTIQUEUE_HPP_INCLUDED
