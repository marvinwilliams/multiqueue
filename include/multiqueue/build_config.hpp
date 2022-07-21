/**
******************************************************************************
* @file:   build_config.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 18:41
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef BUILD_CONFIG_HPP_INCLUDED
#define BUILD_CONFIG_HPP_INCLUDED

#ifndef L1_CACHE_LINESIZE
#define L1_CACHE_LINESIZE 64
#endif

#ifndef MULTIQUEUE_DEFAULT_DISABLE_BUFFERING
#include "multiqueue/buffered_pq.hpp"
#endif
#ifndef MULTIQUEUE_DEFAULT_PQ_STD
#include "multiqueue/heap.hpp"
#endif
#include "multiqueue/stick_policies.hpp"

#ifdef MULTIQUEUE_DEFAULT_PQ_STD
#include <queue>
#endif
#include <cstddef>

namespace multiqueue {

struct BuildConfig {
    static constexpr StickPolicy DefaultStickPolicy =
#ifdef MULTIQUEUE_DEFAULT_STICK_POLICY_NONE
        StickPolicy::None
#elif defined MULTIQUEUE_DEFAULT_STICK_POLICY_RANDOM
        StickPolicy::Random
#elif defined MULTIQUEUE_DEFAULT_STICK_POLICY_SWAPPING
        StickPolicy::Swapping
#elif defined MULTIQUEUE_DEFAULT_STICK_POLICY_PERMUTING
        StickPolicy::Permuting
#else
        StickPolicy::None
#endif
        ;

    template <typename T, typename Comparator>
    using DefaultPriorityQueue =
#ifndef MULTIQUEUE_DEFAULT_DISABLE_BUFFERING
        BufferedPQ<
#ifdef MULTIQUEUE_DEFAULT_INSERTION_BUFFERSIZE
            MULTIQUEUE_DEFAULT_INSERTION_BUFFERSIZE
#else
            64
#endif
            ,
#ifdef MULTIQUEUE_DEFAULT_DELETION_BUFFERSIZE
            MULTIQUEUE_DEFAULT_DELETION_BUFFERSIZE
#else
            64
#endif
            ,
#endif
#ifdef MULTIQUEUE_DEFAULT_PQ_STD
            std::priority_queue<T, std::vector<T>, Comparator>
#else
        Heap<T, Comparator,
#ifdef MULTIQUEUE_DEFAULT_HEAP_ARITY
             MULTIQUEUE_DEFAULT_HEAP_ARITY
#else
             8
#endif
             >
#endif
#ifndef MULTIQUEUE_DEFAULT_DISABLE_BUFFERING
            >
#endif
        ;
};

}  // namespace multiqueue

#endif  //! BUILD_CONFIG_HPP_INCLUDED
