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

#ifndef MULTIQUEUE_DEFAULT_PQ_STD
#include "multiqueue/heap.hpp"
#ifndef MULTIQUEUE_DEFAULT_DISABLE_BUFFERING
#include "multiqueue/buffered_pq.hpp"
#endif
#endif
#include "multiqueue/stick_policy/none.hpp"
#include "multiqueue/stick_policy/perm.hpp"
#include "multiqueue/stick_policy/random.hpp"
#include "multiqueue/stick_policy/swapping.hpp"

#ifdef MULTIQUEUE_DEFAULT_PQ_STD
#include <queue>
#endif
#include <cstddef>

namespace multiqueue {

struct BuildConfig {
    using DefaultStickPolicy =
#ifdef MULTIQUEUE_DEFAULT_STICK_POLICY_NONE
        stick_policy::None
#elif defined MULTIQUEUE_DEFAULT_STICK_POLICY_RANDOM
        stick_policy::Random
#elif defined MULTIQUEUE_DEFAULT_STICK_POLICY_SWAPPING
        stick_policy::Swapping
#elif defined MULTIQUEUE_DEFAULT_STICK_POLICY_PERMUTING
        stick_policy::Permuting
#else
        stick_policy::Swapping
#endif
        ;

    template <typename T, typename Comparator>
    using DefaultPriorityQueue =
#ifdef MULTIQUEUE_DEFAULT_PQ_STD
        std::priority_queue<T, Comparator>
#else
#ifdef MULTIQUEUE_DEFAULT_DISABLE_BUFFERING
        Heap<T, Comparator,
#ifdef MULTIQUEUE_DEFAULT_HEAP_ARITY
             MULTIQUEUE_DEFAULT_HEAP_ARITY
#else
             8
#endif
             >
#else
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
            Heap<T, Comparator,
#ifdef MULTIQUEUE_DEFAULT_HEAP_ARITY
                 MULTIQUEUE_DEFAULT_HEAP_ARITY
#else
                 8
#endif
                 >>
#endif
#endif
        ;
};

}  // namespace multiqueue

#endif  //! BUILD_CONFIG_HPP_INCLUDED
