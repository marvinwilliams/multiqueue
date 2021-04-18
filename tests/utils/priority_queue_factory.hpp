/**
******************************************************************************
* @file:   select_queue.hpp
*
* @author: Marvin Williams
* @date:   2021/02/22 13:23
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef TESTS_UTILS_PRIORITY_QUEUE_FACTORY_HPP_INCLUDED
#define TESTS_UTILS_PRIORITY_QUEUE_FACTORY_HPP_INCLUDED

#if defined PQ_CAPQ
#include "wrapper/capq.hpp"
#elif defined PQ_DLSM
#include "wrapper/dlsm.hpp"
#elif defined PQ_KLSM
#include "wrapper/klsm.hpp"
#elif defined PQ_LINDEN
#include "wrapper/linden.hpp"
#elif defined PQ_SPRAYLIST
#include "wrapper/spraylist.hpp"
#else
#include "multiqueue/configurations.hpp"
#include "multiqueue/multiqueue.hpp"
#endif

#include <cstdint>
#include <functional>
#include <sstream>
#include <string>
#include <type_traits>

namespace multiqueue {
namespace util {

#if defined PQ_KLSM || defined PQ_DLSM || defined PQ_CAPQ || defined PQ_CAPQ1 || defined PQ_CAPQ2 || \
    defined PQ_CAPQ3 || defined PQ_CAPQ4 || defined PQ_LINDEN || defined PQ_SPRAYLIST

#define PQ_IS_WRAPPER 1

template <typename KeyType, typename ValueType>
struct PriorityQueueFactory {};

template <>
struct PriorityQueueFactory<std::uint32_t, std::uint32_t> {
#if defined PQ_KLSM
    using type = wrapper::klsm<std::uint32_t, std::uint32_t>;
#elif defined PQ_DLSM
    using type = wrapper::dlsm<std::uint32_t, std::uint32_t>;
#elif defined PQ_CAPQ || defined PQ_CAPQ1
    using type = wrapper::capq<true, true, true>;
#elif defined PQ_CAPQ2
    using type = wrapper::capq<true, false, true>;
#elif defined PQ_CAPQ3
    using type = wrapper::capq<false, true, true>;
#elif defined PQ_CAPQ4
    using type = wrapper::capq<false, false, true>;
#elif defined PQ_LINDEN
    using type = wrapper::linden;
#elif defined PQ_SPRAYLIST
    using type = wrapper::spraylist;
#endif
};

#else

#ifdef PQ_IS_WRAPPER
#undef PQ_IS_WRAPPER
#endif

#if defined PQ_MQ_NOBUFFERING
using BaseConfig = configuration::NoBuffering;
#elif defined PQ_MQ_DELETIONBUFFER
using BaseConfig = configuration::DeletionBuffer;
#elif defined PQ_MQ_INSERTIONBUFFER
using BaseConfig = configuration::InsertionBuffer;
#elif defined PQ_MQ_FULLBUFFERING
using BaseConfig = configuration::FullBuffering;
#elif defined PQ_MQ_MERGING
using BaseConfig = configuration::Merging;
#elif defined PQ_MQ_NUMA
using BaseConfig = configuration::Numa;
#elif defined PQ_MQ_NUMAMERGING
using BaseConfig = configuration::NumaMerging;
#elif defined PQ_MQ_PHEROMONE
using BaseConfig = configuration::Pheromone;
#endif

struct Config : BaseConfig {
#ifdef MQ_CONFIG_C
    static constexpr unsigned int C = MQ_CONFIG_C;
#endif
#ifdef MQ_CONFIG_STICKINESS
    static constexpr unsigned int K = MQ_CONFIG_STICKINESS;
#endif
#ifdef MQ_CONFIG_DELETION_BUFFER_SIZE
    static constexpr unsigned int DeletionBufferSize = MQ_CONFIG_DELETION_BUFFER_SIZE;
#endif
#ifdef MQ_CONFIG_INSERTION_BUFFER_SIZE
    static constexpr unsigned int InsertionBufferSize = MQ_CONFIG_INSERTION_BUFFER_SIZE;
#endif
#ifdef MQ_CONFIG_NODE_SIZE
    static constexpr unsigned int NodeSize = MQ_CONFIG_NODE_SIZE;
#endif
#ifdef MQ_CONFIG_HEAP_DEGREE
    static constexpr unsigned int HeapDegree = MQ_HEAP_DEGREE;
#endif
};

template <typename KeyType, typename ValueType>
struct PriorityQueueFactory {
    using type = multiqueue<KeyType, ValueType, std::less<KeyType>, Config>;
};

#endif

template <typename PriorityQueue>
struct PriorityQueueTraits {
   private:
    template <typename T = PriorityQueue, typename = void>
    struct has_thread_init_impl : std::false_type {};

    template <typename T>
    struct has_thread_init_impl<T, std::void_t<decltype(std::declval<T>().init_thread(0))>> : std::true_type {};

    template <typename T = PriorityQueue, typename = void>
    struct uses_handle_impl : std::false_type {};

    template <typename T>
    struct uses_handle_impl<T, std::void_t<decltype(std::declval<T>().get_handle(0))>> : std::true_type {};

   public:
    static constexpr bool has_thread_init = has_thread_init_impl<>::value;
    static constexpr bool uses_handle = uses_handle_impl<>::value;
};

}  // namespace util
}  // namespace multiqueue

#endif  //! TESTS_UTILS_PRIORITY_QUEUE_FACTORY_HPP_INCLUDED
