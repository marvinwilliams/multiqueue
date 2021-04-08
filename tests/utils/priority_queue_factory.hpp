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

#include "multiqueue/configurations.hpp"
#include "wrapper/capq.hpp"
#include "wrapper/dlsm.hpp"
#include "wrapper/klsm.hpp"
#include "wrapper/linden.hpp"
#include "wrapper/spraylist.hpp"

#include <cstdint>
#include <functional>
#include <sstream>
#include <string>
#include <type_traits>

namespace util {

#if defined PQ_KLSM || defined PQ_DLSM || defined PQ_CAPQ || defined PQ_CAPQ1 || defined PQ_CAPQ2 || \
    defined PQ_CAPQ3 || defined PQ_CAPQ4 || defined PQ_LINDEN || defined PQ_SPRAYLIST

#define PQ_IS_WRAPPER 1

template <typename KeyType, typename ValueType>
struct PriorityQueueFactory {};

template <>
struct PriorityQueueFactory<std::uint32_t, std::uint32_t> {
#if defined PQ_KLSM
    using type = multiqueue::wrapper::klsm<std::uint32_t, std::uint32_t>;
#elif defined PQ_DLSM
    using type = multiqueue::wrapper::dlsm<std::uint32_t, std::uint32_t>;
#elif defined PQ_CAPQ || defined PQ_CAPQ1
    using type = multiqueue::wrapper::capq<true, true, true>;
#elif defined PQ_CAPQ2
    using type = multiqueue::wrapper::capq<true, false, true>;
#elif defined PQ_CAPQ3
    using type = multiqueue::wrapper::capq<false, true, true>;
#elif defined PQ_CAPQ4
    using type = multiqueue::wrapper::capq<false, false, true>;
#elif defined PQ_LINDEN
    using type = multiqueue::wrapper::linden;
#elif defined PQ_SPRAYLIST
    using type = multiqueue::wrapper::spraylist;
#endif
};

#else

#ifdef PQ_IS_WRAPPER
#undef PQ_IS_WRAPPER
#endif

#if defined PQ_MQ_NOBUFFERING
using BaseConfig = ::multiqueue::configuration::NoBuffer;
#elif defined PQ_MQ_DELETIONBUFFER
using BaseConfig = ::multiqueue::configuration::DeletionBuffer;
#elif defined PQ_MQ_INSERTIONBUFFER
using BaseConfig = ::multiqueue::configuration::InsertionBuffer;
#elif defined PQ_MQ_FULLBUFFERING
using BaseConfig = ::multiqueue::configuration::FullBuffering;
#elif defined PQ_MQ_MERGING
using BaseConfig = ::multiqueue::configuration::Merging;
#elif defined PQ_MQ_NUMA
using BaseConfig = ::multiqueue::configuration::Numa;
#elif defined PQ_MQ_NUMAMERGING
using BaseConfig = ::multiqueue::configuration::NumaMerging;
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
    using type = ::multiqueue::multiqueue<KeyType, ValueType, std::less<KeyType>, Config>;
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

#endif  //! TESTS_UTILS_PRIORITY_QUEUE_FACTORY_HPP_INCLUDED
