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
#ifndef UTILS_SELECT_QUEUE_HPP_INCLUDED
#define UTILS_SELECT_QUEUE_HPP_INCLUDED

#include "multiqueue/default_configuration.hpp"

#include <cstdint>
#include <functional>
#include <sstream>
#include <string>
#include <type_traits>

#if defined PQ_CAPQ || defined PQ_CAPQ1 || defined PQ_CAPQ2 || defined PQ_CAPQ3 || defined PQ_CAPQ4
#include "capq.hpp"
#elif defined PQ_LINDEN
#include "linden.hpp"
#elif defined PQ_SPRAYLIST
#include "spraylist.hpp"
#elif defined PQ_KLSM
#include "k_lsm/k_lsm.h"
#include "klsm.hpp"
#elif defined PQ_DLSM
#include "dist_lsm/dist_lsm.h"
#include "klsm.hpp"
#elif defined PQ_NBMQ
#include "multiqueue/no_buffer_mq.hpp"
#elif defined PQ_TBMQ
#include "multiqueue/top_buffer_mq.hpp"
#elif defined PQ_DBMQ
#include "multiqueue/deletion_buffer_mq.hpp"
#elif defined PQ_IDMQ
#include "multiqueue/ins_del_buffer_mq.hpp"
#elif defined PQ_LQMQ
#include "multiqueue/local_queue_mq.hpp"
#elif defined PQ_MMQ
#include "multiqueue/merge_mq.hpp"
#elif defined PQ_NAMQ
#include "multiqueue/numa_aware_mq.hpp"
#elif defined PQ_NAMMQ
#include "multiqueue/numa_aware_merge_mq.hpp"
#else
#error No supported priority queue defined!
#endif

namespace util {

struct Config : multiqueue::DefaultConfiguration {
#ifdef MQ_C
    static constexpr unsigned int C = MQ_C;
#endif
#ifdef MQ_HEAP_DEGREE
    static constexpr unsigned int HeapDegree = MQ_HEAP_DEGREE;
#endif
#ifdef MQ_IBUFFER_SIZE
    static constexpr unsigned int InsertionBufferSize = MQ_IBUFFER_SIZE;
#endif
#ifdef MQ_DBUFFER_SIZE
    static constexpr unsigned int DeletionBufferSize = MQ_DBUFFER_SIZE;
#endif
#ifdef MQ_HEAP_NODESIZE
    static constexpr unsigned int NodeSize = MQ_HEAP_NODESIZE;
#endif
};

template <typename KeyType, typename ValueType>
struct BaseQueueSelector {
#if defined PQ_KLSM
    using queue_type = multiqueue::wrapper::klsm<kpq::k_lsm<KeyType, ValueType, 256>>;
#elif defined PQ_DLSM
    using queue_type = multiqueue::wrapper::klsm<kpq::dist_lsm<KeyType, ValueType, 256>>;
#elif defined PQ_NBMQ
    using queue_type = multiqueue::no_buffer_mq<KeyType, ValueType, std::less<KeyType>, Config>;
#elif defined PQ_TBMQ
    using queue_type = multiqueue::top_buffer_mq<KeyType, ValueType, std::less<KeyType>, Config>;
#elif defined PQ_DBMQ
    using queue_type = multiqueue::deletion_buffer_mq<KeyType, ValueType, std::less<KeyType>, Config>;
#elif defined PQ_IDMQ
    using queue_type = multiqueue::ins_del_buffer_mq<KeyType, ValueType, std::less<KeyType>, Config>;
#elif defined PQ_LQMQ
    using queue_type = multiqueue::local_queue_mq<KeyType, ValueType, std::less<KeyType>, Config>;
#elif defined PQ_MMQ
    using queue_type = multiqueue::merge_mq<KeyType, ValueType, std::less<KeyType>, Config>;
#elif defined PQ_NAMQ
    using queue_type = multiqueue::numa_aware_mq<KeyType, ValueType, std::less<KeyType>, Config>;
#elif defined PQ_NAMMQ
    using queue_type = multiqueue::numa_aware_merge_mq<KeyType, ValueType, std::less<KeyType>, Config>;
#endif
};

template <typename KeyType, typename ValueType>
struct QueueSelector : BaseQueueSelector<KeyType, ValueType> {};

template <>
struct QueueSelector<std::uint32_t, std::uint32_t> : BaseQueueSelector<std::uint32_t, std::uint32_t> {
#if defined PQ_CAPQ || defined PQ_CAPQ1
    using queue_type = multiqueue::wrapper::capq<true, true, true>;
#elif defined PQ_CAPQ2
    using queue_type = multiqueue::wrapper::capq<true, false, true>;
#elif defined PQ_CAPQ3
    using queue_type = multiqueue::wrapper::capq<false, true, true>;
#elif defined PQ_CAPQ4
    using queue_type = multiqueue::wrapper::capq<false, false, true>;
#elif defined PQ_LINDEN
    using queue_type = multiqueue::wrapper::linden;
#elif defined PQ_SPRAYLIST
    using queue_type = multiqueue::wrapper::spraylist;
#endif
};


template <typename Queue>
struct QueueTraits {
   private:
    template <typename T, typename = void>
    struct has_thread_init_impl : std::false_type {};

    template <typename T>
    struct has_thread_init_impl<T, std::void_t<decltype(std::declval<T>().init_thread(static_cast<std::size_t>(0)))>>
        : std::true_type {};

   public:
    static constexpr bool has_thread_init = has_thread_init_impl<Queue>::value;
};

std::string queue_name() {
#if defined PQ_KLSM
    return "klsm";
#elif defined PQ_DLSM
    return "dlsm";
#elif defined PQ_NBMQ
    return "nbmq";
#elif defined PQ_TBMQ
    return "tbmq";
#elif defined PQ_DBMQ
    return "dbmq";
#elif defined PQ_IDMQ
    return "idmq";
#elif defined PQ_LQMQ
    return "lqmq";
#elif defined PQ_MMQ
    return "mmq";
#elif defined PQ_NAMQ
    return "namq";
#elif defined PQ_NAMMQ
    return "nammq";
#elif defined PQ_CAPQ || defined PQ_CAPQ1
    return "capq1";
#elif defined PQ_CAPQ2
    return "capq2";
#elif defined PQ_CAPQ3
    return "capq3";
#elif defined PQ_CAPQ4
    return "capq4";
#elif defined PQ_LINDEN
    return "linden";
#elif defined PQ_SPRAYLIST
    return "spraylist";
#endif
    return "";
}

std::string config_string() {
    std::stringstream ss;
    ss << "C: " << Config::C << ", "
       << "InsertionBufferSize: " << Config::InsertionBufferSize << ", "
       << "DeletionBufferSize: " << Config::DeletionBufferSize << ", "
       << "NodeSize: " << Config::NodeSize << '\n';
    return ss.str();
}

}  // namespace util

#endif  //! UTILS_SELECT_QUEUE_HPP_INCLUDED
