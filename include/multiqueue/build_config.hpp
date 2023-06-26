/**
******************************************************************************
* @file:   build_config.hpp
*
* @author: Marvin Williams
* @date:   2022/09/21 18:17
* @brief:
*******************************************************************************
**/
#pragma once

#include <cstddef>

namespace multiqueue::build_config {
#ifdef L1_CACHE_LINESIZE
static constexpr std::size_t L1CacheLinesize = L1_CACHE_LINESIZE;
#else
static constexpr std::size_t L1CacheLinesize = 64;
#endif
#ifdef PAGESIZE
static constexpr std::size_t Pagesize = PAGESIZE;
#else
static constexpr std::size_t Pagesize = 4096;
#endif
#ifdef MQ_COMPARE_STRICT
static constexpr bool DefaultStrictComparison = true;
#else
static constexpr bool DefaultStrictComparison = false;
#endif
#ifdef MQ_COUNT_STATS
static constexpr bool DefaultCountStats = true;
#else
static constexpr bool DefaultCountStats = false;
#endif
#ifdef MQ_NUM_POP_TRIES
static constexpr unsigned int DefaultNumPopTries = MQ_NUM_POP_TRIES;
#else
static constexpr unsigned int DefaultNumPopTries = 1;
#endif
#ifdef MQ_NOSCAN_ON_FAILED_POP
static constexpr bool DefaultScanOnFailedPop = false;
#else
static constexpr bool DefaultScanOnFailedPop = true;
#endif
#ifdef MQ_HEAP_ARITY
static constexpr unsigned int DefaultHeapArity = MQ_HEAP_ARITY;
#else
static constexpr unsigned int DefaultHeapArity = 16;
#endif
#ifdef MQ_BUFFERED_PQ_INSERTION_BUFFER_SIZE
static constexpr std::size_t DefaultInsertionBuffersize = MQ_BUFFERED_PQ_INSERTION_BUFFER_SIZE;
#else
static constexpr std::size_t DefaultInsertionBuffersize = 64;
#endif
#ifdef MQ_BUFFERED_PQ_DELETION_BUFFER_SIZE
static constexpr std::size_t DefaultDeletionBuffersize = MQ_BUFFERED_PQ_DELETION_BUFFER_SIZE;
#else
static constexpr std::size_t DefaultDeletionBuffersize = 64;
#endif
#ifdef MQ_NUM_POP_PQS
static constexpr unsigned int DefaultNumPopPQs = MQ_NUM_POP_PQS;
#else
static constexpr unsigned int DefaultNumPopPQs = 2;
#endif
}  // namespace multiqueue::build_config

#ifndef MQ_QUEUE_SELECTION_POLICY
#define MQ_QUEUE_SELECTION_POLICY 1
#endif
#if MQ_QUEUE_SELECTION_POLICY == 0
#include "multiqueue/queue_selection/random.hpp"
namespace multiqueue::build_config {
using DefaultQueueSelectionPolicy = queue_selection::Random<>;
}
#elif MQ_QUEUE_SELECTION_POLICY == 1
#include "multiqueue/queue_selection/stick_random.hpp"
namespace multiqueue::build_config {
using DefaultQueueSelectionPolicy = queue_selection::StickRandom<>;
}
#elif MQ_QUEUE_SELECTION_POLICY == 2
#include "multiqueue/queue_selection/swap_assignment.hpp"
namespace multiqueue::build_config {
using DefaultQueueSelectionPolicy = queue_selection::SwapAssignment<>;
}
#elif MQ_QUEUE_SELECTION_POLICY == 3
#include "multiqueue/queue_selection/global_permutation.hpp"
namespace multiqueue::build_config {
using DefaultQueueSelectionPolicy = queue_selection::GlobalPermutation<>;
}
#else
#error "Invalid MQ_QUEUE_SELECTION_POLICY"
#endif
