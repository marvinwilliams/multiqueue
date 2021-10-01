/**
******************************************************************************
* @file:   stat_counter.hpp
*
* @author: Marvin Williams
* @date:   2021/09/23 13:41
* @brief:  
*******************************************************************************
**/

#pragma once
#ifndef STAT_COUNTER_HPP_INCLUDED
#define STAT_COUNTER_HPP_INCLUDED

#include "system_config.hpp"

struct StatCount {
  alignas (2 * L1_CACHE_LINESIZE) unsigned long long num_insertions = 0;
  alignas (2 * L1_CACHE_LINESIZE) unsigned long long num_tried_deletions = 0;
  alignas (2 * L1_CACHE_LINESIZE) unsigned long long num_successuful_deletions = 0;
};

#endif  //!STAT_COUNTER_HPP_INCLUDED
