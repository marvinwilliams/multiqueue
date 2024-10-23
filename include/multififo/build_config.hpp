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

namespace multififo::build_config {
#ifdef L1_CACHE_LINE_SIZE
static constexpr std::size_t l1_cache_line_size = L1_CACHE_LINE_SIZE;
#else
static constexpr std::size_t l1_cache_line_size = 64;
#endif
#ifdef PAGE_SIZE
static constexpr std::size_t page_size = PAGE_SIZE;
#else
static constexpr std::size_t page_size = 4096;
#endif
}  // namespace multififo::build_config
