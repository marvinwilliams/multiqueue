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

namespace multiqueue {

struct BuildConfiguration {
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
    static constexpr int DefaultFactorThreadsPQ = 4;
    static constexpr int DefaultStickiness = 16;
};

}  // namespace multiqueue
