/**
******************************************************************************
* @file:   config.hpp
*
* @author: Marvin Williams
* @date:   2021/03/29 17:19
* @brief:
*******************************************************************************
**/
#pragma once

#include "multiqueue/build_config.hpp"

#include <cstdint>

namespace multiqueue {

struct Config {
    std::uint64_t seed = 1;
    int c = BuildConfiguration::DefaultFactorThreadsPQ;
    int stickiness = BuildConfiguration::DefaultStickiness;
};

}  // namespace multiqueue
