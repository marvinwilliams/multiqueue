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

#include <cstdint>

namespace multiqueue {

struct Config {
    std::uint64_t seed = 1;
    unsigned int c = 4;
    unsigned int stickiness = 16;
};

}  // namespace multiqueue
