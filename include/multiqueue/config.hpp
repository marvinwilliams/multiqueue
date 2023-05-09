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
    int seed = 1;
    int pqs_per_thread = 2;
    int stickiness = 16;
};

}  // namespace multiqueue
