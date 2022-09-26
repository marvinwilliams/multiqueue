/**
******************************************************************************
* @file:   stick_policy.hpp
*
* @author: Marvin Williams
* @date:   2021/07/20 17:19
* @brief:
*******************************************************************************
**/
#pragma once

namespace multiqueue {

enum class StickPolicy { None, RandomStrict, Random, Swapping, SwappingLazy, SwappingBlocking, Permutation };

template <typename Data, StickPolicy>
struct StickPolicyImpl;

}  // namespace multiqueue
