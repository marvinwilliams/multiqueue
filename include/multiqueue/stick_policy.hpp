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
#ifndef STICK_POLICY_HPP_INCLUDED
#define STICK_POLICY_HPP_INCLUDED

namespace multiqueue {

enum class StickPolicy { None, Random, Swapping, Permutation };

}  // namespace multiqueue

#endif
