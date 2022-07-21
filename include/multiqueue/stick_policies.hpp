/**
******************************************************************************
* @file:   stick_policies.hpp
*
* @author: Marvin Williams
* @date:   2021/07/20 17:19
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef STICK_POLICIES_HPP_INCLUDED
#define STICK_POLICIES_HPP_INCLUDED

namespace multiqueue {

enum class StickPolicy { None, Random, Swapping, Permutation };

}  // namespace multiqueue

#endif
