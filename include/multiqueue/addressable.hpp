/**
******************************************************************************
* @file:   addressable.hpp
*
* @author: Marvin Williams
* @date:   2021/12/21 02:22
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef ADDRESSABLE_HPP_INCLUDED
#define ADDRESSABLE_HPP_INCLUDED

#include <atomic>
#include <cstddef>

namespace multiqueue {

enum struct Location { InsertionBuffer, DeletionBuffer, Heap };

}  // namespace multiqueue

#endif  //! ADDRESSABLE_HPP_INCLUDED
