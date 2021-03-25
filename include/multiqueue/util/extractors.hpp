/**
******************************************************************************
* @file:   extractors.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 16:48
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef UTIL_EXTRACTORS_HPP_INCLUDED
#define UTIL_EXTRACTORS_HPP_INCLUDED

#include <utility>

namespace multiqueue {
namespace util {

template <typename T>
struct identity {
    constexpr T &operator()(T &v) const noexcept {
        return v;
    }
    constexpr T const &operator()(T const &v) const noexcept {
        return v;
    }
};

template <typename T, std::size_t I = 0>
struct get_nth {
    constexpr auto &operator()(T &v) const noexcept {
        return std::get<I>(v);
    }
    constexpr auto const &operator()(T const &v) const noexcept {
        return std::get<I>(v);
    }
};

}  // namespace util
}  // namespace multiqueue

#endif  //! UTIL_EXTRACTORS_HPP_INCLUDED
