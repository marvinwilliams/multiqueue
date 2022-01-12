/**
******************************************************************************
* @file:   sentinel.hpp
*
* @author: Marvin Williams
* @date:   2022/01/11 12:23
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef SENTINEL_HPP_INCLUDED
#define SENTINEL_HPP_INCLUDED

#include <functional>
#include <limits>

namespace multiqueue {

template <typename T, typename Compare>
struct DefaultSentinel;

#define DEFAULT_SENTINEL_SPECIALIZATION(IntType)                                                      \
    static_assert(std::numeric_limits<IntType>::is_specialized,                                       \
                  "std::numeric_limits not specialized for type '" #IntType "'");                     \
    static_assert(std::numeric_limits<IntType>::is_integer, "type '" #IntType "' is not an integer"); \
    template <>                                                                                       \
    struct DefaultSentinel<IntType, std::less<>> {                                                    \
        constexpr IntType operator()() const noexcept {                                               \
            return std::numeric_limits<IntType>::max();                                               \
        }                                                                                             \
    };                                                                                                \
                                                                                                      \
    template <>                                                                                       \
    struct DefaultSentinel<IntType, std::less<IntType>> {                                             \
        constexpr IntType operator()() const noexcept {                                               \
            return std::numeric_limits<IntType>::max();                                               \
        }                                                                                             \
    };                                                                                                \
                                                                                                      \
    template <>                                                                                       \
    struct DefaultSentinel<IntType, std::greater<>> {                                                 \
        constexpr IntType operator()() const noexcept {                                               \
            return std::numeric_limits<IntType>::lowest();                                            \
        }                                                                                             \
    };                                                                                                \
                                                                                                      \
    template <>                                                                                       \
    struct DefaultSentinel<IntType, std::greater<IntType>> {                                          \
        constexpr IntType operator()() const noexcept {                                               \
            return std::numeric_limits<IntType>::lowest();                                            \
        }                                                                                             \
    };

DEFAULT_SENTINEL_SPECIALIZATION(char)
DEFAULT_SENTINEL_SPECIALIZATION(signed char)
DEFAULT_SENTINEL_SPECIALIZATION(unsigned char)
DEFAULT_SENTINEL_SPECIALIZATION(wchar_t)
DEFAULT_SENTINEL_SPECIALIZATION(char16_t)
DEFAULT_SENTINEL_SPECIALIZATION(char32_t)
DEFAULT_SENTINEL_SPECIALIZATION(short)
DEFAULT_SENTINEL_SPECIALIZATION(int)
DEFAULT_SENTINEL_SPECIALIZATION(long)
DEFAULT_SENTINEL_SPECIALIZATION(long long)
DEFAULT_SENTINEL_SPECIALIZATION(unsigned short)
DEFAULT_SENTINEL_SPECIALIZATION(unsigned int)
DEFAULT_SENTINEL_SPECIALIZATION(unsigned long)
DEFAULT_SENTINEL_SPECIALIZATION(unsigned long long)

}  // namespace multiqueue

#endif  //! SENTINEL_HPP_INCLUDED
