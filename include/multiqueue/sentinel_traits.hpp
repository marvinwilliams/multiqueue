/**
******************************************************************************
* @file:   sentinel_traits.hpp
*
* @author: Marvin Williams
* @date:   2021/11/05
* @brief:
*******************************************************************************
**/
#ifndef SENTINEL_TRAITS_HPP_INCLUDED
#define SENTINEL_TRAITS_HPP_INCLUDED

#include <functional>
#include <limits>

namespace multiqueue {

template <typename T, typename Compare>
struct sentinel_traits {
    static constexpr bool is_implicit = false;

    static constexpr T sentinel() noexcept {
        return T{};
    }
};

template <typename T>
struct sentinel_traits<T, std::less<T>> {
    static constexpr bool is_implicit = std::numeric_limits<T>::is_integer;

    static constexpr T sentinel() noexcept {
        if constexpr (is_implicit) {
            return std::numeric_limits<T>::lowest();
        } else {
            return T{};
        }
    }
};

template <typename T>
struct sentinel_traits<T, std::less<>> {
    static constexpr bool is_implicit = std::numeric_limits<T>::is_integer;

    static constexpr T sentinel() noexcept {
        if constexpr (is_implicit) {
            return std::numeric_limits<T>::lowest();
        } else {
            return T{};
        }
    }
};

template <typename T>
struct sentinel_traits<T, std::greater<T>> {
    static constexpr bool is_implicit = std::numeric_limits<T>::is_integer;

    static constexpr T sentinel() noexcept {
        if constexpr (is_implicit) {
            return std::numeric_limits<T>::max();
        } else {
            return T{};
        }
    }
};

template <typename T>
struct sentinel_traits<T, std::greater<>> {
    static constexpr bool is_implicit = std::numeric_limits<T>::is_integer;

    static constexpr T sentinel() noexcept {
        if constexpr (is_implicit) {
            return std::numeric_limits<T>::max();
        } else {
            return T{};
        }
    }
};

}  // namespace multiqueue

#endif
