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
#include <type_traits>
#include <utility>

namespace multiqueue {

template <typename T, typename Compare>
struct sentinel_traits {
    using type = T;
    using compare = Compare;

    static constexpr bool is_implicit = false;

    static constexpr T sentinel() noexcept {
        return T{};
    }
};

template <typename T>
struct sentinel_traits<T, std::less<T>> {
    using type = T;
    using compare = std::less<T>;

    static constexpr bool is_implicit = std::numeric_limits<T>::is_integer;

    static constexpr T sentinel() noexcept {
        if constexpr (std::numeric_limits<T>::is_integer) {
            return std::numeric_limits<T>::max();
        } else {
            return T{};
        }
    }
};

template <typename T>
struct sentinel_traits<T, std::less<>> {
    using type = T;
    using compare = std::less<>;

    static constexpr bool is_implicit = std::numeric_limits<T>::is_integer;

    static constexpr T sentinel() noexcept {
        if constexpr (std::numeric_limits<T>::is_integer) {
            return std::numeric_limits<T>::max();
        } else {
            return T{};
        }
    }
};

template <typename T>
struct sentinel_traits<T, std::greater<T>> {
    using type = T;
    using compare = std::greater<T>;

    static constexpr bool is_implicit = std::numeric_limits<T>::is_integer;

    static constexpr T sentinel() noexcept {
        if constexpr (std::numeric_limits<T>::is_integer) {
            return std::numeric_limits<T>::lowest();
        } else {
            return T{};
        }
    }
};

template <typename T>
struct sentinel_traits<T, std::greater<>> {
    using type = T;
    using compare = std::greater<>;

    static constexpr bool is_implicit = std::numeric_limits<T>::is_integer;

    static constexpr T sentinel() noexcept {
        if constexpr (std::numeric_limits<T>::is_integer) {
            return std::numeric_limits<T>::lowest();
        } else {
            return T{};
        }
    }
};

}  // namespace multiqueue

#endif
