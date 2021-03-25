/**
******************************************************************************
* @file:   conditional_value.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 16:46
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef UTIL_CONDITION_VALUE_HPP_INCLUDED
#define UTIL_CONDITION_VALUE_HPP_INCLUDED

#include <utility>

namespace multiqueue {
namespace util {

// This class does not support move semantics since it is intended for small
// types.
template <bool hold_value, typename T>
struct conditional_value;

template <typename T>
struct conditional_value<false, T> {
    static_assert(std::is_trivial_v<T>, "Conditional value must be trivial");
    static constexpr bool holds_value = false;

    conditional_value() noexcept = default;
    explicit conditional_value(T) noexcept {
    }
    conditional_value(conditional_value const &) noexcept {
    }
    conditional_value &operator=(conditional_value const &) noexcept {
    }
    inline void swap(conditional_value &) noexcept {
    }

    constexpr T get() const noexcept {
        return T();
    }
    constexpr void set(T) noexcept {
    }
};

template <typename T>
struct conditional_value<true, T> {
    static_assert(std::is_trivial_v<T>, "Conditional value must be trivial");
    static constexpr bool holds_value = true;
    T value;

    conditional_value() noexcept = default;
    explicit conditional_value(T val) noexcept : value{val} {
    }
    conditional_value(conditional_value const &other) noexcept : value{other.value} {
    }
    conditional_value &operator=(conditional_value const &other) noexcept {
        value = other.value;
        return *this;
    }
    inline void swap(conditional_value &other) noexcept {
        using std::swap;
        swap(value, other.value);
    }

    constexpr T get() const noexcept {
        return value;
    }
    constexpr void set(T new_val) noexcept {
        value = new_val;
    }
};

template <bool hold_value, typename T>
inline void swap(conditional_value<hold_value, T> &lhs,
                 conditional_value<hold_value, T> &rhs) noexcept(noexcept(lhs.swap(rhs))) {
    lhs.swap(rhs);
}

}  // namespace util
}  // namespace multiqueue

#endif  //! UTIL_CONDITION_VALUE_HPP_INCLUDED
