/**
******************************************************************************
* @file:   value.hpp
*
* @author: Marvin Williams
* @date:   2021/11/05 15:39
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef VALUE_HPP_INCLUDED
#define VALUE_HPP_INCLUDED

#include <type_traits>
#include <utility>

namespace multiqueue::detail {

template <typename Key, typename T>
using value_type = std::conditional_t<std::is_same_v<T, void>, Key, std::pair<Key, T>>;

template <typename Key, typename T>
struct key_extractor {
    constexpr Key &operator()(value_type<Key, T> &v) const noexcept {
        if constexpr (std::is_same_v<T, void>) {
            return v;
        } else {
            return v.first;
        }
    }

    constexpr Key const &operator()(value_type<Key, T> const &v) const noexcept {
        if constexpr (std::is_same_v<T, void>) {
            return v;
        } else {
            return v.first;
        }
    }

    constexpr Key &&operator()(value_type<Key, T> &&v) const noexcept {
        if constexpr (std::is_same_v<T, void>) {
            return std::move(v);
        } else {
            return std::move(v).first;
        }
    }
};

}  // namespace multiqueue::detail

#endif  //! VALUE_HPP_INCLUDED
