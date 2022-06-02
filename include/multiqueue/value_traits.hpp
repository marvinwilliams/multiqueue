/**
******************************************************************************
* @file:   value_traits.hpp
*
* @author: Marvin Williams
* @date:   2021/11/05 15:39
* @brief:
*******************************************************************************
**/

#pragma once
#ifndef VALUE_TRAITS_HPP_INCLUDED
#define VALUE_TRAITS_HPP_INCLUDED

#include <type_traits>
#include <utility>

namespace multiqueue {

template <typename Key, typename T>
struct value_traits {
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<Key, T>;

    static constexpr key_type key_of_value(value_type const &v) noexcept {
        return v.first;
    }
};

template <typename Key>
struct value_traits<Key, void> {
    using key_type = Key;
    using mapped_type = void;
    using value_type = Key;

    static constexpr key_type key_of_value(value_type const &v) noexcept {
        return v;
    }
};

}  // namespace multiqueue

#endif  //! VALUE_TRAITS_HPP_INCLUDED
