/**
******************************************************************************
* @file:   cartesian_product_type.hpp
*
* @author: Marvin Williams
* @date:   2021/04/08 10:40
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef TESTS_UTILS_CARTESIAN_PRODUCT_TYPE_HPP_INCLUDED
#define TESTS_UTILS_CARTESIAN_PRODUCT_TYPE_HPP_INCLUDED

#include <array>
#include <cstddef>
#include <tuple>
#include <type_traits>

namespace {

template <typename TupleList, std::size_t... Is>
static constexpr std::size_t tuple_size_product(std::index_sequence<Is...>) {
    if constexpr (sizeof...(Is) == 0) {
        return 1;
    } else {
        return (std::tuple_size_v<std::tuple_element_t<Is, TupleList>> * ...);
    }
}

template <std::size_t N, std::size_t TupleIndex, typename TupleList>
struct type_in_tuple {
    static constexpr std::size_t local_index =
        (N / tuple_size_product<TupleList>(std::make_index_sequence<TupleIndex>{})) %
        std::tuple_size_v<std::tuple_element_t<TupleIndex, TupleList>>;

    using type = std::tuple_element_t<local_index, std::tuple_element_t<TupleIndex, TupleList>>;
};

template <std::size_t N, typename TupleList, typename>
struct nth_result_tuple {};

template <std::size_t N, typename TupleList, std::size_t... Is>
struct nth_result_tuple<N, TupleList, std::index_sequence<Is...>> {
    using type = std::tuple<typename type_in_tuple<N, Is, TupleList>::type...>;
};

template <typename TupleList, typename>
struct cartesian_product_from_tuple_list {};

template <typename TupleList, std::size_t... Is>
struct cartesian_product_from_tuple_list<TupleList, std::index_sequence<Is...>> {
    using type = std::tuple<
        typename nth_result_tuple<Is, TupleList, std::make_index_sequence<std::tuple_size_v<TupleList>>>::type...>;
};

template <typename... Tuples>
struct cartesian_product {};

template <template <typename...> typename Functor, typename>
struct Executor {};

template <template <typename...> typename Functor, typename... Args>
struct Executor<Functor, std::tuple<Args...>> {
    static void execute() {
        Functor<Args...>{}();
    }
};

template <template <typename...> typename Functor, typename ProductList, std::size_t... Is>
bool execute_with_nth_result_tuple(std::index_sequence<Is...>, std::size_t n) {
    return ((Is == n ? (Executor<Functor, std::tuple_element_t<Is, ProductList>>::execute(), true) : false) || ...);
}
template <template <typename...> typename Functor, typename ProductList, std::size_t... Is>
void execute_for_each_result_tuple(std::index_sequence<Is...>) {
    (Executor<Functor, std::tuple_element_t<Is, ProductList>>::execute(), ...);
}

}  // namespace

template <typename... Tuples>
using carsetian_product_t =
    typename cartesian_product_from_tuple_list<std::tuple<Tuples...>,
                                               std::make_index_sequence<(1 * ... * std::tuple_size_v<Tuples>)>>::type;

template <template <typename...> typename Functor, typename... Tuples>
bool execute_by_index(std::array<std::size_t, sizeof...(Tuples)> const& indices) {
    if constexpr (sizeof...(Tuples) > 0) {
        static constexpr std::size_t N = (1 * ... * std::tuple_size_v<Tuples>);
        using ProductList =
            typename cartesian_product_from_tuple_list<std::tuple<Tuples...>, std::make_index_sequence<N>>::type;
        std::size_t i = 0;
        std::size_t partial_product = 1;
        std::size_t global_index = 0;
        ((global_index += indices[i++] * partial_product, partial_product *= std::tuple_size_v<Tuples>), ...);
        return execute_with_nth_result_tuple<Functor, ProductList>(std::make_index_sequence<N>{}, global_index);
    }
    return false;
}

template <template <typename...> typename Functor, typename... Tuples>
void execute_for_each() {
    if constexpr (sizeof...(Tuples) > 0) {
        static constexpr std::size_t N = (1 * ... * std::tuple_size_v<Tuples>);
        using ProductList =
            typename cartesian_product_from_tuple_list<std::tuple<Tuples...>, std::make_index_sequence<N>>::type;
        execute_for_each_result_tuple<Functor, ProductList>(std::make_index_sequence<N>{});
    }
}

#endif  //! TESTS_UTILS_CARTESIAN_PRODUCT_TYPE_HPP_INCLUDED
