/**
******************************************************************************
* @file:   inplace_merge.hpp
*
* @author: Marvin Williams
* @date:   2021/03/10 17:55
* @brief:
*******************************************************************************
**/
#pragma once
#include <iterator>
#ifndef INPLACE_MERGE_HPP_INCLUDED
#define INPLACE_MERGE_HPP_INCLUDED

#include <cstddef>
#include <utility>

namespace multiqueue {
namespace util {

// Merges the smallest n values of input and in_out into the (empty) out and merges the remaining
// values into in_out
template <typename InputIt, typename InOutIt, typename OutputIt, typename Comparator>
void inplace_merge(InputIt input, InOutIt in_out, OutputIt output, std::size_t n, Comparator comp) {
    auto const input_end = input + static_cast<typename std::iterator_traits<InputIt>::difference_type>(n);
    auto const in_out_end = in_out + static_cast<typename std::iterator_traits<InOutIt>::difference_type>(n);
    auto in_out_copy = in_out;
    while (n > 0) {
        if (comp(*input, *in_out)) {
            *output++ = std::move(*(input++));
        } else {
            *output++ = std::move(*(in_out++));
        }
        --n;
    }
    // Merge into in_out until only elements from in_out remain (those don't need to be moved)
    while (input != input_end && in_out != in_out_end) {
        if (comp(*input, *in_out)) {
            *in_out_copy++ = std::move(*(input++));
        } else {
            *in_out_copy++ = std::move(*(in_out++));
        }
    }
    while (input != input_end) {
        *in_out_copy++ = std::move(*(input++));
    }
    assert(in_out == in_out_copy);
}

}  // namespace util
}  // namespace multiqueue

#endif  //! INPLACE_MERGE_HPP_INCLUDED
