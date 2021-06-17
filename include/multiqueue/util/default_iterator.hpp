/**
******************************************************************************
* @file:   default_iterator.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 16:47
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef UTIL_DEFAULT_ITERATOR_HPP_INCLUDED
#define UTIL_DEFAULT_ITERATOR_HPP_INCLUDED

#include <iterator>

namespace multiqueue {
namespace util {

// The tag is solely used to instantiate different Iterators with the same
// `Iter` type.
template <typename Iter, typename Tag>
class default_iterator {
   protected:
    Iter current_;
    using iter_traits = std::iterator_traits<Iter>;

   public:
    using iterator_category = typename iter_traits::iterator_category;
    using value_type = typename iter_traits::value_type;
    using difference_type = typename iter_traits::difference_type;
    using reference = typename iter_traits::reference;
    using pointer = typename iter_traits::pointer;

    constexpr default_iterator() noexcept : current_{Iter{}} {
    }

    explicit constexpr default_iterator(const Iter& it) noexcept : current_{it} {
    }

    constexpr reference operator*() const noexcept {
        return *current_;
    }

    constexpr pointer operator->() const noexcept {
        return *current_;
    }

    constexpr default_iterator& operator++() noexcept {
        ++current_;
        return *this;
    }

    constexpr default_iterator operator++(int) noexcept {
        return default_iterator{current_++};
    }

    constexpr default_iterator& operator--() noexcept {
        --current_;
        return *this;
    }

    constexpr default_iterator operator--(int) noexcept {
        return default_iterator{current_--};
    }

    constexpr default_iterator& operator+=(difference_type n) noexcept {
        current_ += n;
        return *this;
    }

    constexpr default_iterator operator+(difference_type n) noexcept {
        return default_iterator{current_ + n};
    }

    constexpr default_iterator& operator-=(difference_type n) noexcept {
        current_ -= n;
        return *this;
    }

    constexpr default_iterator operator-(difference_type n) noexcept {
        return default_iterator{current_ - n};
    }

    constexpr reference operator[](difference_type n) noexcept {
        return current_[n];
    }
};

}  // namespace util
}  // namespace multiqueue

#endif  //! UTIL_DEFAULT_ITERATOR_HPP_INCLUDED
