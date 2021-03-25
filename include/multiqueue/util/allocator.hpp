/**
******************************************************************************
* @file:   allocator.hpp
*
* @author: Marvin Williams
* @date:   2021/03/25 16:44
* @brief:
*******************************************************************************
**/
#pragma once
#ifndef UTIL_ALLOCATOR_HPP_INCLUDED
#define UTIL_ALLOCATOR_HPP_INCLUDED

#include <cstddef>
#include <iostream>
#include <limits>
#include <type_traits>

namespace multiqueue {
namespace util {

template <typename T>
class MultiqueueAllocator {
   private:
    int i_;

   public:
    using value_type = T;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    using propagate_on_container_move_assignment = std::true_type;

   public:
    constexpr explicit MultiqueueAllocator(int i) : i_{i} {
    }
    constexpr explicit MultiqueueAllocator(MultiqueueAllocator const &other) : i_{other.i_} {
    }

    [[nodiscard]] constexpr value_type *allocate(std::size_t n) {
        std::cout << '[' << i_ << "] Trying to allocate " << n << " objects" << std::endl;
        if (n > static_cast<std::size_t>(std::numeric_limits<std::ptrdiff_t>::max()) / sizeof(T)) {
            throw std::bad_alloc();
        }
        if (alignof(T) > __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
            return static_cast<T *>(::operator new(n * sizeof(T), static_cast<std::align_val_t>(alignof(T))));
        }
        return static_cast<T *>(::operator new(n * sizeof(T)));
    }

    constexpr void deallocate(T *p, std::size_t n) {
        std::cout << '[' << i_ << "] Deallocate " << n << " objects" << std::endl;
        if (alignof(T) > __STDCPP_DEFAULT_NEW_ALIGNMENT__) {
            ::operator delete(p, n * sizeof(T), static_cast<std::align_val_t>(alignof(T)));
        } else {
            ::operator delete(p, n * sizeof(T));
        }
    }
    friend inline bool operator==(MultiqueueAllocator const &lhs, MultiqueueAllocator const &rhs) noexcept {
        return lhs.i_ == rhs.i_;
    }

    friend inline bool operator!=(MultiqueueAllocator const &lhs, MultiqueueAllocator const &rhs) noexcept {
        return !(lhs == rhs);
    }
};

}  // namespace util
}  // namespace multiqueue

#endif  //! UTIL_ALLOCATOR_HPP_INCLUDED
