#pragma once

#include <functional>
#include <limits>
#include <type_traits>

namespace multiqueue::sentinel {

template <typename T, typename Compare>
struct DefaultConstruct {
    static_assert(std::is_default_constructible_v<T>, "T must be default-constructible");
    static constexpr T sentinel() noexcept {
        return T();
    }

    static constexpr bool is_sentinel(T const& t) noexcept {
        return t == T();
    }

    static constexpr bool compare(Compare const& comp, T const& lhs, T const& rhs) noexcept {
        if (is_sentinel(rhs)) {
            return false;
        }
        if (is_sentinel(lhs)) {
            return true;
        }
        return comp(lhs, rhs);
    }
};

template <typename T, typename Compare>
struct Implicit {
    static_assert(std::numeric_limits<T>::is_bounded, "T must be bounded");
    static_assert(std::is_same_v<Compare, std::less<T>> || std::is_same_v<Compare, std::less<>> ||
                      std::is_same_v<Compare, std::greater<T>> || std::is_same_v<Compare, std::greater<>>,
                  "Compare must be std::less<T>, std::less<>, std::greater<T> or std::greater<>");
    static constexpr T sentinel() noexcept {
        if constexpr (std::is_same_v<Compare, std::less<T>> || std::is_same_v<Compare, std::less<>>) {
            return std::numeric_limits<T>::lowest();
        } else {
            return std::numeric_limits<T>::max();
        }
    }

    static constexpr bool is_sentinel(T const& t) noexcept {
        return t == sentinel();
    }

    static constexpr bool compare(Compare const& comp, T const& lhs, T const& rhs) noexcept {
        return comp(lhs, rhs);
    }
};

}  // namespace multiqueue::sentinel
