#pragma once

#include <utility>

namespace multiqueue::utils {

struct Identity {
    template <typename T>
    static constexpr T &&get(T &&t) noexcept {
        return std::forward<T>(t);
    }
};

struct PairFirst {
    template <typename Pair>
    static constexpr auto const &get(Pair const &p) noexcept {
        return p.first;
    }
};

template <typename Value, typename KeyOfValue, typename Compare>
struct ValueCompare {
    Compare key_comp;

    constexpr bool operator()(const Value &lhs, const Value &rhs) const {
        return key_comp(KeyOfValue::get(lhs), KeyOfValue::get(rhs));
    }
};

}  // namespace multiqueue::utils
