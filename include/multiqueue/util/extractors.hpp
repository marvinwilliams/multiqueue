#ifndef EXTRACTORS_HPP_NYJSHAJH
#define EXTRACTORS_HPP_NYJSHAJH

#include <utility>

namespace multiqueue {
namespace util {

struct identity {
    template <typename T>
    constexpr T &&operator()(T &&v) const noexcept {
        return std::forward<T>(v);
    }
};

struct pair_first {
    template <typename Pair>
    constexpr auto const &operator()(Pair const &p) const noexcept {
        return std::get<0>(p);
    }
};

}  // namespace util
}  // namespace multiqueue

#endif  // !EXTRACTORS_HPP_NYJSHAJH
