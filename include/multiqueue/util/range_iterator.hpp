#ifndef RANGE_ITERATOR_GSPFYYID
#define RANGE_ITERATOR_GSPFYYID

#include <iterator>
#include <limits>

namespace multiqueue {
namespace util {

template <typename T>
class range_iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;

   private:
    T v_ = T{};
    T const max_ = T{};
    difference_type const step_ = 1;

   public:
    range_iterator() = default;

    constexpr explicit range_iterator(T value, T max_value = std::numeric_limits<T>::max(), difference_type step = 1)
        : v_{value}, max_{max_value}, step_{step} {
    }

    constexpr T operator*() const noexcept {
        return v_;
    }

    constexpr range_iterator& operator++() noexcept {
        v_ += step_;
        return *this;
    }

    constexpr range_iterator operator++(int) noexcept {
        auto tmp = *this;
        ++*this;
        return tmp;
    }

    constexpr range_iterator& operator--() noexcept {
        v_ -= step_;
        return *this;
    }

    constexpr range_iterator operator--(int) noexcept {
        auto tmp = *this;
        --*this;
        return tmp;
    }

    constexpr range_iterator& operator+=(difference_type n) {
        v_ += n * step_;
        return *this;
    }

    constexpr range_iterator& operator-=(difference_type n) {
        v_ -= n * step_;
        return *this;
    }

    friend constexpr bool operator==(const range_iterator& it, const range_iterator&) {
        return it.v_ >= it.max_;
    }

    friend constexpr bool operator!=(const range_iterator& it, const range_iterator&) {
        return it.v_ < it.max_;
    }

    friend constexpr bool operator<(const range_iterator& lhs, const range_iterator& rhs) {
        return lhs.v_ < rhs.v_;
    }
    friend constexpr bool operator>(const range_iterator& lhs, const range_iterator& rhs) {
        return lhs.v_ > rhs.v_;
    }
    friend constexpr bool operator<=(const range_iterator& lhs, const range_iterator& rhs) {
        return lhs.v_ <= rhs.v_;
    }
    friend constexpr bool operator>=(const range_iterator& lhs, const range_iterator& rhs) {
        return lhs.v_ >= rhs.v_;
    }

    friend constexpr range_iterator operator+(range_iterator it, difference_type n) {
        return it += n;
    }

    friend constexpr range_iterator operator-(range_iterator it, difference_type n) {
        return it -= n;
    }

    friend constexpr difference_type operator-(const range_iterator& lhs, const range_iterator& rhs) {
        return lhs.v_ - rhs.v_;
    }
};

template <typename T, typename Predicate>
class predicate_iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = T*;
    using reference = T&;

   private:
    T v_ = T{};
    T const max_ = T{};
    Predicate pred_;

   public:
    predicate_iterator() = default;

    constexpr explicit predicate_iterator(T value, T max_value = std::numeric_limits<T>::max(),
                                          Predicate const& pred = Predicate{})
        : v_{value}, max_{max_value}, pred_{pred} {
    }

    constexpr predicate_iterator end() const noexcept {
        return predicate_iterator{0, 0, pred_};
    }

    constexpr T operator*() const noexcept {
        return v_;
    }

    constexpr predicate_iterator& operator++() noexcept {
        do {
            ++v_;
        } while (v_ < max_ && !pred_(v_));
        return *this;
    }

    constexpr predicate_iterator operator++(int) noexcept {
        auto tmp = *this;
        ++*this;
        return tmp;
    }

    constexpr predicate_iterator& operator+=(difference_type n) {
        for (difference_type i = 0; i < n; ++i) {
            ++*this;
        }
        return *this;
    }

    friend constexpr bool operator==(const predicate_iterator& it, const predicate_iterator&) {
        return it.v_ == it.max_;
    }

    friend constexpr bool operator!=(const predicate_iterator& it, const predicate_iterator&) {
        return it.v_ < it.max_;
    }

    friend constexpr bool operator<(const predicate_iterator& lhs, const predicate_iterator& rhs) {
        return lhs.v_ < rhs.v_;
    }
    friend constexpr bool operator>(const predicate_iterator& lhs, const predicate_iterator& rhs) {
        return lhs.v_ > rhs.v_;
    }
    friend constexpr bool operator<=(const predicate_iterator& lhs, const predicate_iterator& rhs) {
        return lhs.v_ <= rhs.v_;
    }
    friend constexpr bool operator>=(const predicate_iterator& lhs, const predicate_iterator& rhs) {
        return lhs.v_ >= rhs.v_;
    }

    friend constexpr predicate_iterator operator+(predicate_iterator it, difference_type n) {
        return it += n;
    }

    friend constexpr difference_type operator-(const predicate_iterator& lhs, const predicate_iterator& rhs) {
        return lhs.v_ - rhs.v_;
    }
};

}  // namespace util
}  // namespace multiqueue

#endif  // !RANGE_ITERATOR_GSPFYYID
