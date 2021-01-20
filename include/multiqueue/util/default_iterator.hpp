/******************************************************************************
 * File:             default_iterator.hpp
 *
 * Author:           Marvin Williams
 * Created:          12/07/20
 * Description:      This header defines a default iterator that does not change
 *                   any semantics of the supported operations and typedefs.
 *                   This iterator can be used to wrap pointer types.
 *****************************************************************************/

#include <iterator>

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

    constexpr default_iterator& operator++(int) noexcept {
        return default_iterator{current_++};
    }

    constexpr default_iterator& operator--() noexcept {
        --current_;
        return *this;
    }

    constexpr default_iterator& operator--(int) noexcept {
        return default_iterator{current_--};
    }

    constexpr default_iterator& operator+=(difference_type n) noexcept {
        current_ += n;
        return *this;
    }

    constexpr default_iterator& operator+(difference_type n) noexcept {
        return default_iterator{current_ + n};
    }

    constexpr default_iterator& operator-=(difference_type n) noexcept {
        current_ -= n;
        return *this;
    }

    constexpr default_iterator& operator-(difference_type n) noexcept {
        return default_iterator{current_ - n};
    }

    constexpr reference operator[](difference_type n) noexcept {
        return current_[n];
    }
};
