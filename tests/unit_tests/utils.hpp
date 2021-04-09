#ifndef DEFINITIONS_HPP_1XVO9BK7
#define DEFINITIONS_HPP_1XVO9BK7

struct no_default_no_copy_type {
  int i;

  no_default_no_copy_type(int arg) : i(arg) {}
  no_default_no_copy_type() = delete;
  no_default_no_copy_type(no_default_no_copy_type const &) = delete;
  no_default_no_copy_type(no_default_no_copy_type &&) = default;
  no_default_no_copy_type &operator=(no_default_no_copy_type const &) = delete;
  no_default_no_copy_type &operator=(no_default_no_copy_type &&) = default;
};

constexpr bool operator<(no_default_no_copy_type const &lhs,
                         no_default_no_copy_type const &rhs) noexcept {
  return lhs.i < rhs.i;
}

constexpr bool operator==(no_default_no_copy_type const &lhs,
                         no_default_no_copy_type const &rhs) noexcept {
  return lhs.i == rhs.i;
}

constexpr bool operator!=(no_default_no_copy_type const &lhs,
                         no_default_no_copy_type const &rhs) noexcept {
  return lhs.i != rhs.i;
}

template <typename T> struct mod_10_compare {
  bool operator()(T const &lhs, T const &rhs) const {
    return (lhs % 10) < (rhs % 10);
  }
};

template <typename T> struct counting_compare {
  bool operator()(T const &lhs, T const &rhs) const {
    ++counter;
    return lhs < rhs;
  }
  inline static unsigned int counter = 0;
};


#endif /* end of include guard: DEFINITIONS_HPP_1XVO9BK7 */
