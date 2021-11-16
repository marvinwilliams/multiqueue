#pragma once
#ifndef TEST_TYPES_HPP_INCLUDED
#define TEST_TYPES_HPP_INCLUDED

namespace test_types {

struct nocopy {
    nocopy() = default;
    nocopy(nocopy const&) = delete;
    nocopy& operator=(nocopy const&) = delete;
    nocopy(nocopy&&) = default;
    nocopy& operator=(nocopy&&) = default;
    bool operator<(nocopy const& other) const {
        return i < other.i;
    }

    int i;
};

struct nodefault {
    nodefault() = delete;
    explicit nodefault(int i) : i{i} {
    }
    bool operator<(nodefault const& other) const {
        return i < other.i;
    }

    int i;
};

struct countingdtor {
    static inline int count = 0;
    ~countingdtor() {
        ++count;
    }
};

template <typename T>
struct countingcmp {
    bool operator()(T const& lhs, T const& rhs) const {
        ++count;
        return lhs < rhs;
    }
    static inline int count = 0;
};

}  // namespace test_types

#endif  //! TEST_TYPES_HPP_INCLUDED
