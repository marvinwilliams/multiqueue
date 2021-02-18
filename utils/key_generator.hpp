#pragma once
#ifndef KEY_GENERATOR_HPP_INCLUDED
#define KEY_GENERATOR_HPP_INCLUDED

#include <cassert>
#include <limits>
#include <random>

namespace util {

enum class KeyDistribution { Uniform, Dijkstra, Ascending, Descending };

template <typename T, KeyDistribution>
struct KeyGenerator;

template <typename T>
struct KeyGenerator<T, KeyDistribution::Uniform> {
   private:
    std::mt19937 gen_;
    std::uniform_int_distribution<T> dist_;

   public:
    explicit KeyGenerator(unsigned int seed = 0u, T min = 0, T max = std::numeric_limits<T>::max())
        : gen_{seed}, dist_{min, max} {
    }

    T operator()() {
        return dist_(gen_);
    }
};

template <typename T>
struct KeyGenerator<T, KeyDistribution::Ascending> {
   private:
    T current_;

   public:
    explicit KeyGenerator(T start = 0) : current_{start} {
    }

    T operator()() {
        return current_++;
    }
};

template <typename T>
struct KeyGenerator<T, KeyDistribution::Descending> {
   private:
    T current_;

   public:
    explicit KeyGenerator(T start) : current_{start} {
    }

    T operator()() {
        assert(current_ != std::numeric_limits<T>::max());
        return current_--;
    }
};

template <typename T>
struct KeyGenerator<T, KeyDistribution::Dijkstra> {
   private:
    std::mt19937 gen_;
    std::uniform_int_distribution<T> dist_;
    T current_ = 0;

   public:
    explicit KeyGenerator(unsigned int seed = 0u, T increase_min = 1, T increase_max = 100)
        : gen_{seed}, dist_{increase_min, increase_max} {
    }

    T operator()() {
        return current_++ + dist_(gen_);
    }
};

}  // namespace util

#endif  //! KEY_GENERATOR_HPP_INCLUDED
