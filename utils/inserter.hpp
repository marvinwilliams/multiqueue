#pragma once
#ifndef INSERTER_HPP_INCLUDED
#define INSERTER_HPP_INCLUDED

#include <cstdint>
#include <random>

namespace util {

enum class InsertPolicy { Uniform, Split, Producer, Alternating };

template <InsertPolicy>
struct Inserter {
    bool insert;

    bool operator()() {
        return insert;
    }
};

template <>
struct Inserter<InsertPolicy::Uniform> {
   private:
    std::mt19937 gen_;
    std::uniform_int_distribution<std::uint64_t> dist_;
    std::uint64_t rand_num_;
    std::uint8_t bit_pos_ : 6;

   public:
    explicit Inserter(unsigned int seed = 0u) : gen_{seed}, rand_num_{0u}, bit_pos_{0u} {
    }

    bool operator()() {
        if (bit_pos_ == 0) {
            rand_num_ = dist_(gen_);
        }
        return rand_num_ & (1 << bit_pos_++);
    }
};

template <>
struct Inserter<InsertPolicy::Alternating> {
   private:
    bool insert_ = false;

   public:
    bool operator()() {
        return insert_ = !insert_, insert_;
    }
};

}  // namespace utils

#endif  //! INSERTER_HPP_INCLUDED
