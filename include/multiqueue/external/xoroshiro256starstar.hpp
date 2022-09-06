// Cpp wrapper around xoroshiro128plus with the following pretext:

/*  Written in 2018 by David Blackman and Sebastiano Vigna (vigna@acm.org)

To the extent possible under law, the author has dedicated all copyright
and related and neighboring rights to this software to the public domain
worldwide. This software is distributed without any warranty.

See <http://creativecommons.org/publicdomain/zero/1.0/>.

This is xoshiro256** 1.0, one of our all-purpose, rock-solid
generators. It has excellent (sub-ns) speed, a state (256 bits) that is
large enough for any parallel application, and it passes all tests we are
aware of.

For generating just floating-point numbers, xoshiro256+ is even faster.

The state must be seeded so that it is not everywhere zero. If you have
a 64-bit seed, we suggest to seed a splitmix64 generator and use its
output to fill s. */

#pragma once

#include <cstdint>
#include <limits>

class xoroshiro256starstar {
   public:
    using result_type = std::uint64_t;

   private:
    result_type s_[4]{};
    static constexpr result_type JUMP[] = {0x180ec6d33cfd0aba, 0xd5a61266f0c9392c, 0xa9582618e03fc9aa,
                                           0x39abdc4529b1661c};
    static constexpr result_type LONG_JUMP[] = {0x76e15d3efefdcbbf, 0xc5004e441c522fb3, 0x77710069854ee241,
                                                0x39109bb02acbe635};

    static constexpr result_type rotl(result_type x, int k) noexcept {
        return (x << k) | (x >> (64 - k));
    }

    static constexpr result_type splitmix64(result_type& x) noexcept {
        result_type z = (x += UINT64_C(0x9e3779b97f4a7c15));
        z = (z ^ (z >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)) * UINT64_C(0x94d049bb133111eb);
        return z ^ (z >> 31);
    }

   public:
    constexpr explicit xoroshiro256starstar(result_type value) noexcept {
        s_[0] = splitmix64(value);
        s_[1] = splitmix64(value);
        s_[2] = splitmix64(value);
        s_[3] = splitmix64(value);
    }

    xoroshiro256starstar() : xoroshiro256starstar(0) {
    }

    constexpr void seed(result_type value) noexcept {
        s_[0] = splitmix64(value);
        s_[1] = splitmix64(value);
        s_[2] = splitmix64(value);
        s_[3] = splitmix64(value);
    }

    constexpr result_type operator()() noexcept {
        result_type const result = rotl(s_[1] * 5, 7) * 9;
        result_type const t = s_[1] << 17;
        s_[2] ^= s_[0];
        s_[3] ^= s_[1];
        s_[1] ^= s_[2];
        s_[0] ^= s_[3];
        s_[2] ^= t;
        s_[3] = rotl(s_[3], 45);
        return result;
    }

    // This is the jump function for the generator. It is equivalent to 2^128 calls to next(); it can be used to
    // generate 2^128 non-overlapping subsequences for parallel computations.
    constexpr void jump() noexcept {
        result_type s0 = 0;
        result_type s1 = 0;
        result_type s2 = 0;
        result_type s3 = 0;
        for (unsigned int i = 0; i < sizeof JUMP / sizeof *JUMP; i++) {
            for (int b = 0; b < 64; b++) {
                if (JUMP[i] & UINT64_C(1) << b) {
                    s0 ^= s_[0];
                    s1 ^= s_[1];
                    s2 ^= s_[2];
                    s3 ^= s_[3];
                }
                operator()();
            }
        }
        s_[0] = s0;
        s_[1] = s1;
        s_[2] = s2;
        s_[3] = s3;
    }

    // This is the long-jump function for the generator. It is equivalent to 2^192 calls to next(); it can be used to
    // generate 2^64 starting points, from each of which jump() will generate 2^64 non-overlapping subsequences for
    // parallel distributed computations.
    constexpr void long_jump() noexcept {
        result_type s0 = 0;
        result_type s1 = 0;
        result_type s2 = 0;
        result_type s3 = 0;
        for (unsigned int i = 0; i < sizeof LONG_JUMP / sizeof *LONG_JUMP; i++) {
            for (int b = 0; b < 64; b++) {
                if (LONG_JUMP[i] & UINT64_C(1) << b) {
                    s0 ^= s_[0];
                    s1 ^= s_[1];
                    s2 ^= s_[2];
                    s3 ^= s_[3];
                }
                operator()();
            }
        }
        s_[0] = s0;
        s_[1] = s1;
        s_[2] = s2;
        s_[3] = s3;
    }

    static constexpr result_type min() noexcept {
        return std::numeric_limits<result_type>::min();
    }

    static constexpr result_type max() noexcept {
        return std::numeric_limits<result_type>::max();
    }
};
