// Cpp wrapper around xoroshiro128plus with the following pretext:

/*  Written in 2016-2018 by David Blackman and Sebastiano Vigna (vigna@acm.org)

To the extent possible under law, the author has dedicated all copyright
and related and neighboring rights to this software to the public domain
worldwide. This software is distributed without any warranty.

See <http://creativecommons.org/publicdomain/zero/1.0/>. */

/* This is xoroshiro128+ 1.0, our best and fastest small-state generator
   for floating-point numbers. We suggest to use its upper bits for
   floating-point generation, as it is slightly faster than
   xoroshiro128++/xoroshiro128**. It passes all tests we are aware of
   except for the four lower bits, which might fail linearity tests (and
   just those), so if low linear complexity is not considered an issue (as
   it is usually the case) it can be used to generate 64-bit outputs, too;
   moreover, this generator has a very mild Hamming-weight dependency
   making our test (http://prng.di.unimi.it/hwd.php) fail after 5 TB of
   output; we believe this slight bias cannot affect any application. If
   you are concerned, use xoroshiro128++, xoroshiro128** or xoshiro256+.

   We suggest to use a sign test to extract a random Boolean value, and
   right shifts to extract subsets of bits.

   The state must be seeded so that it is not everywhere zero. If you have
   a 64-bit seed, we suggest to seed a splitmix64 generator and use its
   output to fill s.

   NOTE: the parameters (a=24, b=16, b=37) of this version give slightly
   better results in our test than the 2016 version (a=55, b=14, c=36).
*/

#include <cstdint>
#include <limits>

static inline std::uint64_t rotl(std::uint64_t const x, int k) noexcept {
    return (x << k) | (x >> (64 - k));
}

class xoroshiro128plus {
   private:
    std::uint64_t s_[2];

   public:
    using result_type = std::uint64_t;
    static constexpr std::uint64_t default_seed = 1;

    explicit xoroshiro128plus(std::uint64_t value) {
        s_[0] = value & ((UINT64_C(1) << 32) - 1);
        s_[1] = value >> 32;
    }

    xoroshiro128plus() : xoroshiro128plus(default_seed) {
    }

    template <typename Sseq>
    explicit xoroshiro128plus(Sseq& s) {
        s.generate(s_, s_ + 2);
    }

    xoroshiro128plus(xoroshiro128plus const&) = default;
    xoroshiro128plus& operator=(xoroshiro128plus const&) = default;

    template <typename Sseq>
    void seed(Sseq& s) noexcept {
      s.generate(s_, s_ + 2);
    }

    std::uint64_t operator()() noexcept {
        std::uint64_t const s0 = s_[0];
        std::uint64_t s1 = s_[1];
        std::uint64_t const result = s0 + s1;

        s1 ^= s0;
        s_[0] = rotl(s0, 24) ^ s1 ^ (s1 << 16);  // a, b
        s_[1] = rotl(s1, 37);                    // c

        return result;
    }

    void discard() noexcept {
        operator()();
    }

    static constexpr result_type min() noexcept {
        return std::numeric_limits<result_type>::min();
    }

    static constexpr result_type max() noexcept {
        return std::numeric_limits<result_type>::max();
    }

    /* This is the jump function for the generator. It is equivalent
       to 2^64 calls to next(); it can be used to generate 2^64
       non-overlapping subsequences for parallel computations. */

    void jump() noexcept {
        static std::uint64_t const JUMP[] = {0xdf900294d8f554a5, 0x170865df4b3201fc};

        std::uint64_t s0 = 0;
        std::uint64_t s1 = 0;
        for (int i = 0; i < sizeof(JUMP) / sizeof(*JUMP); ++i)
            for (int b = 0; b < 64; ++b) {
                if (JUMP[i] & UINT64_C(1) << b) {
                    s0 ^= s_[0];
                    s1 ^= s_[1];
                }
                operator()();
            }

        s_[0] = s0;
        s_[1] = s1;
    }

    /* This is the long-jump function for the generator. It is equivalent to
       2^96 calls to next(); it can be used to generate 2^32 starting points,
       from each of which jump() will generate 2^32 non-overlapping
       subsequences for parallel distributed computations. */

    void long_jump() noexcept {
        static std::uint64_t const LONG_JUMP[] = {0xd2a98b26625eee7b, 0xdddf9b1090aa7ac1};

        std::uint64_t s0 = 0;
        std::uint64_t s1 = 0;
        for (int i = 0; i < sizeof(LONG_JUMP) / sizeof(*LONG_JUMP); ++i)
            for (int b = 0; b < 64; ++b) {
                if (LONG_JUMP[i] & UINT64_C(1) << b) {
                    s0 ^= s_[0];
                    s1 ^= s_[1];
                }
                operator()();
            }

        s_[0] = s0;
        s_[1] = s1;
    }
};
