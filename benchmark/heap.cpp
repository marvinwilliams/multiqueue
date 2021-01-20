#include "catch2/catch.hpp"
#include "multiqueue/pq.hpp"
#include "multiqueue/heap/full_up_strategy.hpp"

#include <iterator>
#include <queue>
#include <random>
#include <string_view>

#ifndef NDEBUG
#error "Benchmarks must not be compiled in debug build!"
#endif

static constexpr int reps = 100'000;

TEST_CASE("std::priority_queue", "[benchmark][std]") {
    auto pq = std::priority_queue<int, std::vector<int>, std::greater<int>>{};

    BENCHMARK("up") {
        for (int i = 1; i <= reps; ++i) {
            pq.push(i);
        }
        for (int i = 1; i <= reps; ++i) {
            pq.pop();
        }
        // to guarantee computation
        return pq.empty();
    };

    BENCHMARK("down") {
        for (int i = reps; i > 0; --i) {
            pq.push(i);
        }
        for (int i = 1; i <= reps; ++i) {
            pq.pop();
        }
        // to guarantee computation
        return pq.empty();
    };

    BENCHMARK("up_down") {
        for (int i = 1; i <= reps / 2; ++i) {
            pq.push(i);
        }
        for (int i = reps; i > reps / 2; --i) {
            pq.push(i);
        }
        for (int i = 1; i <= reps; ++i) {
            pq.pop();
        }
        // to guarantee computation
        return pq.empty();
    };
}

template <unsigned int D>
struct DegreeHeapSettings {
    template <typename T>
    struct HeapSettings : multiqueue::local_nonaddressable::default_heap_settings<T> {
        static constexpr unsigned int Degree = D;
    };
};

TEMPLATE_TEST_CASE_SIG("Degree", "[benchmark][pq][degree]", ((unsigned int Degree), Degree), 2, 4, 8, 16) {
    using pq_t =
        multiqueue::local_nonaddressable::pq<int, std::less<int>, DegreeHeapSettings<Degree>::template HeapSettings>;

    auto pq = pq_t{};

    BENCHMARK("up") {
        for (int i = 1; i <= reps; ++i) {
            pq.push(i);
        }
        for (int i = 1; i <= reps; ++i) {
            pq.pop();
        }
        // to guarantee computation
        return pq.empty();
    };

    BENCHMARK("down") {
        for (int i = reps; i > 0; --i) {
            pq.push(i);
        }
        for (int i = 1; i <= reps; ++i) {
            pq.pop();
        }
        // to guarantee computation
        return pq.empty();
    };

    BENCHMARK("up_down") {
        for (int i = 1; i <= reps / 2; ++i) {
            pq.push(i);
        }
        for (int i = reps; i > reps / 2; --i) {
            pq.push(i);
        }
        for (int i = 1; i <= reps; ++i) {
            pq.pop();
        }
        // to guarantee computation
        return pq.empty();
    };
}

    template <typename T>
    struct StrategyHeapSettings : multiqueue::local_nonaddressable::default_heap_settings<T> {
      using Strategy = multiqueue::local_nonaddressable::full_up_strategy;
    };
TEST_CASE("Full Up Strategy", "[benchmark][pq][strategy]") {
    using pq_t =
        multiqueue::local_nonaddressable::pq<int, std::less<int>, StrategyHeapSettings>;

    auto pq = pq_t{};

    BENCHMARK("up") {
        for (int i = 1; i <= reps; ++i) {
            pq.push(i);
        }
        for (int i = 1; i <= reps; ++i) {
            pq.pop();
        }
        // to guarantee computation
        return pq.empty();
    };

    BENCHMARK("down") {
        for (int i = reps; i > 0; --i) {
            pq.push(i);
        }
        for (int i = 1; i <= reps; ++i) {
            pq.pop();
        }
        // to guarantee computation
        return pq.empty();
    };

    BENCHMARK("up_down") {
        for (int i = 1; i <= reps / 2; ++i) {
            pq.push(i);
        }
        for (int i = reps; i > reps / 2; --i) {
            pq.push(i);
        }
        for (int i = 1; i <= reps; ++i) {
            pq.pop();
        }
        // to guarantee computation
        return pq.empty();
    };
}
