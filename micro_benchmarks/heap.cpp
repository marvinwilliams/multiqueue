#include "multiqueue/sequential/heap/full_down_strategy.hpp"
#include "multiqueue/sequential/heap/full_up_strategy.hpp"
#include "multiqueue/sequential/heap/heap.hpp"
#include "multiqueue/util/extractors.hpp"

#include "catch2/benchmark/catch_benchmark.hpp"
#include "catch2/catch_template_test_macros.hpp"
#include "catch2/catch_test_macros.hpp"

#include <iterator>
#include <queue>
#include <random>

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

TEMPLATE_TEST_CASE_SIG("Degree", "[benchmark][heap][degree]", ((unsigned int Degree), Degree), 2, 4, 8, 16) {
    using heap_t = multiqueue::sequential::heap<int, int, multiqueue::util::identity<int>, std::less<int>, Degree,
                                                multiqueue::sequential::sift_strategy::FullUp, std::allocator<int>>;

    auto heap = heap_t{};

    BENCHMARK("up") {
        for (int i = 1; i <= reps; ++i) {
            heap.insert(i);
        }
        for (int i = 1; i <= reps; ++i) {
            heap.pop();
        }
        // to guarantee computation
        return heap.empty();
    };

    BENCHMARK("down") {
        for (int i = reps; i > 0; --i) {
            heap.insert(i);
        }
        for (int i = 1; i <= reps; ++i) {
            heap.pop();
        }
        // to guarantee computation
        return heap.empty();
    };

    BENCHMARK("up_down") {
        for (int i = 1; i <= reps / 2; ++i) {
            heap.insert(i);
        }
        for (int i = reps; i > reps / 2; --i) {
            heap.insert(i);
        }
        for (int i = 1; i <= reps; ++i) {
            heap.pop();
        }
        // to guarantee computation
        return heap.empty();
    };
}

TEST_CASE("Full Up Strategy", "[benchmark][heap][strategy]") {
    using heap_t = multiqueue::sequential::heap<int, int, multiqueue::util::identity<int>, std::less<int>, 4,
                                                multiqueue::sequential::sift_strategy::FullDown, std::allocator<int>>;

    auto heap = heap_t{};

    BENCHMARK("up") {
        for (int i = 1; i <= reps; ++i) {
            heap.insert(i);
        }
        for (int i = 1; i <= reps; ++i) {
            heap.pop();
        }
        // to guarantee computation
        return heap.empty();
    };

    BENCHMARK("down") {
        for (int i = reps; i > 0; --i) {
            heap.insert(i);
        }
        for (int i = 1; i <= reps; ++i) {
            heap.pop();
        }
        // to guarantee computation
        return heap.empty();
    };

    BENCHMARK("up_down") {
        for (int i = 1; i <= reps / 2; ++i) {
            heap.insert(i);
        }
        for (int i = reps; i > reps / 2; --i) {
            heap.insert(i);
        }
        for (int i = 1; i <= reps; ++i) {
            heap.pop();
        }
        // to guarantee computation
        return heap.empty();
    };
}
