#include "multiqueue/heap.hpp"
#include "multiqueue/buffered_pq.hpp"

#include "catch2/benchmark/catch_benchmark.hpp"
#include "catch2/catch_template_test_macros.hpp"

#include <iterator>
#include <queue>
#include <vector>

static constexpr int reps = 500'000;

TEST_CASE("std::priority_queue", "[benchmark][std]") {
    auto pq = std::priority_queue<int, std::vector<int>, std::greater<>>{};

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

    BENCHMARK("mixed") {
        for (int i = 1; i <= reps / 4; ++i) {
            pq.push(i * 3);
            pq.push(i);
            pq.push(i * 4);
            pq.push(i * 2);
            pq.pop();
            pq.pop();
            pq.pop();
        }
        for (int i = 1; i <= reps / 4; ++i) {
            pq.pop();
        }
        // to guarantee computation
        return pq.empty();
    };
}

TEMPLATE_TEST_CASE_SIG("Degree", "[benchmark][heap][degree]", ((unsigned int Degree), Degree), 2, 4, 8, 16, 64) {
    using heap_t = multiqueue::Heap<int, std::less<>, Degree>;

    auto heap = heap_t{};

    BENCHMARK("up") {
        for (int i = 1; i <= reps; ++i) {
            heap.push(i);
        }
        for (int i = 1; i <= reps; ++i) {
            heap.pop();
        }
        // to guarantee computation
        return heap.empty();
    };

    BENCHMARK("down") {
        for (int i = reps; i > 0; --i) {
            heap.push(i);
        }
        for (int i = 1; i <= reps; ++i) {
            heap.pop();
        }
        // to guarantee computation
        return heap.empty();
    };

    BENCHMARK("up_down") {
        for (int i = 1; i <= reps / 2; ++i) {
            heap.push(i);
        }
        for (int i = reps; i > reps / 2; --i) {
            heap.push(i);
        }
        for (int i = 1; i <= reps; ++i) {
            heap.pop();
        }
        // to guarantee computation
        return heap.empty();
    };

    BENCHMARK("mixed") {
        for (int i = 1; i <= reps / 4; ++i) {
            heap.push(i * 3);
            heap.push(i);
            heap.push(i * 4);
            heap.push(i * 2);
            heap.pop();
            heap.pop();
            heap.pop();
        }
        for (int i = 1; i <= reps / 4; ++i) {
            heap.pop();
        }
        // to guarantee computation
        return heap.empty();
    };
}

TEMPLATE_TEST_CASE_SIG("BufferedPQ", "[benchmark][buffered_pq]", ((unsigned int Buffersize), Buffersize), 4, 8, 16, 64,
                       256) {
    using pq_t = multiqueue::BufferedPQ<multiqueue::Heap<int, std::less<>>, Buffersize, Buffersize>;

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

    BENCHMARK("mixed") {
        for (int i = 1; i <= reps / 4; ++i) {
            pq.push(i * 3);
            pq.push(i);
            pq.push(i * 4);
            pq.push(i * 2);
            pq.pop();
            pq.pop();
            pq.pop();
        }
        for (int i = 1; i <= reps / 4; ++i) {
            pq.pop();
        }
        // to guarantee computation
        return pq.empty();
    };
}

TEMPLATE_TEST_CASE_SIG("BufferedPQ std::pq", "[benchmark][buffered_pq]", ((unsigned int Buffersize), Buffersize), 4, 8,
                       16, 64, 256) {
    using pq_t =
        multiqueue::BufferedPQ<std::priority_queue<int, std::vector<int>, std::greater<>>, Buffersize, Buffersize>;

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

    BENCHMARK("mixed") {
        for (int i = 1; i <= reps / 4; ++i) {
            pq.push(i * 3);
            pq.push(i);
            pq.push(i * 4);
            pq.push(i * 2);
            pq.pop();
            pq.pop();
            pq.pop();
        }
        for (int i = 1; i <= reps / 4; ++i) {
            pq.pop();
        }
        // to guarantee computation
        return pq.empty();
    };
}
