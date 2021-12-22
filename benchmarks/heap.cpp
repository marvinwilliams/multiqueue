#include "multiqueue/heap.hpp"

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
    struct Info {
        std::size_t index;
    };
    std::vector<Info> infos(reps);
    using heap_t = multiqueue::Heap<std::pair<int, int>, std::less<>, Degree>;

    auto heap = heap_t{};

    BENCHMARK("up") {
        for (int i = 1; i <= reps; ++i) {
            heap.push({i, i}, infos.data());
        }
        for (int i = 1; i <= reps; ++i) {
            heap.pop(infos.data());
        }
        // to guarantee computation
        return heap.empty();
    };

    BENCHMARK("down") {
        for (int i = reps; i > 0; --i) {
            heap.push({i, i}, infos.data());
        }
        for (int i = 1; i <= reps; ++i) {
            heap.pop(infos.data());
        }
        // to guarantee computation
        return heap.empty();
    };

    BENCHMARK("up_down") {
        for (int i = 1; i <= reps / 2; ++i) {
            heap.push({i, i}, infos.data());
        }
        for (int i = reps; i > reps / 2; --i) {
            heap.push({i, i}, infos.data());
        }
        for (int i = 1; i <= reps; ++i) {
            heap.pop(infos.data());
        }
        // to guarantee computation
        return heap.empty();
    };

    BENCHMARK("mixed") {
        for (int i = 1; i <= reps / 4; ++i) {
            heap.push({i * 3, i * 3}, infos.data());
            heap.push({i, i}, infos.data());
            heap.push({i * 4, i*4}, infos.data());
            heap.push({i * 2, i*2}, infos.data());
            heap.pop(infos.data());
            heap.pop(infos.data());
            heap.pop(infos.data());
        }
        for (int i = 1; i <= reps / 4; ++i) {
            heap.pop(infos.data());
        }
        // to guarantee computation
        return heap.empty();
    };
}
