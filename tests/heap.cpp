#include "multiqueue/heap.hpp"
#include "test_types.hpp"

#include "catch2/catch_template_test_macros.hpp"
#include "catch2/catch_test_macros.hpp"
#include "catch2/generators/catch_generators_all.hpp"
#include "catch2/generators/catch_generators_random.hpp"

#include <algorithm>
#include <array>
#include <cstdlib>
#include <functional>
#include <iterator>
#include <list>
#include <queue>
#include <random>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

struct HeapInfo {
    std::size_t index;
};

TEMPLATE_TEST_CASE_SIG("heap supports basic operations", "[heap][basic]", ((unsigned int Degree), Degree), 2, 3, 4,
                       99) {
    std::vector<HeapInfo> infos(1000);
    using heap_t = multiqueue::Heap<std::pair<int, int>, std::less<>, Degree>;

    auto heap = heap_t{};

    SECTION("push increasing numbers and pop them") {
        for (int n = 0; n < 1000; ++n) {
            heap.push({n, n}, infos.data());
        }

        for (int i = 0; i < 1000; ++i) {
            REQUIRE(heap.top().first == i);
            heap.pop(infos.data());
        }
        REQUIRE(heap.empty());
    }

    SECTION("push decreasing numbers and pop them") {
        for (int n = 999; n >= 0; --n) {
            heap.push({n, n}, infos.data());
        }

        for (int i = 0; i < 1000; ++i) {
            REQUIRE(heap.top().first == i);
            heap.pop(infos.data());
        }
        REQUIRE(heap.empty());
    }

    SECTION(
        "first push increasing numbers, then push decreasing numbers and "
        "pop them") {
        for (int i = 0; i <= 500; ++i) {
            heap.push({i, i}, infos.data());
        }
        for (int i = 999; i > 500; --i) {
            heap.push({i, i}, infos.data());
        }
        for (int i = 0; i < 1000; ++i) {
            REQUIRE(heap.top().first == i);
            heap.pop(infos.data());
        }
        REQUIRE(heap.empty());
    }
}

TEST_CASE("heap can use std::greater as comparator", "[heap][comparator]") {
    std::vector<HeapInfo> infos(1000);
    using heap_t = multiqueue::Heap<std::pair<int, int>, std::greater<>>;

    auto heap = heap_t{};

    SECTION("push increasing numbers and pop them") {
        for (int n = 0; n < 1000; ++n) {
            heap.push({n, n}, infos.data());
        }

        for (int i = 999; i >= 0; --i) {
            REQUIRE(heap.top().first == i);
            heap.pop(infos.data());
        }
        REQUIRE(heap.empty());
    }

    SECTION("push decreasing numbers and pop them") {
        for (int n = 999; n >= 0; --n) {
            heap.push({n, n}, infos.data());
        }

        for (int i = 999; i >= 0; --i) {
            REQUIRE(heap.top().first == i);
            heap.pop(infos.data());
        }
        REQUIRE(heap.empty());
    }

    SECTION(
        "first push increasing numbers, then push decreasing numbers and "
        "pop them") {
        for (int i = 0; i < 500; ++i) {
            heap.push({i, i}, infos.data());
        }
        for (int i = 999; i >= 500; --i) {
            heap.push({i, i}, infos.data());
        }
        for (int i = 999; i >= 0; --i) {
            REQUIRE(heap.top().first == i);
            heap.pop(infos.data());
        }
        REQUIRE(heap.empty());
    }
}

TEST_CASE("heap works with randomized workloads", "[heap][workloads]") {
    std::vector<HeapInfo> infos(1000);
    using heap_t = multiqueue::Heap<std::pair<int, int>, std::less<>>;

    auto heap = heap_t{};
    auto ref_pq = std::priority_queue<int, std::vector<int>, std::greater<int>>{};
    auto gen = std::mt19937{0};

    SECTION("push random numbers and pop them") {
        auto dist = std::uniform_int_distribution{-100, 100};

        for (std::size_t i = 0; i < 1000; ++i) {
            auto n = dist(gen);
            heap.push({n, i}, infos.data());
            ref_pq.push(n);
            REQUIRE(heap.top().first == ref_pq.top());
        }

        for (std::size_t i = 0; i < 1000; ++i) {
            REQUIRE(heap.top().first == ref_pq.top());
            heap.pop(infos.data());
            ref_pq.pop();
        }
        REQUIRE(heap.empty());
    }

    SECTION("interleave pushing and popping random numbers") {
        auto dist = std::uniform_int_distribution{-100, 100};
        auto seq_dist = std::uniform_int_distribution{0, 10};

        for (int s = 0; s < 100; ++s) {
            auto num_push = seq_dist(gen);
            for (int i = 0; i < num_push; ++i) {
                auto n = dist(gen);
                heap.push({n, s * 10 + i}, infos.data());
                ref_pq.push(n);
                REQUIRE(heap.top().first == ref_pq.top());
            }
            auto num_pop = seq_dist(gen);
            for (int i = 0; i > num_pop; --i) {
                if (!heap.empty()) {
                    REQUIRE(heap.top().first == ref_pq.top());
                    heap.pop(infos.data());
                    ref_pq.pop();
                }
            }
        }
        while (!heap.empty()) {
            REQUIRE(heap.top().first == ref_pq.top());
            heap.pop(infos.data());
            ref_pq.pop();
        }
    }

    SECTION("dijkstra") {
        auto dist = std::uniform_int_distribution{-100, 100};
        auto seq_dist = std::uniform_int_distribution{1, 10};

        heap.push({0, 0}, infos.data());
        ref_pq.push(0);
        for (int s = 0; s < 100; ++s) {
            auto top = heap.top();
            heap.pop(infos.data());
            ref_pq.pop();
            auto num_push = seq_dist(gen);
            for (int i = 0; i < num_push; ++i) {
                top.first += dist(gen);
                top.second = s * 10 + i;
                heap.push(top, infos.data());
                ref_pq.push(top.first);
                REQUIRE(heap.top().first == ref_pq.top());
            }
        }
        while (!heap.empty()) {
            REQUIRE(heap.top().first == ref_pq.top());
            heap.pop(infos.data());
            ref_pq.pop();
        }
    }
}

TEST_CASE("heap works with non-default-constructible types", "[heap][types]") {
    std::vector<HeapInfo> infos(3);
    using heap_t = multiqueue::Heap<std::pair<test_types::nodefault, int>, std::less<>>;
    heap_t heap{};
    heap.push({test_types::nodefault(0), 1}, infos.data());
    test_types::nodefault t1(2);
    std::pair<test_types::nodefault, int> tp(t1, 2);
    heap.push(tp, infos.data());
    std::pair<test_types::nodefault, int> t2{2, 3};
    t2 = heap.top();
    heap.pop(infos.data());
    heap.pop(infos.data());
}
