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

TEMPLATE_TEST_CASE_SIG("heap supports basic operations", "[heap][basic]", ((unsigned int Degree), Degree), 2, 3, 4,
                       99) {
    using heap_t = multiqueue::Heap<int, std::less<>, Degree>;

    auto heap = heap_t{};

    SECTION("push increasing numbers and pop them") {
        for (int n = 0; n < 1000; ++n) {
            heap.push(n);
        }

        for (int i = 0; i < 1000; ++i) {
            REQUIRE(heap.top() == i);
            heap.pop();
        }
        REQUIRE(heap.empty());
    }

    SECTION("push decreasing numbers and pop them") {
        for (int n = 999; n >= 0; --n) {
            heap.push(n);
        }

        for (int i = 0; i < 1000; ++i) {
            REQUIRE(heap.top() == i);
            heap.pop();
        }
        REQUIRE(heap.empty());
    }

    SECTION(
        "first push increasing numbers, then push decreasing numbers and "
        "pop them") {
        for (int i = 1; i <= 500; ++i) {
            heap.push(i);
        }
        for (int i = 1000; i > 500; --i) {
            heap.push(i);
        }
        for (int i = 1; i <= 1000; ++i) {
            REQUIRE(heap.top() == i);
            heap.pop();
        }
        REQUIRE(heap.empty());
    }
}

TEST_CASE("heap can use std::greater as comparator", "[heap][comparator]") {
    using heap_t = multiqueue::Heap<int, std::greater<>>;

    auto heap = heap_t{};

    SECTION("push increasing numbers and pop them") {
        for (int n = 0; n < 1000; ++n) {
            heap.push(n);
        }

        for (int i = 999; i >= 0; --i) {
            REQUIRE(heap.top() == i);
            heap.pop();
        }
        REQUIRE(heap.empty());
    }

    SECTION("push decreasing numbers and pop them") {
        for (int n = 999; n >= 0; --n) {
            heap.push(n);
        }

        for (int i = 999; i >= 0; --i) {
            REQUIRE(heap.top() == i);
            heap.pop();
        }
        REQUIRE(heap.empty());
    }

    SECTION(
        "first push increasing numbers, then push decreasing numbers and "
        "pop them") {
        for (int i = 0; i < 500; ++i) {
            heap.push(i);
        }
        for (int i = 999; i >= 500; --i) {
            heap.push(i);
        }
        for (int i = 999; i >= 0; --i) {
            REQUIRE(heap.top() == i);
            heap.pop();
        }
        REQUIRE(heap.empty());
    }
}

TEST_CASE("heap works with randomized workloads", "[heap][workloads]") {
    using heap_t = multiqueue::Heap<int, std::less<>>;

    auto heap = heap_t{};
    auto ref_pq = std::priority_queue<int, std::vector<int>, std::greater<int>>{};
    auto gen = std::mt19937{0};

    SECTION("push random numbers and pop them") {
        auto dist = std::uniform_int_distribution{-100, 100};

        for (std::size_t i = 0; i < 1000; ++i) {
            auto n = dist(gen);
            heap.push(n);
            ref_pq.push(n);
            REQUIRE(heap.top() == ref_pq.top());
        }

        for (std::size_t i = 0; i < 1000; ++i) {
            REQUIRE(heap.top() == ref_pq.top());
            heap.pop();
            ref_pq.pop();
        }
        REQUIRE(heap.empty());
    }

    SECTION("interleave pushing and popping random numbers") {
        auto dist = std::uniform_int_distribution{-100, 100};
        auto seq_dist = std::uniform_int_distribution{0, 10};

        for (int s = 0; s < 1000; ++s) {
            auto num_push = seq_dist(gen);
            for (int i = 0; i < num_push; ++i) {
                auto n = dist(gen);
                heap.push(n);
                ref_pq.push(n);
                REQUIRE(heap.top() == ref_pq.top());
            }
            auto num_pop = seq_dist(gen);
            for (int i = 0; i > num_pop; --i) {
                if (!heap.empty()) {
                    REQUIRE(heap.top() == ref_pq.top());
                    heap.pop();
                    ref_pq.pop();
                }
            }
        }
        while (!heap.empty()) {
            REQUIRE(heap.top() == ref_pq.top());
            heap.pop();
            ref_pq.pop();
        }
    }

    SECTION("dijkstra") {
        auto dist = std::uniform_int_distribution{-100, 100};
        auto seq_dist = std::uniform_int_distribution{1, 10};

        heap.push(0);
        ref_pq.push(0);
        for (int s = 0; s < 1000; ++s) {
            auto top = heap.top();
            heap.pop();
            ref_pq.pop();
            auto num_push = seq_dist(gen);
            for (int i = 0; i < num_push; ++i) {
                auto n = top + dist(gen);
                heap.push(n);
                ref_pq.push(n);
                REQUIRE(heap.top() == ref_pq.top());
            }
        }
        while (!heap.empty()) {
            REQUIRE(heap.top() == ref_pq.top());
            heap.pop();
            ref_pq.pop();
        }
    }
}

TEST_CASE("heap works with non-default-constructible types", "[heap][types]") {
    using heap_t = multiqueue::Heap<std::pair<test_types::nodefault, test_types::nodefault>, std::less<>>;
    heap_t heap{};
    heap.push({test_types::nodefault(0), test_types::nodefault(1)});
    test_types::nodefault t1(2);
    std::pair<test_types::nodefault, test_types::nodefault> tp(t1, t1);
    heap.push(tp);
    std::pair<test_types::nodefault, test_types::nodefault> t2{2, 3};
    t2 = heap.top();
    heap.pop();
    heap.pop();
}
