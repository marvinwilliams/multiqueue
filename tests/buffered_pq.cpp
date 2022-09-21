#include "multiqueue/buffered_pq.hpp"
#include "multiqueue/heap.hpp"

#include "catch2/catch_template_test_macros.hpp"
#include "catch2/generators/catch_generators_all.hpp"

#include <array>
#include <list>
#include <queue>
#include <random>
#include <type_traits>
#include <vector>

TEST_CASE("buffered pq supports basic operations", "[buffered_pq][basic]") {
    using pq_t = multiqueue::BufferedPQ<8, 8, multiqueue::Heap<int>>;

    auto pq = pq_t{};

    SECTION("push increasing numbers and pop them") {
        for (int n = 0; n < 1000; ++n) {
            pq.push(n);
        }

        for (int i = 0; i < 1000; ++i) {
            REQUIRE(pq.top() == 999 - i);
            pq.pop();
        }
        REQUIRE(pq.empty());
    }

    SECTION("push decreasing numbers and pop them") {
        for (int n = 999; n >= 0; --n) {
            pq.push(n);
        }

        for (int i = 0; i < 1000; ++i) {
            REQUIRE(pq.top() == 999 - i);
            pq.pop();
        }
        REQUIRE(pq.empty());
    }

    SECTION(
        "first push increasing numbers, then push decreasing numbers and "
        "pop them") {
        for (int i = 0; i < 500; ++i) {
            pq.push(i);
        }
        for (int i = 999; i >= 500; --i) {
            pq.push(i);
        }
        for (int i = 0; i < 1000; ++i) {
            REQUIRE(pq.top() == 999 - i);
            pq.pop();
        }
        REQUIRE(pq.empty());
    }
}

TEST_CASE("buffered pq can use std::greater as comparator", "[buffered_pq][comparator]") {
    using pq_t = multiqueue::BufferedPQ<8, 8, multiqueue::Heap<int, std::greater<>>>;

    auto pq = pq_t{};

    SECTION("push increasing numbers and pop them") {
        for (int n = 0; n < 1000; ++n) {
            pq.push(n);
        }

        for (int i = 0; i < 1000; ++i) {
            REQUIRE(pq.top() == i);
            pq.pop();
        }
        REQUIRE(pq.empty());
    }

    SECTION("push decreasing numbers and pop them") {
        for (int n = 999; n >= 0; --n) {
            pq.push(n);
        }

        for (int i = 0; i < 1000; ++i) {
            REQUIRE(pq.top() == i);
            pq.pop();
        }
        REQUIRE(pq.empty());
    }

    SECTION(
        "first push increasing numbers, then push decreasing numbers and "
        "pop them") {
        for (int i = 0; i < 500; ++i) {
            pq.push(i);
        }
        for (int i = 999; i >= 500; --i) {
            pq.push(i);
        }
        for (int i = 0; i < 1000; ++i) {
            REQUIRE(pq.top() == i);
            pq.pop();
        }
        REQUIRE(pq.empty());
    }
}

TEST_CASE("buffered pq works with randomized workloads", "[buffered_pq][workloads]") {
    using pq_t = multiqueue::BufferedPQ<8, 8, multiqueue::Heap<int, std::greater<>>>;

    auto pq = pq_t{};
    auto ref_pq = std::priority_queue<int, std::vector<int>, std::greater<>>{};
    auto gen = std::mt19937{42};

    SECTION("push random numbers and pop them") {
        auto dist = std::uniform_int_distribution{-100, 100};

        for (std::size_t i = 0; i < 1000; ++i) {
            auto n = dist(gen);
            pq.push(n);
            ref_pq.push(n);
            REQUIRE(pq.top() == ref_pq.top());
        }

        for (std::size_t i = 0; i < 1000; ++i) {
            REQUIRE(pq.top() == ref_pq.top());
            pq.pop();
            ref_pq.pop();
        }
        REQUIRE(pq.empty());
        REQUIRE(ref_pq.empty());
    }

    SECTION("interleave pushing and popping random numbers") {
        auto dist = std::uniform_int_distribution{-100, 100};
        auto seq_dist = std::uniform_int_distribution{0, 10};

        for (int s = 0; s < 1000; ++s) {
            auto num_push = seq_dist(gen);
            for (int i = 0; i < num_push; ++i) {
                auto n = dist(gen);
                pq.push(n);
                ref_pq.push(n);
                REQUIRE(pq.top() == ref_pq.top());
            }
            auto num_pop = seq_dist(gen);
            for (int i = 0; i > num_pop; --i) {
                if (!pq.empty()) {
                    REQUIRE(pq.top() == ref_pq.top());
                    pq.pop();
                    ref_pq.pop();
                }
            }
        }
        while (!pq.empty()) {
            REQUIRE(!ref_pq.empty());
            REQUIRE(pq.top() == ref_pq.top());
            pq.pop();
            ref_pq.pop();
        }
        REQUIRE(ref_pq.empty());
    }

    SECTION("dijkstra") {
        auto dist = std::uniform_int_distribution{-100, 100};
        auto seq_dist = std::uniform_int_distribution{1, 10};

        pq.push(0);
        ref_pq.push(0);
        for (int s = 0; s < 1000; ++s) {
            auto top = pq.top();
            pq.pop();
            ref_pq.pop();
            auto num_push = seq_dist(gen);
            for (int i = 0; i < num_push; ++i) {
                auto n = top + dist(gen);
                pq.push(n);
                ref_pq.push(n);
                REQUIRE(pq.top() == ref_pq.top());
            }
        }
        while (!pq.empty()) {
            REQUIRE(!ref_pq.empty());
            REQUIRE(pq.top() == ref_pq.top());
            pq.pop();
            ref_pq.pop();
        }
        REQUIRE(ref_pq.empty());
    }
}
