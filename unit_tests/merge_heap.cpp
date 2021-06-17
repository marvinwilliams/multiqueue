#include "catch2/catch_test_macros.hpp"

#include <algorithm>
#include <array>
#include <functional>
#include <queue>
#include <random>

#include "multiqueue/sequential/heap/merge_heap.hpp"
#include "multiqueue/util/extractors.hpp"

TEST_CASE("merge_heap simple", "[merge_heap]") {
    multiqueue::sequential::merge_heap<
        std::uint32_t, std::uint32_t, multiqueue::util::identity<std::uint32_t>, std::less<std::uint32_t>, 8u>
        heap;
    std::array<std::uint32_t, 8> in = {1, 3, 5, 7, 8, 9, 10, 12};
    heap.insert(in.begin(), in.end());
    REQUIRE(heap.top() == 1);
    heap.pop_node();
    in = {2, 4, 6, 11, 13, 14, 15, 16};
    heap.insert(in.begin(), in.end());
    REQUIRE(heap.top() == 2);
}

TEST_CASE("merge_heap push pop", "[merge_heap]") {
    multiqueue::sequential::merge_heap<
        std::uint32_t, std::uint32_t, multiqueue::util::identity<std::uint32_t>, std::less<std::uint32_t>, 8u>
        heap;

    std::array<std::uint32_t, 8> input;

    SECTION("push increasing numbers and pop them") {
        for (std::uint32_t i = 0; i < 1000; ++i) {
            std::iota(input.begin(), input.end(), i * 8);
            heap.insert(input.begin(), input.end());
        }

        for (std::uint32_t i = 0; i < 1000; ++i) {
            std::iota(input.begin(), input.end(), i * 8);
            REQUIRE(std::equal(heap.top_node().begin(), heap.top_node().end(), input.begin()));
            heap.pop_node();
        }
        REQUIRE(heap.empty());
    }

    SECTION("push decreasing numbers and pop them") {
        for (std::uint32_t i = 1000; i > 0; --i) {
            std::iota(input.begin(), input.end(), (i - 1) * 8);
            heap.insert(input.begin(), input.end());
        }

        for (std::uint32_t i = 0; i < 1000; ++i) {
            std::iota(input.begin(), input.end(), i * 8);
            REQUIRE(std::equal(heap.top_node().begin(), heap.top_node().end(), input.begin()));
            heap.pop_node();
        }
        REQUIRE(heap.empty());
    }

    SECTION(
        "first push increasing numbers, then push decreasing numbers and "
        "pop them") {
        for (std::uint32_t i = 1; i <= 500; ++i) {
            std::iota(input.begin(), input.end(), i * 8);
            heap.insert(input.begin(), input.end());
        }
        for (std::uint32_t i = 1000; i > 500; --i) {
            std::iota(input.begin(), input.end(), i * 8);
            heap.insert(input.begin(), input.end());
        }
        for (std::uint32_t i = 1; i <= 1000; ++i) {
            std::iota(input.begin(), input.end(), i * 8);
            REQUIRE(std::equal(heap.top_node().begin(), heap.top_node().end(), input.begin()));
            heap.pop_node();
        }
        REQUIRE(heap.empty());
    }

    SECTION("push increasing interleaved numbers and pop them") {
        for (std::uint32_t i = 0; i < 1000; ++i) {
            std::iota(input.begin(), input.end(), 0);
            std::transform(input.begin(), input.end(), input.begin(), [&](auto n) { return i + n * 1000; });
            heap.insert(input.begin(), input.end());
        }

        for (std::uint32_t i = 0; i < 1000; ++i) {
            std::iota(input.begin(), input.end(), i * 8);
            REQUIRE(std::equal(heap.top_node().begin(), heap.top_node().end(), input.begin()));
            heap.pop_node();
        }
        REQUIRE(heap.empty());
    }

    SECTION("push increasing interleaved numbers and pop them") {
        for (std::uint32_t i = 1000; i > 0; --i) {
            std::iota(input.begin(), input.end(), 0);
            std::transform(input.begin(), input.end(), input.begin(), [&](auto n) { return (i - 1) + n * 1000; });
            heap.insert(input.begin(), input.end());
        }

        for (std::uint32_t i = 0; i < 1000; ++i) {
            std::iota(input.begin(), input.end(), i * 8);
            REQUIRE(std::equal(heap.top_node().begin(), heap.top_node().end(), input.begin()));
            heap.pop_node();
        }
        REQUIRE(heap.empty());
    }
}

TEST_CASE("merge_heap workloads", "[merge_heap]") {
    multiqueue::sequential::merge_heap<
        std::uint32_t, std::uint32_t, multiqueue::util::identity<std::uint32_t>, std::less<std::uint32_t>, 8u>
        heap;

    std::array<std::uint32_t, 8> input;
    auto ref_pq = std::priority_queue<std::uint32_t, std::vector<std::uint32_t>, std::greater<std::uint32_t>>{};
    auto gen = std::mt19937{0};
    auto dist = std::uniform_int_distribution<std::uint32_t>{0, 100};

    SECTION("push random numbers and pop them") {
        for (size_t i = 0; i < 1000; ++i) {
            std::generate(input.begin(), input.end(), [&]() { return dist(gen); });
            std::sort(input.begin(), input.end());
            std::for_each(input.begin(), input.end(), [&](auto n) { ref_pq.push(n); });
            heap.insert(input.begin(), input.end());
            REQUIRE(heap.top_node().front() == ref_pq.top());
        }

        for (size_t i = 0; i < 1000; ++i) {
            for (auto const& t : heap.top_node()) {
                REQUIRE(t == ref_pq.top());
                ref_pq.pop();
            }
            heap.pop_node();
        }
        REQUIRE(heap.empty());
    }

    SECTION("interleave pushing and popping random numbers") {
        auto seq_dist = std::uniform_int_distribution<std::uint32_t>{0, 10};

        for (std::uint32_t s = 0; s < 1000; ++s) {
            auto num_push = seq_dist(gen);
            for (std::uint32_t i = 0; i < num_push; ++i) {
                std::generate(input.begin(), input.end(), [&]() { return dist(gen); });
                std::sort(input.begin(), input.end());
                std::for_each(input.begin(), input.end(), [&](auto n) { ref_pq.push(n); });
                heap.insert(input.begin(), input.end());
                REQUIRE(heap.top_node().front() == ref_pq.top());
            }
            auto num_pop = seq_dist(gen);
            while (num_pop > 0) {
                if (!heap.empty()) {
                    for (auto const& t : heap.top_node()) {
                        REQUIRE(t == ref_pq.top());
                        ref_pq.pop();
                    }
                    heap.pop_node();
                }
                --num_pop;
            }
        }
        while (!heap.empty()) {
            for (auto const& t : heap.top_node()) {
                REQUIRE(t == ref_pq.top());
                ref_pq.pop();
            }
            heap.pop_node();
        }
    }

    SECTION("dijkstra") {
        auto seq_dist = std::uniform_int_distribution<std::uint32_t>{1, 10};

        std::iota(input.begin(), input.end(), 0);
        std::for_each(input.begin(), input.end(), [&](auto n) { ref_pq.push(n); });
        heap.insert(input.begin(), input.end());

        for (std::uint32_t s = 0; s < 1000; ++s) {
            auto top_node = heap.top_node();
            heap.pop_node();
            for (auto const& t : top_node) {
                REQUIRE(t == ref_pq.top());
                ref_pq.pop();
            }
            auto num_push = seq_dist(gen);
            for (std::uint32_t i = 0; i < num_push; ++i) {
                std::transform(top_node.begin(), top_node.end(), input.begin(), [&](auto n) { return n + dist(gen); });
                std::sort(input.begin(), input.end());
                std::for_each(input.begin(), input.end(), [&](auto n) { ref_pq.push(n); });
                heap.insert(input.begin(), input.end());
            }
        }
        while (!heap.empty()) {
            for (auto const& t : heap.top_node()) {
                REQUIRE(t == ref_pq.top());
                ref_pq.pop();
            }
            heap.pop_node();
        }
    }
}
