#include "multiqueue/buffered_pq.hpp"
#include "multiqueue/addressable.hpp"
#include "multiqueue/default_configuration.hpp"
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

struct BufferedInfo {
    multiqueue::Location location;
    std::size_t index;
};

static constexpr std::size_t num_elements = 1000;

TEMPLATE_TEST_CASE("buffered pq supports basic operations", "[buffered_pq][basic]", std::less<>, std::greater<>) {
    std::vector<BufferedInfo> infos(num_elements);

    using pq_t = multiqueue::BufferedPQ<multiqueue::Heap<std::pair<int, int>, TestType>, 8, 8>;

    auto pq = pq_t{};

    std::vector<int> data(num_elements);
    std::iota(data.begin(), data.end(), 0);
    std::sort(data.begin(), data.end(), TestType{});

    SECTION("push sorted numbers and pop them") {
        for (auto n : data) {
            pq.push({n, n}, infos.data());
        }

        for (auto n : data) {
            REQUIRE(pq.top().first == n);
            pq.pop(infos.data());
        }
        REQUIRE(pq.empty());
    }

    SECTION("push reverse sorted numbers and pop them") {
        for (auto it = data.rbegin(); it != data.rend(); ++it) {
            pq.push({*it, *it}, infos.data());
        }

        for (auto n : data) {
            REQUIRE(pq.top().first == n);
            pq.pop(infos.data());
        }
        REQUIRE(pq.empty());
    }

    SECTION(
        "first push sorted numbers, then push reverse sorted numbers and "
        "pop them") {
        for (auto it = data.begin(); it != data.begin() + data.size() / 2; ++it) {
            pq.push({*it, *it}, infos.data());
        }
        for (auto it = data.rbegin(); it != data.rbegin() + data.size() / 2; ++it) {
            pq.push({*it, *it}, infos.data());
        }
        for (auto n : data) {
            REQUIRE(pq.top().first == n);
            pq.pop(infos.data());
        }
        REQUIRE(pq.empty());
    }
}

TEST_CASE("buffered pq supports updating", "[buffered_pq][update]") {
    std::vector<BufferedInfo> infos(num_elements);

    using pq_t = multiqueue::BufferedPQ<multiqueue::Heap<std::pair<int, int>, std::less<>>, 8, 8>;

    auto pq = pq_t{};

    SECTION("push increasing numbers reverse order") {
        for (int n = 0; n < num_elements; ++n) {
            pq.push({n, n}, infos.data());
        }

        for (int n = 0; n < num_elements; ++n) {
            pq.update({num_elements - n - 1, n}, infos.data());
        }

        for (int i = 0; i < num_elements; ++i) {
            REQUIRE(pq.top().first == i);
            REQUIRE(pq.top().second == num_elements - i - 1);
            pq.pop(infos.data());
        }
        REQUIRE(pq.empty());
    }

    SECTION("push decreasing numbers and reverse order") {
        for (int n = num_elements - 1; n >= 0; --n) {
            pq.push({n, n}, infos.data());
        }

        for (int n = 0; n < num_elements; ++n) {
            pq.update({num_elements - n - 1, n}, infos.data());
        }

        for (int i = 0; i < num_elements; ++i) {
            REQUIRE(pq.top().first == i);
            REQUIRE(pq.top().second == num_elements - i - 1);
            pq.pop(infos.data());
        }
        REQUIRE(pq.empty());
    }

    SECTION(
        "first push and update increasing numbers, then push and update decreasing numbers and "
        "pop them") {
        for (int i = 0; i < num_elements / 2; ++i) {
            pq.push({i, i}, infos.data());
        }
        for (int i = 0; i < num_elements / 2; ++i) {
            pq.update({num_elements / 2 - i - 1, i}, infos.data());
        }
        for (int i = num_elements - 1; i >= num_elements / 2; --i) {
            pq.push({i, i}, infos.data());
        }
        for (int i = num_elements - 1; i >= num_elements / 2; --i) {
            pq.update({num_elements - i - 1 + num_elements / 2, i}, infos.data());
        }
        for (int i = 0; i < num_elements / 2; ++i) {
            REQUIRE(pq.top().first == i);
            REQUIRE(pq.top().second == num_elements / 2 - i - 1);
            pq.pop(infos.data());
        }
        for (int i = num_elements / 2; i < num_elements; ++i) {
            REQUIRE(pq.top().first == i);
            REQUIRE(pq.top().second == num_elements - i - 1 + num_elements / 2);
            pq.pop(infos.data());
        }
        REQUIRE(pq.empty());
    }
}

TEST_CASE("buffered pq works with randomized workloads", "[buffered_pq][workloads]") {
    std::vector<BufferedInfo> infos(num_elements);
    using pq_t = multiqueue::BufferedPQ<multiqueue::Heap<std::pair<int, int>, std::less<>>, 8, 8>;

    auto pq = pq_t{};
    auto ref_pq = std::priority_queue<int, std::vector<int>, std::greater<int>>{};
    auto gen = std::mt19937{0};

    SECTION("push random numbers and pop them") {
        auto dist = std::uniform_int_distribution{-num_elements / 10, num_elements / 10};

        for (std::size_t i = 0; i < num_elements; ++i) {
            auto n = dist(gen);
            pq.push({n, i}, infos.data());
            ref_pq.push(n);
            REQUIRE(pq.top().first == ref_pq.top());
        }

        for (std::size_t i = 0; i < num_elements; ++i) {
            REQUIRE(pq.top().first == ref_pq.top());
            pq.pop(infos.data());
            ref_pq.pop();
        }
        REQUIRE(pq.empty());
    }

    SECTION("interleave pushing and popping random numbers") {
        auto dist = std::uniform_int_distribution{-num_elements / 10, num_elements / 10};
        auto seq_dist = std::uniform_int_distribution{0, 10};

        for (int s = 0; s < num_elements / 10; ++s) {
            auto num_push = seq_dist(gen);
            for (int i = 0; i < num_push; ++i) {
                auto n = dist(gen);
                pq.push({n, s * 10 + i}, infos.data());
                ref_pq.push(n);
                REQUIRE(pq.top().first == ref_pq.top());
            }
            auto num_pop = seq_dist(gen);
            for (int i = 0; i > num_pop; --i) {
                if (!pq.empty()) {
                    REQUIRE(pq.top().first == ref_pq.top());
                    pq.pop(infos.data());
                    ref_pq.pop();
                }
            }
        }
        while (!pq.empty()) {
            REQUIRE(pq.top().first == ref_pq.top());
            pq.pop(infos.data());
            ref_pq.pop();
        }
    }

    SECTION("dijkstra") {
        auto dist = std::uniform_int_distribution{-num_elements / 10, num_elements / 10};
        auto seq_dist = std::uniform_int_distribution{1, 10};

        pq.push({0, 0}, infos.data());
        ref_pq.push(0);
        for (int s = 0; s < num_elements / 10; ++s) {
            auto top = pq.top();
            pq.pop(infos.data());
            ref_pq.pop();
            auto num_push = seq_dist(gen);
            for (int i = 0; i < num_push; ++i) {
                top.first += dist(gen);
                top.second = s * 10 + i;
                pq.push(top, infos.data());
                ref_pq.push(top.first);
                REQUIRE(pq.top().first == ref_pq.top());
            }
        }
        while (!pq.empty()) {
            REQUIRE(pq.top().first == ref_pq.top());
            pq.pop(infos.data());
            ref_pq.pop();
        }
    }
}

/* TEST_CASE("pq works with non-default-constructible types", "[pq][types]") { */
/*     using pq_t = multiqueue::BufferedPQ<multiqueue::Heap<std::pair<test_types::nodefault, int>, std::less<>>, 8, 8>;
 */
/*     pq_t pq{}; */
/*     pq.push({test_types::nodefault(0), 1}, infos.data()); */
/*     test_types::nodefault t1(2); */
/*     std::pair<test_types::nodefault, int> tp(t1, 2); */
/*     pq.push(tp, infos.data()); */
/*     std::pair<test_types::nodefault, int> t2{2, 3}; */
/*     t2 = pq.top(); */
/*     pq.pop(infos.data()); */
/*     pq.pop(infos.data()); */
/* } */
