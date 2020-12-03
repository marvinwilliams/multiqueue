#include "catch2/catch.hpp"
#include "multiqueue/kv_pq.hpp"

#include <algorithm>  // std::generate_n, std::min_element
#include <functional> // std::greater
#include <iterator>   // std::back_inserter
#include <queue>
#include <random>
#include <vector>

TEMPLATE_TEST_CASE_SIG("kv_pq heap representation can have different degrees",
                       "[kv_pq][correctness][degree]",
                       ((unsigned int Degree), Degree), 1, 2, 3, 4, 99) {
  using pq_t = multiqueue::local_nonaddressable::kv_pq<int, int, std::less<int>,
                                                    std::vector<std::pair<int, int>>, Degree>;

  INFO("Degree " << Degree);
  auto pq = pq_t{};

  SECTION("push increasing numbers and pop them") {
    for (int n = 0; n < 1000; ++n) {
      pq.push({n, 0});
      REQUIRE(pq.top().first == 0);
    }

    for (int i = 0; i < 1000; ++i) {
      REQUIRE(pq.top().first == i);
      pq.pop();
    }
    REQUIRE(pq.empty());
  }

  SECTION("push decreasing numbers and pop them") {
    for (int n = 999; n >= 0; --n) {
      pq.push({n, 0});
      REQUIRE(pq.top().first == n);
    }

    for (int i = 0; i < 1000; ++i) {
      REQUIRE(pq.top().first == i);
      pq.pop();
    }
    REQUIRE(pq.empty());
  }

  SECTION("first push increasing numbers, then push decreasing numbers and "
          "pop them") {
    for (int i = 1; i <= 500; ++i) {
      pq.push({i, 0});
    }
    for (int i = 1000; i > 500; --i) {
      pq.push({i, 0});
    }
    for (int i = 1; i <= 1000; ++i) {
      REQUIRE(pq.top().first == i);
      pq.pop();
    }
    REQUIRE(pq.empty());
  }
}

TEST_CASE("kv_pq use std::greater as comparator",
          "[kv_pq][correctness][comparator]") {
  using pq_t = multiqueue::local_nonaddressable::kv_pq<int, int, std::greater<int>,
                                                    std::vector<std::pair<int, int>>, 4>;

  auto pq = pq_t{};

  SECTION("push increasing numbers and pop them") {
    for (int n = 0; n < 1000; ++n) {
      pq.push({n, 0});
      REQUIRE(pq.top().first == n);
    }

    for (int i = 999; i >= 0; --i) {
      REQUIRE(pq.top().first == i);
      pq.pop();
    }
    REQUIRE(pq.empty());
  }

  SECTION("push decreasing numbers and pop them") {
    for (int n = 999; n >= 0; --n) {
      pq.push({n, 0});
      REQUIRE(pq.top().first == 999);
    }

    for (int i = 999; i >= 0; --i) {
      REQUIRE(pq.top().first == i);
      pq.pop();
    }
    REQUIRE(pq.empty());
  }

  SECTION("first push increasing numbers, then push decreasing numbers and "
          "pop them") {
    for (int i = 0; i < 500; ++i) {
      pq.push({i, 0});
    }
    for (int i = 999; i >= 500; --i) {
      pq.push({i, 0});
    }
    for (int i = 999; i >= 0; --i) {
      REQUIRE(pq.top().first == i);
      pq.pop();
    }
    REQUIRE(pq.empty());
  }
}

TEST_CASE("kv_pq test sample workloads", "[kv_pq][correctness][workloads]") {
  using pq_t = multiqueue::local_nonaddressable::kv_pq<int, int, std::less<int>,
                                                    std::vector<std::pair<int, int>>, 4>;

  auto pq = pq_t{};
  auto ref_pq = std::priority_queue<int, std::vector<int>, std::greater<int>>{};
  auto gen = std::mt19937{0};

  SECTION("push random numbers and pop them") {
    auto dist = std::uniform_int_distribution{-100, 100};

    for (size_t i = 0; i < 1000; ++i) {
      auto n = dist(gen);
      pq.push({n, 0});
      ref_pq.push(n);
      REQUIRE(pq.top().first == ref_pq.top());
    }

    for (size_t i = 0; i < 1000; ++i) {
      REQUIRE(pq.top().first == ref_pq.top());
      pq.pop();
      ref_pq.pop();
    }
    REQUIRE(pq.empty());
  }

  SECTION("interleave pushing and popping random numbers") {
    auto dist = std::uniform_int_distribution{-100, 100};
    auto seq_dist = std::uniform_int_distribution{0, 10};

    for (int s = 0; s < 1000; ++s) {
      auto num_push = seq_dist(gen);
      for (int i = 0; i < num_push; ++i) {
        auto n = dist(gen);
        pq.push({n, 0});
        ref_pq.push(n);
        REQUIRE(pq.top().first == ref_pq.top());
      }
      auto num_pop = seq_dist(gen);
      for (int i = 0; i > num_pop; --i) {
        if (!pq.empty()) {
          REQUIRE(pq.top().first == ref_pq.top());
          pq.pop();
          ref_pq.pop();
        }
      }
    }
    while (!pq.empty()) {
      REQUIRE(pq.top().first == ref_pq.top());
      pq.pop();
      ref_pq.pop();
    }
  }

  SECTION("dijkstra") {
    auto dist = std::uniform_int_distribution{-100, 100};
    auto seq_dist = std::uniform_int_distribution{1, 10};

    pq.push({0, 0});
    ref_pq.push(0);
    for (int s = 0; s < 1000; ++s) {
      auto top= pq.top().first;
      pq.pop();
      ref_pq.pop();
      auto num_push = seq_dist(gen);
      for (int i = 0; i < num_push; ++i) {
        auto n = top + dist(gen);
        pq.push({n, s});
        ref_pq.push(n);
        REQUIRE(pq.top().first == ref_pq.top());
      }
    }
    while (!pq.empty()) {
      REQUIRE(pq.top().first == ref_pq.top());
      pq.pop();
      ref_pq.pop();
    }
  }
}
