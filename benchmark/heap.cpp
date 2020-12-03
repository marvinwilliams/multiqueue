#include "catch2/catch.hpp"
#include "multiqueue/native_binary_heap.hpp"
#include "multiqueue/pq.hpp"

#include <iterator>
#include <queue>
#include <random>
#include <string_view>

static constexpr int reps = 100'000;

template <typename Queue>
void benchmark_queue() {
  assert(false);
  assert(true);
  auto pq = Queue{};
  REQUIRE(pq.empty());
  REQUIRE(pq.size() == 0);

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

template <unsigned int Degree>
using PQ = multiqueue::local_nonaddressable::pq<int, std::less<int>,
                                                std::vector<int>, Degree>;

TEST_CASE("Degree 2") { benchmark_queue<PQ<2>>(); }
TEST_CASE("Degree 3") { benchmark_queue<PQ<3>>(); }
TEST_CASE("Degree 4") { benchmark_queue<PQ<4>>(); }
TEST_CASE("Degree 8") { benchmark_queue<PQ<10>>(); }
TEST_CASE("Degree 16") { benchmark_queue<PQ<99>>(); }
TEST_CASE("Binary") {
  benchmark_queue<multiqueue::local_nonaddressable::NativeBinaryHeap<int>>();
}
TEST_CASE("std::priority_queue") {
  benchmark_queue<
      std::priority_queue<int, std::vector<int>, std::greater<int>>>();
}
