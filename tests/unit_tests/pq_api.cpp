#include "catch2/catch.hpp"
#include "multiqueue/sequential/pq.hpp"
#include "utils.hpp"

#include <array>
#include <functional>
#include <list>
#include <type_traits>
#include <utility>

using multiqueue::sequential::pq;

TEMPLATE_TEST_CASE("pq basic operations with different types", "[pq][basic]", int, std::string,
                   (std::pair<int, float>)) {
    SECTION("Construction") {
        auto q = pq<TestType>();
        REQUIRE(q.empty());
        REQUIRE(q.size() == 0);
        q.push(TestType());
        REQUIRE(!q.empty());
        REQUIRE(q.size() == 1);
        auto q2 = q;
        q2.push(TestType());
        REQUIRE(q.size() == 1);
        REQUIRE(q2.size() == 2);
        auto q3 = std::move(q2);
        q3.clear();
        REQUIRE(q3.empty());
        q.pop();
        REQUIRE(q.empty());
    }

    SECTION("Assignment") {
        auto q = pq<TestType>();
        auto q2 = q;
        q.push(TestType());
        q2 = q;
        q2.push(TestType());
        REQUIRE(q.size() == 1);
        REQUIRE(q2.size() == 2);
        auto q3 = pq<TestType>();
        q3.push(TestType());
        q3 = std::move(q2);
        REQUIRE(q3.size() == 2);
        q.pop();
        REQUIRE(q.empty());
    }

    SECTION("swap") {
        auto q = pq<TestType>();
        q.push(TestType());
        auto q2 = pq<TestType>();
        q2.push(TestType());
        q2.push(TestType());
        swap(q, q2);
        REQUIRE(q.size() == 2);
        REQUIRE(q2.size() == 1);
    }
}

TEST_CASE("pq iterators", "[pq][iterator]") {
    auto q = pq<int>();
    auto v = std::array<int, 100>{0};
    auto i = GENERATE(take(20, random(0, 99)));
    q.push(i);
    ++v[static_cast<size_t>(i)];
    REQUIRE(std::count(q.begin(), q.end(), i) == v[static_cast<size_t>(i)]);
}

TEST_CASE("pq move-only", "[pq][move-only]") {
    auto q = pq<no_default_no_copy_type>();
    q.push(no_default_no_copy_type{0});
    q.push(no_default_no_copy_type{1});
    {
        [[maybe_unused]] auto const &top = q.top();
        REQUIRE(top.i == 0);
    }
    q.pop();
    no_default_no_copy_type extracted{0};
    q.extract_top(extracted);
    REQUIRE(extracted.i == 1);
    q.push(no_default_no_copy_type{2});
    q.push(no_default_no_copy_type{3});
    auto q2 = std::move(q);
    REQUIRE(q2.top().i == 2);
}

// TODO test allocators
