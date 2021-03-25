#include "catch2/catch.hpp"
#include "multiqueue/sequential/kv_pq.hpp"
#include "utils.hpp"

#include <array>
#include <functional>
#include <list>
#include <type_traits>
#include <utility>

using multiqueue::sequential::kv_pq;

TEMPLATE_PRODUCT_TEST_CASE("kv_pq basic operations with different types", "[kv_pq][basic]", std::pair,
                           ((int, int), (int, std::string), (std::string, (std::pair<int, float>)))) {
    using Key = typename TestType::first_type;
    using Value = typename TestType::second_type;
    SECTION("Construction") {
        auto q = kv_pq<Key, Value>();
        REQUIRE(q.empty());
        REQUIRE(q.size() == 0u);
        q.push({});
        REQUIRE(!q.empty());
        REQUIRE(q.size() == 1u);
        auto q2 = q;
        q2.push({});
        REQUIRE(q.size() == 1u);
        REQUIRE(q2.size() == 2u);
        auto q3 = std::move(q2);
        q3.clear();
        REQUIRE(q3.empty());
        q.pop();
        REQUIRE(q.empty());
    }

    SECTION("Assignment") {
        auto q = kv_pq<Key, Value>();
        auto q2 = q;
        q.push({});
        q2 = q;
        q2.push({});
        REQUIRE(q.size() == 1u);
        REQUIRE(q2.size() == 2u);
        auto q3 = kv_pq<Key, Value>();
        q3.push({});
        q3 = std::move(q2);
        REQUIRE(q3.size() == 2u);
        q.pop();
        REQUIRE(q.empty());
    }

    SECTION("swap") {
        auto q = kv_pq<Key, Value>();
        q.push({});
        auto q2 = kv_pq<Key, Value>();
        q2.push({});
        q2.push({});
        swap(q, q2);
        REQUIRE(q.size() == 2u);
        REQUIRE(q2.size() == 1u);
    }
}

TEST_CASE("kv_pq iterators", "[kv_pq][iterator]") {
    auto q = kv_pq<int, int>();
    auto v = std::array<int, 100>{0};
    auto i = GENERATE(take(20, random(0, 99)));
    q.push(std::pair<int, int>({i, i}));
    ++v[static_cast<size_t>(i)];
    REQUIRE(std::count_if(q.begin(), q.end(), [i](auto const &p) { return p.first == i; }) ==
            v[static_cast<size_t>(i)]);
}

TEST_CASE("kv_pq emplace", "[kv_pq][emplace]") {
    auto q = kv_pq<int, std::string>();
    q.emplace(1, "one");
    q.emplace_key(2, "two");
    q.emplace_key(3, "three");
    auto t = q.top();
    REQUIRE(t.first == 1);
    REQUIRE(t.second == "one");
    q.pop();
    t = q.top();
    REQUIRE(t.first == 2);
    REQUIRE(t.second == "two");
    q.pop();
    t = q.top();
    REQUIRE(t.first == 3);
    REQUIRE(t.second == "three");
}

TEST_CASE("kv_pq move-only", "[kv_pq][move-only]") {
    auto q = kv_pq<no_default_no_copy_type, no_default_no_copy_type>();
    q.push({no_default_no_copy_type{0}, no_default_no_copy_type{0}});
    q.push({no_default_no_copy_type{1}, no_default_no_copy_type{1}});
    q.push({no_default_no_copy_type{2}, no_default_no_copy_type{2}});
    {
        // top is a dangling reference after a call to q.pop()
        [[maybe_unused]] auto const &top = q.top();
        REQUIRE(top.first.i == 0);
    }
    q.pop();
    // This is not the case with q.extract_top(), which moves from the queue
    decltype(q)::value_type extracted{0, 0};
    q.extract_top(extracted);
    q.pop();
    REQUIRE(extracted.first.i == 1);
    q.emplace(3, 3);
    auto q2 = std::move(q);
    REQUIRE(q2.top().first.i == 3);
}

// TODO test allocators
