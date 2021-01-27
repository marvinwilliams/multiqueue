#include "catch2/catch.hpp"
#include "multiqueue/pq.hpp"
#include "multiqueue/relaxed_kv_multiqueue.hpp"
#include "multiqueue/relaxed_multiqueue.hpp"

#include <algorithm>   // std::generate_n, std::min_element
#include <functional>  // std::greater
#include <iterator>
#include <iterator>  // std::back_inserter
#include <limits>
#include <queue>
#include <random>
#include <thread>
#include <vector>

namespace multiqueue {
namespace rsm {

template <>
struct Sentinel<int> {
    static constexpr int get() noexcept {
        return std::numeric_limits<int>::max();
    }
    static constexpr bool is_sentinel(int const &i) noexcept {
        return i == std::numeric_limits<int>::max();
    }
};

}  // namespace rsm
}  // namespace multiqueue

using multiqueue_t = multiqueue::rsm::priority_queue<int>;
using kv_multiqueue_t = multiqueue::rsm::kv_priority_queue<int, int>;

static constexpr size_t elements_per_thread = 10000u;

TEST_CASE("kv_multiqueue single thread", "[kv_multiqueue][workloads]") {
    auto pq = kv_multiqueue_t{1u};

    SECTION("push increasing numbers and pop them") {
        for (size_t n = 0u; n < elements_per_thread; ++n) {
            pq.push({static_cast<int>(n), static_cast<int>(n)});
        }
        auto v = std::vector<int>();
        for (size_t i = 0u; i < elements_per_thread; ++i) {
            auto top = pq.extract_top();
            if (top.first != std::numeric_limits<int>::max()) {
                REQUIRE(top.first == top.second);
                v.push_back(top.first);
            }
        }
        if (v.size() < elements_per_thread) {
            WARN("Not all elements popped: " << v.size() << '/' << elements_per_thread);
        }
        std::sort(v.begin(), v.end());
        REQUIRE(v.front() >= 0);
        REQUIRE(v.back() < static_cast<int>(elements_per_thread));
        REQUIRE(std::adjacent_find(v.begin(), v.end()) == v.end());
    }

    SECTION("push decreasing numbers and pop them") {
        for (size_t n = 0u; n < elements_per_thread; ++n) {
            pq.push({static_cast<int>(elements_per_thread - n - 1), static_cast<int>(n)});
        }
        auto v = std::vector<int>();
        for (size_t i = 0u; i < elements_per_thread; ++i) {
            auto top = pq.extract_top();
            if (top.first != std::numeric_limits<int>::max()) {
                REQUIRE(top.first == static_cast<int>(elements_per_thread) - top.second - 1);
                v.push_back(top.first);
            }
        }
        if (v.size() < elements_per_thread) {
            WARN("Not all elements popped: " << v.size() << '/' << elements_per_thread);
        }
        std::sort(v.begin(), v.end());
        REQUIRE(v.front() >= 0);
        REQUIRE(v.back() < static_cast<int>(elements_per_thread));
        REQUIRE(std::adjacent_find(v.begin(), v.end()) == v.end());
    }
}

TEST_CASE("multiqueue single thread", "[multiqueue][workloads]") {
    auto pq = multiqueue_t{1u};

    SECTION("push increasing numbers and pop them") {
        for (size_t n = 0u; n < elements_per_thread; ++n) {
            pq.push(static_cast<int>(n));
        }
        auto v = std::vector<int>();
        for (size_t i = 0u; i < elements_per_thread; ++i) {
            int top = pq.extract_top();
            if (top != std::numeric_limits<int>::max()) {
                v.push_back(top);
            }
        }
        if (v.size() < elements_per_thread) {
            WARN("Not all elements popped: " << v.size() << '/' << elements_per_thread);
        }
        std::sort(v.begin(), v.end());
        REQUIRE(v.front() >= 0);
        REQUIRE(v.back() < static_cast<int>(elements_per_thread));
        REQUIRE(std::adjacent_find(v.begin(), v.end()) == v.end());
    }

    SECTION("push decreasing numbers and pop them") {
        for (size_t n = 0u; n < elements_per_thread; ++n) {
            pq.push(static_cast<int>(elements_per_thread - n - 1));
        }
        auto v = std::vector<int>();
        for (size_t i = 0u; i < elements_per_thread; ++i) {
            int top = pq.extract_top();
            if (top != std::numeric_limits<int>::max()) {
                v.push_back(top);
            }
        }
        if (v.size() < elements_per_thread) {
            WARN("Not all elements popped: " << v.size() << '/' << elements_per_thread);
        }
        std::sort(v.begin(), v.end());
        REQUIRE(v.front() >= 0);
        REQUIRE(v.back() < static_cast<int>(elements_per_thread));
        REQUIRE(std::adjacent_find(v.begin(), v.end()) == v.end());
    }
}

TEMPLATE_TEST_CASE_SIG("multiqueue seq push multi pop", "[multiqueue][workloads]", ((unsigned int threads), threads), 2,
                       4, 8, 16) {
    static constexpr size_t num_elements = elements_per_thread * threads;
    auto pq = multiqueue_t{threads};

    SECTION("push increasing numbers and pop them") {
        for (size_t n = 0u; n < num_elements; ++n) {
            pq.push(static_cast<int>(n));
        }
        auto t = std::vector<std::thread>();
        std::vector<int> v(num_elements, 0);
        std::atomic_size_t count = 0u;
        for (unsigned int id = 0u; id < threads; ++id) {
            t.emplace_back([&pq, &v, &count]() {
                for (unsigned int i = 0u; i < elements_per_thread; ++i) {
                    int top = pq.extract_top();
                    if (top != std::numeric_limits<int>::max()) {
                        size_t pos = count.fetch_add(1u, std::memory_order_relaxed);
                        v[pos] = top;
                    }
                }
            });
        }
        std::for_each(t.begin(), t.end(), [](auto &thread) { thread.join(); });
        if (count < num_elements) {
            WARN("Not all elements popped: " << count << '/' << num_elements);
        }
        std::sort(v.begin(), v.begin() + static_cast<int>(count));
        REQUIRE(v.front() >= 0);
        REQUIRE(v[count - 1u] < static_cast<int>(num_elements));
        REQUIRE(std::adjacent_find(v.begin(), v.begin() + static_cast<int>(count)) ==
                v.begin() + static_cast<int>(count));
    }

    SECTION("push decreasing numbers and pop them") {
        for (size_t n = 0u; n < num_elements; ++n) {
            pq.push(static_cast<int>(num_elements - n - 1));
        }
        auto t = std::vector<std::thread>();
        std::vector<int> v(num_elements, 0);
        std::atomic_size_t count = 0;
        for (unsigned int id = 0; id < threads; ++id) {
            t.emplace_back([&pq, &v, &count]() {
                for (unsigned int i = 0u; i < elements_per_thread; ++i) {
                    int top = pq.extract_top();
                    if (top != std::numeric_limits<int>::max()) {
                        size_t pos = count.fetch_add(1u, std::memory_order_relaxed);
                        v[pos] = top;
                    }
                }
            });
        }
        std::for_each(t.begin(), t.end(), [](auto &thread) { thread.join(); });
        if (count < num_elements) {
            WARN("Not all elements popped: " << count << '/' << num_elements);
        }
        std::sort(v.begin(), v.begin() + static_cast<int>(count));
        REQUIRE(v.front() >= 0);
        REQUIRE(v[count - 1u] < static_cast<int>(num_elements));
        REQUIRE(std::adjacent_find(v.begin(), v.begin() + static_cast<int>(count)) ==
                v.begin() + static_cast<int>(count));
    }
}

TEMPLATE_TEST_CASE_SIG("multiqueue multi push seq pop", "[multiqueue][workloads]", ((unsigned int threads), threads), 2,
                       4, 8, 16) {
    static constexpr size_t num_elements = elements_per_thread * threads;
    auto pq = multiqueue_t{threads};

    SECTION("push increasing numbers and pop them") {
        auto t = std::vector<std::thread>();
        for (unsigned int id = 0u; id < threads; ++id) {
            t.emplace_back([&pq, id]() {
                for (unsigned int i = 0u; i < elements_per_thread; ++i) {
                    pq.push(static_cast<int>(id * elements_per_thread + i));
                }
            });
        }
        std::for_each(t.begin(), t.end(), [](auto &thread) { thread.join(); });
        t.clear();
        std::vector<int> v(num_elements, 0);
        std::atomic_size_t count = 0;
        for (unsigned int id = 0; id < threads; ++id) {
            t.emplace_back([&pq, &v, &count]() {
                for (unsigned int i = 0; i < elements_per_thread; ++i) {
                    int top = pq.extract_top();
                    if (top != std::numeric_limits<int>::max()) {
                        size_t pos = count.fetch_add(1u, std::memory_order_relaxed);
                        v[pos] = top;
                    }
                }
            });
        }
        std::for_each(t.begin(), t.end(), [](auto &thread) { thread.join(); });
        if (count < num_elements) {
            WARN("Not all elements popped: " << count << '/' << num_elements);
        }
        std::sort(v.begin(), v.begin() + static_cast<int>(count));
        REQUIRE(v.front() >= 0);
        REQUIRE(v[count - 1u] < static_cast<int>(num_elements));
        REQUIRE(std::adjacent_find(v.begin(), v.begin() + static_cast<int>(count)) ==
                v.begin() + static_cast<int>(count));
    }

    SECTION("push decreasing numbers and pop them") {
        auto t = std::vector<std::thread>();
        for (unsigned int id = 0u; id < threads; ++id) {
            t.emplace_back([&pq, id]() {
                for (size_t i = 0; i < elements_per_thread; ++i) {
                    pq.push(static_cast<int>((id + 1) * elements_per_thread - i - 1));
                }
            });
        }
        std::for_each(t.begin(), t.end(), [](auto &thread) { thread.join(); });
        t.clear();
        std::vector<int> v(num_elements, 0);
        std::atomic_size_t count = 0;
        for (unsigned int id = 0; id < threads; ++id) {
            t.emplace_back([&pq, &v, &count]() {
                for (unsigned int i = 0; i < elements_per_thread; ++i) {
                    int top = pq.extract_top();
                    if (top != std::numeric_limits<int>::max()) {
                        size_t pos = count.fetch_add(1u, std::memory_order_relaxed);
                        v[pos] = top;
                    }
                }
            });
        }
        std::for_each(t.begin(), t.end(), [](auto &thread) { thread.join(); });
        if (count < num_elements) {
            WARN("Not all elements popped: " << count << '/' << num_elements);
        }
        std::sort(v.begin(), v.begin() + static_cast<int>(count));
        REQUIRE(v.front() >= 0);
        REQUIRE(v[count - 1u] < static_cast<int>(num_elements));
        REQUIRE(std::adjacent_find(v.begin(), v.begin() + static_cast<int>(count)) ==
                v.begin() + static_cast<int>(count));
    }
}
