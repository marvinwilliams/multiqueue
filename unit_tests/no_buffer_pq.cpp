#include "multiqueue/configurations.hpp"
#include "multiqueue/multiqueue.hpp"

#include "catch2/catch_test_macros.hpp"
#include "catch2/catch_template_test_macros.hpp"

#include <algorithm>   // std::generate_n, std::min_element
#include <functional>  // std::greater
#include <iterator>
#include <iterator>  // std::back_inserter
#include <limits>
#include <queue>
#include <random>
#include <thread>
#include <vector>

using multiqueue_t = multiqueue::multiqueue<int, int, std::less<int>, multiqueue::configuration::NoBuffering>;

static constexpr size_t elements_per_thread = 1000;

TEST_CASE("no_buffer single thread", "[no_buffer][workloads]") {
    auto pq = multiqueue_t{1};
    auto handle = pq.get_handle(0);

    SECTION("push increasing numbers and pop them") {
        for (size_t n = 0; n < elements_per_thread; ++n) {
            pq.push(handle, {static_cast<int>(n), static_cast<int>(elements_per_thread - n - 1)});
        }
        auto v = std::vector<int>();
        decltype(pq)::value_type top;
        for (size_t i = 0; i < elements_per_thread; ++i) {
            if (pq.extract_top(handle, top)) {
                REQUIRE(top.first == static_cast<int>(elements_per_thread) - top.second - 1);
                v.push_back(top.first);
            }
        }
        if (v.size() < elements_per_thread) {
            WARN("Not all elements popped: " << v.size() << '/' << elements_per_thread);
        }
        if (!v.empty()) {
            std::sort(v.begin(), v.end());
            REQUIRE(v.front() >= 0);
            REQUIRE(v.back() < static_cast<int>(elements_per_thread));
            REQUIRE(std::adjacent_find(v.begin(), v.end()) == v.end());
        }
    }

    SECTION("push decreasing numbers and pop them") {
        for (size_t n = 0; n < elements_per_thread; ++n) {
            pq.push(handle, {static_cast<int>(elements_per_thread - n - 1), static_cast<int>(n)});
        }
        auto v = std::vector<int>();
        decltype(pq)::value_type top;
        for (size_t i = 0; i < elements_per_thread; ++i) {
            if (pq.extract_top(handle, top)) {
                REQUIRE(top.first == static_cast<int>(elements_per_thread) - top.second - 1);
                v.push_back(top.first);
            }
        }
        if (v.size() < elements_per_thread) {
            WARN("Not all elements popped: " << v.size() << '/' << elements_per_thread);
        }
        if (!v.empty()) {
            std::sort(v.begin(), v.end());
            REQUIRE(v.front() >= 0);
            REQUIRE(v.back() < static_cast<int>(elements_per_thread));
            REQUIRE(std::adjacent_find(v.begin(), v.end()) == v.end());
        }
    }
}

TEMPLATE_TEST_CASE_SIG("no_buffer seq push multi pop", "[no_buffer][workloads]", ((unsigned int threads), threads), 2,
                       4, 8, 16) {
    static constexpr size_t num_elements = elements_per_thread * threads;
    auto pq = multiqueue_t{threads};

    SECTION("push increasing numbers and pop them") {
        for (size_t n = 0; n < num_elements; ++n) {
            pq.push(pq.get_handle(0), {static_cast<int>(n), static_cast<int>(num_elements - n - 1)});
        }
        auto t = std::vector<std::thread>();
        std::vector<std::pair<int, int>> v(num_elements);
        std::atomic_size_t count = 0;
        for (unsigned int id = 0; id < threads; ++id) {
            t.emplace_back([&, id]() {
                auto handle = pq.get_handle(id);
                decltype(pq)::value_type top;
                for (unsigned int i = 0; i < elements_per_thread; ++i) {
                    if (pq.extract_top(handle, top)) {
                        size_t pos = count.fetch_add(1, std::memory_order_relaxed);
                        v[pos] = top;
                    }
                }
            });
        }
        std::for_each(t.begin(), t.end(), [](auto &thread) { thread.join(); });
        if (count < num_elements) {
            WARN("Not all elements popped: " << count << '/' << num_elements);
        }
        if (count > 0) {
            for (auto it = v.begin(); it != v.begin() + static_cast<int>(count); ++it) {
                REQUIRE(it->first == static_cast<int>(num_elements) - it->second - 1);
            }
            std::sort(v.begin(), v.begin() + static_cast<int>(count));
            REQUIRE(v.front().first >= 0);
            REQUIRE(v[count - 1].first < static_cast<int>(num_elements));
            REQUIRE(std::adjacent_find(v.begin(), v.begin() + static_cast<int>(count)) ==
                    v.begin() + static_cast<int>(count));
        }
    }

    SECTION("push decreasing numbers and pop them") {
        for (size_t n = 0; n < num_elements; ++n) {
            pq.push(pq.get_handle(0), {static_cast<int>(num_elements - n - 1), static_cast<int>(n)});
        }
        auto t = std::vector<std::thread>();
        std::vector<std::pair<int, int>> v(num_elements);
        std::atomic_size_t count = 0;
        for (unsigned int id = 0; id < threads; ++id) {
            t.emplace_back([&, id]() {
                auto handle = pq.get_handle(id);
                decltype(pq)::value_type top;
                for (unsigned int i = 0; i < elements_per_thread; ++i) {
                    if (pq.extract_top(handle, top)) {
                        size_t pos = count.fetch_add(1, std::memory_order_relaxed);
                        v[pos] = top;
                    }
                }
            });
        }
        std::for_each(t.begin(), t.end(), [](auto &thread) { thread.join(); });
        if (count < num_elements) {
            WARN("Not all elements popped: " << count << '/' << num_elements);
        }
        if (count > 0) {
            for (auto it = v.begin(); it != v.begin() + static_cast<int>(count); ++it) {
                REQUIRE(it->first == static_cast<int>(num_elements) - it->second - 1);
            }
            std::sort(v.begin(), v.begin() + static_cast<int>(count));
            REQUIRE(v.front().first >= 0);
            REQUIRE(v[count - 1].first < static_cast<int>(num_elements));
            REQUIRE(std::adjacent_find(v.begin(), v.begin() + static_cast<int>(count)) ==
                    v.begin() + static_cast<int>(count));
        }
    }
}

TEMPLATE_TEST_CASE_SIG("no_buffer multi push seq pop", "[no_buffer][workloads]", ((unsigned int threads), threads), 2,
                       4, 8, 16) {
    static constexpr size_t num_elements = elements_per_thread * threads;
    auto pq = multiqueue_t{threads};

    SECTION("push increasing numbers and pop them") {
        auto t = std::vector<std::thread>();
        for (unsigned int id = 0; id < threads; ++id) {
            t.emplace_back([&, id]() {
                for (unsigned int i = 0; i < elements_per_thread; ++i) {
                    auto handle = pq.get_handle(id);
                    pq.push(handle,
                            {static_cast<int>(id * elements_per_thread + i),
                             num_elements - (id * elements_per_thread + i) - 1});
                }
            });
        }
        std::for_each(t.begin(), t.end(), [](auto &thread) { thread.join(); });
        t.clear();
        std::vector<std::pair<int, int>> v(num_elements);
        std::atomic_size_t count = 0;
        for (unsigned int id = 0; id < threads; ++id) {
            t.emplace_back([&, id]() {
                auto handle = pq.get_handle(id);
                decltype(pq)::value_type top;
                for (unsigned int i = 0; i < elements_per_thread; ++i) {
                    if (pq.extract_top(handle, top)) {
                        size_t pos = count.fetch_add(1, std::memory_order_relaxed);
                        v[pos] = top;
                    }
                }
            });
        }
        std::for_each(t.begin(), t.end(), [](auto &thread) { thread.join(); });
        if (count < num_elements) {
            WARN("Not all elements popped: " << count << '/' << num_elements);
        }
        if (count > 0) {
            for (auto it = v.begin(); it != v.begin() + static_cast<int>(count); ++it) {
                REQUIRE(it->first == static_cast<int>(num_elements) - it->second - 1);
            }
            std::sort(v.begin(), v.begin() + static_cast<int>(count));
            REQUIRE(v.front().first >= 0);
            REQUIRE(v[count - 1].first < static_cast<int>(num_elements));
            REQUIRE(std::adjacent_find(v.begin(), v.begin() + static_cast<int>(count)) ==
                    v.begin() + static_cast<int>(count));
        }
    }

    SECTION("push decreasing numbers and pop them") {
        auto t = std::vector<std::thread>();
        for (unsigned int id = 0; id < threads; ++id) {
            t.emplace_back([&, id]() {
                for (size_t i = 0; i < elements_per_thread; ++i) {
                auto handle = pq.get_handle(id);
                    pq.push(handle,
                            {static_cast<int>((id + 1) * elements_per_thread - i - 1),
                             static_cast<int>(num_elements - ((id + 1) * elements_per_thread - i))});
                }
            });
        }
        std::for_each(t.begin(), t.end(), [](auto &thread) { thread.join(); });
        t.clear();
        std::vector<std::pair<int, int>> v(num_elements);
        std::atomic_size_t count = 0;
        for (unsigned int id = 0; id < threads; ++id) {
            t.emplace_back([&, id]() {
                auto handle = pq.get_handle(id);
                decltype(pq)::value_type top;
                for (unsigned int i = 0; i < elements_per_thread; ++i) {
                    if (pq.extract_top(handle, top)) {
                        size_t pos = count.fetch_add(1, std::memory_order_relaxed);
                        v[pos] = top;
                    }
                }
            });
        }
        std::for_each(t.begin(), t.end(), [](auto &thread) { thread.join(); });
        if (count < num_elements) {
            WARN("Not all elements popped: " << count << '/' << num_elements);
        }
        if (count > 0) {
            for (auto it = v.begin(); it != v.begin() + static_cast<int>(count); ++it) {
                REQUIRE(it->first == static_cast<int>(num_elements) - it->second - 1);
            }
            std::sort(v.begin(), v.begin() + static_cast<int>(count));
            REQUIRE(v.front().first >= 0);
            REQUIRE(v[count - 1].first < static_cast<int>(num_elements));
            REQUIRE(std::adjacent_find(v.begin(), v.begin() + static_cast<int>(count)) ==
                    v.begin() + static_cast<int>(count));
        }
    }
}
