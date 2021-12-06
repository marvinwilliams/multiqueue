#include "multiqueue/buffer.hpp"
#include "multiqueue/ring_buffer.hpp"
#include "test_types.hpp"

#include "catch2/catch_template_test_macros.hpp"
#include "catch2/catch_test_macros.hpp"

#include <algorithm>
#include <string>
#include <utility>

template <typename T>
using buffer_wrapper_t = multiqueue::Buffer<T, 4>;

template <typename T>
using ring_buffer_wrapper_t = multiqueue::RingBuffer<T, 4>;

TEST_CASE("buffer supports basic operations", "[buffer][basic]") {
    multiqueue::Buffer<unsigned int, 16> buffer;
    REQUIRE(buffer.empty());
    REQUIRE(buffer.capacity() == 16);

    for (unsigned int i = 0; i < buffer.capacity(); ++i) {
        REQUIRE(!buffer.full());
        REQUIRE(buffer.size() == i);
        buffer.push_back(i);
    }

    CHECK(buffer.full());
    CHECK(buffer.size() == buffer.capacity());

    SECTION("bracket") {
        for (unsigned int i = 0; i < buffer.capacity(); ++i) {
            REQUIRE(buffer[i] == i);
        }

        CHECK(buffer.back() == buffer.capacity() - 1);
        CHECK(buffer.front() == 0);

        for (unsigned int i = 0; i < buffer.capacity(); ++i) {
            buffer[i] = buffer.capacity() - i - 1;
        }
        for (unsigned int i = 0; i < buffer.capacity(); ++i) {
            REQUIRE(buffer[i] == buffer.capacity() - i - 1);
        }
        buffer.clear();
        CHECK(buffer.empty());
    }

    SECTION("pop_back") {
        buffer.pop_back();
        buffer.pop_back();
        buffer.pop_back();
        CHECK(buffer.back() == buffer.capacity() - 4);
        buffer.push_back(buffer.back() + 1);
        buffer.push_back(buffer.back() + 1);
        buffer.push_back(buffer.back() + 1);
        for (unsigned int i = buffer.capacity(); i-- > 0;) {
            REQUIRE(buffer.back() == i);
            buffer.pop_back();
            REQUIRE(buffer.size() == i);
        }

        CHECK(buffer.empty());
        CHECK(buffer.size() == 0);
    }
}

TEST_CASE("ring buffer supports basic operations", "[ring_buffer][basic]") {
    multiqueue::RingBuffer<unsigned int, 16> buffer;
    REQUIRE(buffer.empty());
    REQUIRE(buffer.capacity() == 16);

    for (unsigned int i = 0; i < buffer.capacity(); ++i) {
        REQUIRE(!buffer.full());
        REQUIRE(buffer.size() == i);
        buffer.push_back(i);
    }

    CHECK(buffer.full());
    CHECK(buffer.size() == buffer.capacity());

    SECTION("bracket") {
        for (unsigned int i = 0; i < buffer.capacity(); ++i) {
            REQUIRE(buffer[i] == i);
        }

        CHECK(buffer.back() == buffer.capacity() - 1);
        CHECK(buffer.front() == 0);

        for (unsigned int i = 0; i < buffer.capacity(); ++i) {
            buffer[i] = buffer.capacity() - i - 1;
        }
        for (unsigned int i = 0; i < buffer.capacity(); ++i) {
            REQUIRE(buffer[i] == buffer.capacity() - i - 1);
        }
        buffer.clear();
        CHECK(buffer.empty());
    }

    SECTION("pop_back/pop_front") {
        buffer.pop_back();
        buffer.pop_front();
        buffer.pop_back();
        REQUIRE(buffer.back() == buffer.capacity() - 3);
        buffer.push_back(buffer.back() + 1);
        buffer.push_front(0);
        buffer.push_back(buffer.back() + 1);
        for (unsigned int i = buffer.capacity(); i-- > buffer.capacity() / 2;) {
            REQUIRE(buffer.back() == i);
            buffer.pop_back();
            buffer.pop_front();
        }
        REQUIRE(buffer.empty());
        while (!buffer.full()) {
            buffer.push_back(static_cast<unsigned int>(buffer.size()));
        }
        for (unsigned int i = 0; i != buffer.capacity(); ++i) {
            REQUIRE(buffer.front() == i);
            buffer.pop_front();
        }

        REQUIRE(buffer.empty());
        REQUIRE(buffer.size() == 0);
    }
}

TEMPLATE_PRODUCT_TEST_CASE("buffer supports different types", "[types][buffer]",
                           (buffer_wrapper_t, ring_buffer_wrapper_t), (int, std::string, (std::pair<int, double>))) {
    using value_t = typename TestType::value_type;
    TestType buffer;
    buffer.push_back(value_t{});
    value_t t1{};
    buffer[0] = t1;
    [[maybe_unused]] value_t t2 = buffer[0];
    buffer.pop_back();
}

TEMPLATE_TEST_CASE("buffer works with non-copyable types", "[buffer][types]",
                   (multiqueue::Buffer<test_types::nocopy, 4>), (multiqueue::RingBuffer<test_types::nocopy, 4>)) {
    TestType buffer;
    buffer.push_back(test_types::nocopy());
    test_types::nocopy t1;
    buffer.push_back(std::move(t1));
    test_types::nocopy t2;
    buffer[0] = std::move(t2);
    [[maybe_unused]] test_types::nocopy t3 = std::move(buffer[0]);
    t2 = std::move(buffer[1]);
    buffer.pop_back();
    buffer.pop_back();
}

TEMPLATE_TEST_CASE("buffer works with non-default-constructible types", "[buffer][types]",
                   (multiqueue::Buffer<test_types::nodefault, 4>), (multiqueue::RingBuffer<test_types::nodefault, 4>)) {
    TestType buffer;
    buffer.push_back(test_types::nodefault(0));
    test_types::nodefault t1{1};
    buffer.push_back(t1);
    test_types::nodefault t{2};
    buffer[0] = t;
    buffer.pop_back();
    buffer.pop_back();
}

TEMPLATE_TEST_CASE("buffer destructs", "[buffer][types]", (multiqueue::Buffer<test_types::countingdtor, 4>),
                   (multiqueue::RingBuffer<test_types::countingdtor, 4>)) {
    test_types::countingdtor::count = 0;
    {
        TestType buffer;
        test_types::countingdtor t1;
        buffer.push_back(t1);
        buffer.push_back(t1);
        buffer.push_back(t1);
        test_types::countingdtor t2;
        buffer[0] = t2;
        buffer.pop_back();  // + 1
        buffer.pop_back();  // + 1
        // destruct t1, t2 // + 2
        // destruct buffer // + 1
    }
    REQUIRE(test_types::countingdtor::count == 5);
}

TEMPLATE_TEST_CASE("buffer iterator tests", "[buffer][iterator]", (multiqueue::Buffer<int, 4>),
                   (multiqueue::RingBuffer<int, 16>)) {
    TestType buffer;
    buffer.insert(buffer.begin(), 1);
    buffer.insert(buffer.end(), 2);
    buffer.insert(buffer.cbegin(), 0);
    REQUIRE(buffer.size() == 3);
    REQUIRE(buffer[0] == 0);
    REQUIRE(buffer[1] == 1);
    REQUIRE(buffer[2] == 2);
    std::for_each(buffer.begin(), buffer.end(), [](auto& n) { ++n; });
    REQUIRE(buffer[0] == 1);
    REQUIRE(buffer[1] == 2);
    REQUIRE(buffer[2] == 3);
    for (auto& i : buffer) {
        --i;
    }
    REQUIRE(buffer[0] == 0);
    REQUIRE(buffer[1] == 1);
    REQUIRE(buffer[2] == 2);
    auto it = buffer.insert(buffer.cbegin() + 1, 5);
    REQUIRE(*it == 5);
    buffer.erase(it);
    buffer.erase(buffer.end() - 1);
    REQUIRE(buffer.size() == 2);
    REQUIRE(buffer[0] == 0);
}
