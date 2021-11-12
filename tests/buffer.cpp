#include "multiqueue/buffer.hpp"
#include "test_types.hpp"

#include "catch2/catch_template_test_macros.hpp"
#include "catch2/catch_test_macros.hpp"

#include <algorithm>

TEST_CASE("basic functionality", "[buffer]") {
    multiqueue::Buffer<unsigned int, 4> buffer;
    REQUIRE(buffer.empty());
    REQUIRE(buffer.capacity() == (1 << 4));

    for (unsigned int i = 0; i < buffer.capacity(); ++i) {
        REQUIRE(!buffer.full());
        REQUIRE(buffer.size() == i);
        buffer.push_back(i);
    }

    REQUIRE(buffer.full());
    REQUIRE(buffer.size() == buffer.capacity());

    SECTION("bracket") {
        for (unsigned int i = 0; i < buffer.capacity(); ++i) {
            REQUIRE(buffer[i] == i);
        }

        REQUIRE(buffer.back() == buffer.capacity() - 1);
        REQUIRE(buffer.front() == 0);

        for (unsigned int i = 0; i < buffer.capacity(); ++i) {
            buffer[i] = buffer.capacity() - i - 1;
        }
        for (unsigned int i = 0; i < buffer.capacity(); ++i) {
            REQUIRE(buffer[i] == buffer.capacity() - i - 1);
        }
        buffer.clear();
        REQUIRE(buffer.empty());
    }

    SECTION("pop_back") {
        buffer.pop_back();
        buffer.pop_back();
        buffer.pop_back();
        REQUIRE(buffer.back() == buffer.capacity() - 4);
        buffer.push_back(buffer.back() + 1);
        buffer.push_back(buffer.back() + 1);
        buffer.push_back(buffer.back() + 1);
        for (unsigned int i = buffer.capacity(); i-- > 0;) {
            REQUIRE(buffer.back() == i);
            buffer.pop_back();
            REQUIRE(buffer.size() == i);
        }

        REQUIRE(buffer.empty());
        REQUIRE(buffer.size() == 0);
    }
}

TEMPLATE_TEST_CASE("buffer works with different types", "[buffer]", int, std::string, (std::pair<int, double>)) {
    multiqueue::Buffer<TestType, 4> buffer;
    buffer.push_back(TestType());
    TestType t1{};
    buffer[0] = t1;
    [[maybe_unused]] TestType t2 = buffer[0];
    buffer.pop_back();
}

TEST_CASE("buffer works with non-copyable types", "[buffer]") {
    multiqueue::Buffer<test_types::nocopy, 2> buffer;
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

TEST_CASE("buffer works with non-default-constructible types", "[buffer]") {
    multiqueue::Buffer<test_types::nodefault, 2> buffer;
    buffer.push_back(test_types::nodefault(0));
    test_types::nodefault t1{1};
    buffer.push_back(t1);
    test_types::nodefault t{2};
    buffer[0] = t;
    buffer.pop_back();
    buffer.pop_back();
}

TEST_CASE("buffer destructs", "[buffer]") {
    int start = test_types::countingdtor::count;
    {
        multiqueue::Buffer<test_types::countingdtor, 2> buffer;
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
    REQUIRE(test_types::countingdtor::count - start == 5);
}

TEST_CASE("buffer iterator tests", "[buffer]") {
    multiqueue::Buffer<int, 4> buffer;
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
