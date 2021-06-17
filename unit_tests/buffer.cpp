#include "catch2/catch_test_macros.hpp"
#include "multiqueue/util/buffer.hpp"


TEST_CASE("buffer", "[buffer]") {
  multiqueue::util::buffer<std::uint32_t, 16> buffer;
  REQUIRE(buffer.empty());
  REQUIRE(buffer.size() == 0u);
  buffer.push_back(1);
  buffer.push_back(3);
  buffer.push_back(2);
  REQUIRE(buffer[0] == 1);
  REQUIRE(buffer[1] == 3);
  REQUIRE(buffer[2] == 2);
  buffer.pop_back();
  buffer.push_back(4);
  REQUIRE(buffer[0] == 1);
  REQUIRE(buffer[1] == 3);
  REQUIRE(buffer[2] == 4);
  buffer.push_back(5);
  buffer.insert_at(0, 0);
  REQUIRE(buffer.size() == 5);
  REQUIRE(buffer[0] == 0);
  REQUIRE(buffer[1] == 1);
  REQUIRE(buffer[2] == 3);
  REQUIRE(buffer[3] == 4);
  REQUIRE(buffer[4] == 5);
  buffer.insert_at(5, 2);
  buffer.insert_at(2, 22);
  REQUIRE(buffer[0] == 0);
  REQUIRE(buffer[1] == 1);
  REQUIRE(buffer[2] == 22);
  REQUIRE(buffer[3] == 3);
  REQUIRE(buffer[4] == 4);
  REQUIRE(buffer[5] == 5);
  REQUIRE(buffer[6] == 2);
  buffer.pop_back();
  for (unsigned int i = 0; i < (16 - 6); ++i) {
    buffer.push_back(i);
  }
  REQUIRE(buffer.size() == 16);
  buffer.pop_back();
  REQUIRE(buffer.size() == 15);
  buffer.clear();
  REQUIRE(buffer.empty());
  REQUIRE(buffer.size() == 0);
}
