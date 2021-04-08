#include "catch2/catch.hpp"
#include "multiqueue/util/ring_buffer.hpp"


TEST_CASE("ring_buffer", "[ring_buffer]") {
  multiqueue::util::ring_buffer<std::uint32_t, 16> buffer;
  REQUIRE(buffer.empty());
  REQUIRE(buffer.size() == 0u);
  buffer.push_front(1);
  buffer.push_front(3);
  buffer.push_back(2);
  REQUIRE(buffer[0] == 3);
  REQUIRE(buffer[1] == 1);
  REQUIRE(buffer[2] == 2);
  buffer.pop_front();
  buffer.push_back(3);
  REQUIRE(buffer[0] == 1);
  REQUIRE(buffer[1] == 2);
  REQUIRE(buffer[2] == 3);
  buffer.push_back(4);
  buffer.insert_at(0, 0);
  REQUIRE(buffer.size() == 5);
  REQUIRE(buffer[0] == 0);
  REQUIRE(buffer[1] == 1);
  REQUIRE(buffer[2] == 2);
  REQUIRE(buffer[3] == 3);
  REQUIRE(buffer[4] == 4);
  buffer.insert_at(5, 5);
  buffer.insert_at(2, 22);
  REQUIRE(buffer[0] == 0);
  REQUIRE(buffer[1] == 1);
  REQUIRE(buffer[2] == 22);
  REQUIRE(buffer[3] == 2);
  REQUIRE(buffer[4] == 3);
  REQUIRE(buffer[5] == 4);
  REQUIRE(buffer[6] == 5);
  buffer.pop_front();
  for (unsigned int i = 0; i < (16 - 6)/2; ++i) {
    buffer.push_back(i);
    buffer.push_front(16 - i);
  }
  REQUIRE(buffer.size() == 16);
  buffer.pop_back();
  REQUIRE(buffer.size() == 15);
  buffer.clear();
  REQUIRE(buffer.empty());
  REQUIRE(buffer.size() == 0);
}
