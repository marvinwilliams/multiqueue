include(Catch)

add_library(catch_test INTERFACE)
target_link_libraries(catch_test INTERFACE basic_test)
target_link_libraries_system(catch_test INTERFACE Catch2::Catch2)

add_executable(unit_tests main.cpp pq_api.cpp pq_correctness.cpp kv_pq_api.cpp kv_pq_correctness.cpp no_buffer_pq.cpp ring_buffer.cpp buffer.cpp merge_heap.cpp)
target_link_libraries(unit_tests PRIVATE catch_test multiqueue::multiqueue Threads::Threads)

if(BUILD_TESTING)
  catch_discover_tests(unit_tests)
endif()

