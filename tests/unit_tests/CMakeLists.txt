include(Catch)

add_executable(unit_tests main.cpp pq_api.cpp pq_correctness.cpp kv_pq_api.cpp kv_pq_correctness.cpp no_buffer_pq.cpp ring_buffer.cpp buffer.cpp merge_heap.cpp)
target_link_libraries(unit_tests PRIVATE basic_test Threads::Threads)
target_link_libraries_system(unit_tests PRIVATE Catch2::Catch2)

if(BUILD_TESTING)
  catch_discover_tests(unit_tests)
endif()

