include(Catch)

add_executable(unit_tests no_buffer_pq.cpp ring_buffer.cpp buffer.cpp merge_heap.cpp)
target_link_libraries(unit_tests PRIVATE multiqueue_internal Threads::Threads Catch2::Catch2WithMain)
target_link_libraries_system(unit_tests PRIVATE Catch2::Catch2)

if(BUILD_TESTING)
  catch_discover_tests(unit_tests)
endif()
