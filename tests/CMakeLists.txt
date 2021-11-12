include(Catch)

add_executable(tests heap.cpp buffer.cpp ring_buffer.cpp buffered_pq.cpp multiqueue.cpp)
target_link_libraries(tests PRIVATE multiqueue_internal Threads::Threads Catch2::Catch2WithMain)

if(BUILD_TESTING)
  catch_discover_tests(tests)
endif()
