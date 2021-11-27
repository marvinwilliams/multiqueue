add_executable(tests heap.cpp buffer.cpp buffered_pq.cpp multiqueue.cpp)
target_link_libraries(tests PRIVATE multiqueue_internal Threads::Threads Catch2::Catch2WithMain)

list(APPEND CMAKE_MODULE_PATH "${EXTERNAL_DIR}/Catch2/extras")
include(Catch)

if(BUILD_TESTING)
  catch_discover_tests(tests)
endif()
