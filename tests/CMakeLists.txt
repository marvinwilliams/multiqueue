add_executable(heap_test heap.cpp)
target_link_libraries(heap_test PRIVATE multiqueue_internal Threads::Threads Catch2::Catch2WithMain)

add_executable(buffered_pq_test buffered_pq.cpp)
target_link_libraries(buffered_pq_test PRIVATE multiqueue_internal Threads::Threads Catch2::Catch2WithMain)

add_executable(multiqueue_test multiqueue.cpp)
target_link_libraries(multiqueue_test PRIVATE multiqueue_internal Threads::Threads Catch2::Catch2WithMain)

list(APPEND CMAKE_MODULE_PATH "${EXTERNAL_DIR}/Catch2/extras")
include(Catch)

if(BUILD_TESTING)
  catch_discover_tests(heap_test)
  catch_discover_tests(buffered_pq_test)
  catch_discover_tests(multiqueue_test)
endif()
