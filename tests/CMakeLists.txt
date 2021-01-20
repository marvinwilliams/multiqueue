include(Catch)

add_library(test_target INTERFACE)
target_link_libraries(test_target INTERFACE Catch2::Catch2)
target_link_libraries(test_target INTERFACE multiqueue::multiqueue)
target_link_libraries(test_target INTERFACE multiqueue_warnings)
target_compile_options(test_target INTERFACE "-g")
target_compile_options(test_target INTERFACE "-Og")

add_executable(tests main.cpp pq_api.cpp pq_correctness.cpp kv_pq_api.cpp kv_pq_correctness.cpp)
target_link_libraries(tests PRIVATE test_target)

if(BUILD_TESTING)
  catch_discover_tests(tests)
endif()
