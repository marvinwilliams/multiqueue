add_library(basic_test INTERFACE)
target_link_libraries(basic_test INTERFACE multiqueue multiqueue_internal)
target_compile_features(basic_test INTERFACE cxx_std_17)
target_include_directories(basic_test INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")

add_subdirectory(unit_tests)

add_custom_target(all_tests COMMENT "Build all tests")
add_dependencies(all_tests unit_tests)
