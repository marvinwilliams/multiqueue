include(Catch)

add_library(basic_test INTERFACE)
target_link_libraries(basic_test INTERFACE multiqueue_warnings)
target_compile_features(basic_test INTERFACE cxx_std_17)

add_library(catch_test INTERFACE)
target_link_libraries(catch_test INTERFACE basic_test)
target_link_libraries_system(catch_test INTERFACE Catch2::Catch2)

add_executable(unit_tests main.cpp pq_api.cpp pq_correctness.cpp kv_pq_api.cpp kv_pq_correctness.cpp no_buffer_pq.cpp)
target_link_libraries(unit_tests PRIVATE catch_test multiqueue::multiqueue Threads::Threads)

if(BUILD_TESTING)
  catch_discover_tests(unit_tests)
endif()

add_library(wrapper_test INTERFACE)
target_link_libraries(wrapper_test INTERFACE threading basic_test)
target_link_libraries_system(wrapper_test INTERFACE cxxopts)
target_sources(wrapper_test INTERFACE wrapper.cpp)

add_custom_target(all_wrapper_tests COMMENT "Build all wrapper tests")

function(add_wrapper_test name define)
  add_executable(${name}_wrapper_test)
  target_compile_definitions(${name}_wrapper_test PRIVATE ${define})
  target_link_libraries(${name}_wrapper_test PRIVATE ${name}_wrapper wrapper_test)
  add_dependencies(all_wrapper_tests ${name}_wrapper_test)
endfunction()

add_wrapper_test(capq PQ_CAPQ)
add_wrapper_test(klsm PQ_KLSM)
add_wrapper_test(dlsm PQ_DLSM)
add_wrapper_test(linden PQ_LINDEN)
add_wrapper_test(spraylist PQ_SPRAYLIST)


add_library(consistency_test INTERFACE)
target_link_libraries(consistency_test INTERFACE threading basic_test)
target_link_libraries_system(consistency_test INTERFACE cxxopts)
target_sources(consistency_test INTERFACE consistency.cpp)
target_compile_definitions(consistency_test INTERFACE "NDEBUG")
target_compile_options(consistency_test INTERFACE "-O3")
target_compile_options(consistency_test INTERFACE "-march=native")
if(CMAKE_COMPILER_IS_GNUCXX)
  target_link_options(consistency_test INTERFACE "-flto")
endif()

add_custom_target(all_consistency_tests COMMENT "Build all consistency tests")

function(add_wrapper_consistency_test name define)
  add_executable(${name}_consistency_test)
  target_compile_definitions(${name}_consistency_test PRIVATE ${define})
  target_link_libraries(${name}_consistency_test PRIVATE ${name}_wrapper consistency_test)
  add_dependencies(all_consistency_tests ${name}_consistency_test)
endfunction()

function(add_mq_consistency_test name define)
  add_executable(${name}_consistency_test)
  target_compile_definitions(${name}_consistency_test PRIVATE ${define})
  target_link_libraries(${name}_consistency_test PRIVATE multiqueue::multiqueue consistency_test)
  add_dependencies(all_consistency_tests ${name}_consistency_test)
endfunction()


add_wrapper_consistency_test(capq PQ_CAPQ)
add_wrapper_consistency_test(klsm PQ_KLSM)
add_wrapper_consistency_test(dlsm PQ_DLSM)
add_wrapper_consistency_test(linden PQ_LINDEN)
add_wrapper_consistency_test(spraylist PQ_SPRAYLIST)
add_mq_consistency_test(nbmq PQ_NBMQ)
add_mq_consistency_test(tbmq PQ_TBMQ)
add_mq_consistency_test(dbmq PQ_DBMQ)
add_mq_consistency_test(smdmq PQ_SMDMQ)

add_executable(verify_consistency verify_consistency.cpp)
target_link_libraries(verify_consistency PRIVATE basic_test)

add_custom_target(all_tests COMMENT "Build all tests")
add_dependencies(all_tests unit_tests all_wrapper_tests all_consistency_tests)
