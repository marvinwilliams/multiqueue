include(Catch)

add_library(basic_test INTERFACE)
target_link_libraries(basic_test INTERFACE multiqueue_warnings)
target_compile_features(basic_test INTERFACE cxx_std_17)

add_library(catch_test INTERFACE)
target_link_libraries(catch_test INTERFACE basic_test Catch2::Catch2)

add_executable(unit_tests main.cpp pq_api.cpp pq_correctness.cpp kv_pq_api.cpp kv_pq_correctness.cpp multiqueue.cpp)
target_link_libraries(unit_tests PRIVATE catch_test multiqueue::multiqueue)

if(BUILD_TESTING)
  catch_discover_tests(unit_tests)
endif()

add_library(wrapper_test INTERFACE)
target_link_libraries(wrapper_test INTERFACE threading)
target_link_libraries(wrapper_test INTERFACE basic_test)
target_link_libraries_system(wrapper_test INTERFACE cxxopts)

add_executable(capq_wrapper_test wrapper.cpp)
target_compile_definitions(capq_wrapper_test PRIVATE PQ_CAPQ1)
target_link_libraries(capq_wrapper_test PRIVATE capq_wrapper wrapper_test)

add_executable(klsm_wrapper_test wrapper.cpp)
target_compile_definitions(klsm_wrapper_test PRIVATE PQ_KLSM)
target_link_libraries(klsm_wrapper_test PRIVATE klsm_wrapper wrapper_test)

add_executable(dlsm_wrapper_test wrapper.cpp)
target_compile_definitions(dlsm_wrapper_test PRIVATE PQ_DLSM)
target_link_libraries(dlsm_wrapper_test PRIVATE klsm_wrapper wrapper_test)

add_executable(linden_wrapper_test wrapper.cpp)
target_compile_definitions(linden_wrapper_test PRIVATE PQ_LINDEN)
target_link_libraries(linden_wrapper_test PRIVATE linden_wrapper wrapper_test)

add_executable(spraylist_wrapper_test wrapper.cpp)
target_compile_definitions(spraylist_wrapper_test PRIVATE PQ_SPRAYLIST)
target_link_libraries(spraylist_wrapper_test PRIVATE spraylist_wrapper wrapper_test)

add_executable(verify verify.cpp)
target_link_libraries(verify PRIVATE basic_test)
