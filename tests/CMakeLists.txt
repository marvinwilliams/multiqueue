add_library(basic_test INTERFACE)
target_link_libraries(basic_test INTERFACE multiqueue multiqueue_internal)
target_compile_features(basic_test INTERFACE cxx_std_17)
target_include_directories(basic_test INTERFACE "${CMAKE_CURRENT_LIST_DIR}")

add_library(performance INTERFACE)
target_link_libraries(performance INTERFACE threading basic_test)
target_link_libraries_system(performance INTERFACE cxxopts)
target_sources(performance INTERFACE performance.cpp)

add_executable(evaluate_quality evaluate_quality.cpp)
target_link_libraries(evaluate_quality PRIVATE multiqueue_internal)
target_link_libraries_system(evaluate_quality PRIVATE cxxopts tlx)
target_compile_features(
  evaluate_quality PRIVATE
  cxx_std_17
)

add_custom_target(all_performances COMMENT "Build the performance test for all priority queues")

function(add_wrapper_performance name define)
  set(target_name wrapper_${name}_performance)
  add_executable(${target_name})
  target_compile_definitions(${target_name} PRIVATE ${define})
  target_link_libraries(${target_name} PRIVATE ${name}_wrapper performance)
  add_dependencies(all_performances ${target_name})
if(BUILD_TESTING)
  add_test(NAME ${target_name}_small COMMAND /bin/bash -c "$<TARGET_FILE:${target_name}> -n 0 -j 1 -t 100 | $<TARGET_FILE:evaluate_quality> -v")
  add_test(NAME ${target_name}_large COMMAND /bin/bash -c "$<TARGET_FILE:${target_name}> -n 1000000 -j 8 -t 500 | $<TARGET_FILE:evaluate_quality> -v")
endif()
endfunction()

add_wrapper_performance(capq PQ_CAPQ)
add_wrapper_performance(klsm PQ_KLSM)
add_wrapper_performance(dlsm PQ_DLSM)
add_wrapper_performance(linden PQ_LINDEN)
add_wrapper_performance(spraylist PQ_SPRAYLIST)

function(add_mq_performance name define)
  set(target_name ${name}_performance)
  add_executable(${target_name})
  target_compile_definitions(${target_name} PRIVATE ${define})
  target_link_libraries(${target_name} PRIVATE multiqueue::multiqueue performance)
  add_dependencies(all_performances ${target_name})
if(BUILD_TESTING)
  add_test(NAME ${target_name}_small COMMAND /bin/bash -c "$<TARGET_FILE:${target_name}> -n 0 -j 1 -t 100 | $<TARGET_FILE:evaluate_quality> -v")
  add_test(NAME ${target_name}_large COMMAND /bin/bash -c "$<TARGET_FILE:${target_name}> -n 1000000 -j 8 -t 500 | $<TARGET_FILE:evaluate_quality> -v")
endif()
endfunction()

add_mq_performance(nobufferingmq PQ_MQ_NOBUFFERING)
add_mq_performance(deletionbuffermq PQ_MQ_DELETIONBUFFER)
add_mq_performance(insertionbuffermq PQ_MQ_INSERTIONBUFFER)
add_mq_performance(fullbufferingmq PQ_MQ_FULLBUFFERING)
add_mq_performance(mergingmq PQ_MQ_MERGING)
add_mq_performance(numamq PQ_MQ_NUMA)
add_mq_performance(numamergingmq PQ_MQ_NUMAMERGING)

if (Numa_FOUND)
  target_link_libraries(numamq_performance PRIVATE Numa::Numa)
  target_link_libraries(numamergingmq_performance PRIVATE Numa::Numa)
endif()

function(add_buffersize_performance ib db)
  set(target_name fullbufferingmq_ib_${ib}_db_${db}_performance)
  add_executable(${target_name})
  target_compile_definitions(${target_name}
    PRIVATE
      PQ_MQ_FULLBUFFERING
      MQ_CONFIG_DELETION_BUFFER_SIZE=${db}
      MQ_CONFIG_INSERTION_BUFFER_SIZE=${ib}
      )
  target_link_libraries(${target_name} PRIVATE multiqueue::multiqueue performance)
endfunction()

add_buffersize_performance(4 4)
add_buffersize_performance(8 4)
add_buffersize_performance(16 4)
add_buffersize_performance(64 4)
add_buffersize_performance(4 8)
add_buffersize_performance(8 8)
add_buffersize_performance(16 8)
add_buffersize_performance(64 8)
add_buffersize_performance(4 16)
add_buffersize_performance(8 16)
add_buffersize_performance(16 16)
add_buffersize_performance(64 16)
add_buffersize_performance(4 64)
add_buffersize_performance(8 64)
add_buffersize_performance(16 64)
add_buffersize_performance(64 64)

function(add_stickiness_performance c k)
  set(target_name fullbufferingmq_c_${c}_k_${k}_performance)
  add_executable(${target_name})
  target_compile_definitions(${target_name}
    PRIVATE
      PQ_MQ_FULLBUFFERING
      MQ_CONFIG_C=${c}
      MQ_CONFIG_STICKINESS=${k}
      )
  target_link_libraries(${target_name} PRIVATE multiqueue::multiqueue performance)
endfunction()

add_stickiness_performance(4 2)
add_stickiness_performance(4 4)
add_stickiness_performance(4 8)
add_stickiness_performance(4 16)
add_stickiness_performance(4 64)
add_stickiness_performance(8 2)
add_stickiness_performance(8 4)
add_stickiness_performance(8 8)
add_stickiness_performance(8 16)
add_stickiness_performance(8 64)
add_stickiness_performance(16 2)
add_stickiness_performance(16 4)
add_stickiness_performance(16 8)
add_stickiness_performance(16 16)
add_stickiness_performance(16 64)

function(add_nodesize_performance ns)
  set(target_name mergingmq_ns_${ns}_performance)
  add_executable(${target_name})
  target_compile_definitions(${target_name}
    PRIVATE
      PQ_MQ_MERGING
      MQ_CONFIG_NODE_SIZE=${ns}
      )
  target_link_libraries(${target_name} PRIVATE multiqueue::multiqueue performance)
endfunction()

add_nodesize_performance(4)
add_nodesize_performance(8)
add_nodesize_performance(64)
add_nodesize_performance(128)
add_nodesize_performance(512)

add_subdirectory(unit_tests)

add_custom_target(all_tests COMMENT "Build all tests")
add_dependencies(all_tests unit_tests all_performances)
