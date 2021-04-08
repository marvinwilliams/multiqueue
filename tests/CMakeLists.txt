add_library(basic_test INTERFACE)
target_link_libraries(basic_test INTERFACE multiqueue multiqueue_internal)
target_compile_features(basic_test INTERFACE cxx_std_17)
target_compile_options(basic_test INTERFACE "-UNDEBUG")
target_include_directories(basic_test INTERFACE "${CMAKE_LIST_DIRECTORY}")

add_library(generic_test INTERFACE)
target_link_libraries(generic_test INTERFACE threading basic_test)
target_link_libraries_system(generic_test INTERFACE cxxopts)
target_sources(generic_test INTERFACE generic.cpp)

add_custom_target(all_generic_tests COMMENT "Build the generic test for all priority queues")

function(add_wrapper_generic_test name define)
  add_executable(wrapper_${name}_generic_test)
  target_compile_definitions(wrapper_${name}_generic_test PRIVATE ${define})
  target_link_libraries(wrapper_${name}_generic_test PRIVATE ${name}_wrapper generic_test)
  add_dependencies(all_generic_tests wrapper_${name}_generic_test)
endfunction()

add_wrapper_generic_test(capq PQ_CAPQ)
add_wrapper_generic_test(klsm PQ_KLSM)
add_wrapper_generic_test(dlsm PQ_DLSM)
add_wrapper_generic_test(linden PQ_LINDEN)
add_wrapper_generic_test(spraylist PQ_SPRAYLIST)

function(add_mq_generic_test name define)
  set(target_name ${name}_generic_test)
  add_executable(${target_name})
  target_compile_definitions(${target_name} PRIVATE ${define})
  target_link_libraries(${target_name} PRIVATE multiqueue::multiqueue generic_test)
  add_dependencies(all_generic_tests ${target_name})
endfunction()

add_mq_generic_test(nobufferingmq PQ_MQ_NOBUFFERING)
add_mq_generic_test(deletionbuffermq PQ_MQ_DELETIONBUFFER)
add_mq_generic_test(insertionbuffermq PQ_MQ_INSERTIONBUFFER)
add_mq_generic_test(fullbufferingmq PQ_MQ_FULLBUFFERING)
add_mq_generic_test(mergingmq PQ_MQ_MERGING)
add_mq_generic_test(numamq PQ_MQ_NUMA)
add_mq_generic_test(numamergingmq PQ_MQ_NUMAMERGING)

if (NUMA_FOUND)
target_link_libraries(numamq_generic_test PRIVATE numa)
target_link_libraries(numamergingmq_generic_test PRIVATE numa)
endif()

function(add_buffersize_generic_test ib db)
  set(target_name fullbufferingmq_ib_${ib}_db_${db}_generic_test)
  add_executable(${target_name})
  target_compile_definitions(${target_name}
    PRIVATE
      PQ_MQ_FULLBUFFERING
      MQ_CONFIG_DELETION_BUFFER_SIZE=${db}
      MQ_CONFIG_INSERTION_BUFFER_SIZE=${ib}
      )
  target_link_libraries(${target_name} PRIVATE multiqueue::multiqueue generic_test)
endfunction()

add_buffersize_generic_test(4 4)
add_buffersize_generic_test(8 4)
add_buffersize_generic_test(16 4)
add_buffersize_generic_test(64 4)
add_buffersize_generic_test(4 8)
add_buffersize_generic_test(8 8)
add_buffersize_generic_test(16 8)
add_buffersize_generic_test(64 8)
add_buffersize_generic_test(4 16)
add_buffersize_generic_test(8 16)
add_buffersize_generic_test(16 16)
add_buffersize_generic_test(64 16)
add_buffersize_generic_test(4 64)
add_buffersize_generic_test(8 64)
add_buffersize_generic_test(16 64)
add_buffersize_generic_test(64 64)

function(add_stickiness_generic_test c k)
  set(target_name fullbufferingmq_c_${c}_k_${k}_generic_test)
  add_executable(${target_name})
  target_compile_definitions(${target_name}
    PRIVATE
      PQ_MQ_FULLBUFFERING
      MQ_CONFIG_C=${c}
      MQ_CONFIG_STICKINESS=${stickiness}
      )
  target_link_libraries(${target_name} PRIVATE multiqueue::multiqueue generic_test)
endfunction()

add_stickiness_generic_test(4 2)
add_stickiness_generic_test(4 4)
add_stickiness_generic_test(4 8)
add_stickiness_generic_test(4 16)
add_stickiness_generic_test(4 64)
add_stickiness_generic_test(8 2)
add_stickiness_generic_test(8 4)
add_stickiness_generic_test(8 8)
add_stickiness_generic_test(8 16)
add_stickiness_generic_test(8 64)
add_stickiness_generic_test(16 2)
add_stickiness_generic_test(16 4)
add_stickiness_generic_test(16 8)
add_stickiness_generic_test(16 16)
add_stickiness_generic_test(16 64)

function(add_nodesize_generic_test ns)
  set(target_name numamq_ns_${ns}_generic_test)
  add_executable(${target_name})
  target_compile_definitions(${target_name}
    PRIVATE
      PQ_MQ_MERGING
      MQ_CONFIG_NODE_SIZE=${ns}
      )
  target_link_libraries(${target_name} PRIVATE multiqueue::multiqueue generic_test)
endfunction()

add_nodesize_generic_test(4)
add_nodesize_generic_test(8)
add_nodesize_generic_test(64)
add_nodesize_generic_test(128)
add_nodesize_generic_test(512)

add_executable(evaluate_quality evaluate_quality.cpp)
target_link_libraries(evaluate_quality PRIVATE multiqueue_warnings)
target_link_libraries_system(evaluate_quality PRIVATE cxxopts tlx)
target_compile_features(
  evaluate_quality PRIVATE
  cxx_std_17
)

add_subdirectory(unit_tests)

add_custom_target(all_tests COMMENT "Build all tests")
add_dependencies(all_tests unit_tests all_generic_tests)
