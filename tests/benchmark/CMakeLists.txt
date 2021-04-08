add_library(basic_benchmark INTERFACE)
target_link_libraries(basic_benchmark INTERFACE multiqueue multiqueue_warnings)
target_compile_features(basic_benchmark INTERFACE cxx_std_17)
target_compile_options(basic_benchmark INTERFACE $<$<CONFIG:Release>:-march=native>)
if(CMAKE_COMPILER_IS_GNUCXX)
  target_link_options(basic_benchmark INTERFACE $<$<CONFIG:Release>:-flto>)
endif()
target_include_directories(basic_benchmark INTERFACE "${CMAKE_LIST_DIRECTORY}/..")

add_executable(micro_benchmarks main.cpp heap.cpp)
target_link_libraries(micro_benchmarks PRIVATE multiqueue::multiqueue basic_benchmark)
target_link_libraries_system(micro_benchmarks PRIVATE Catch2::Catch2)
target_compile_definitions(micro_benchmarks PRIVATE CATCH_CONFIG_ENABLE_BENCHMARKING)

add_library(throughput_benchmark INTERFACE)
target_link_libraries(throughput_benchmark INTERFACE threading basic_benchmark)
target_link_libraries_system(throughput_benchmark INTERFACE cxxopts)
target_sources(throughput_benchmark INTERFACE throughput.cpp)

add_custom_target(all_throughput_benchmarks COMMENT "Build all throughput benchmarks")

function(add_wrapper_throughput_benchmark name define)
  add_executable(wrapper_${name}_throughput_benchmark)
  target_compile_definitions(wrapper_${name}_throughput_benchmark PRIVATE ${define})
  target_link_libraries(wrapper_${name}_throughput_benchmark PRIVATE ${name}_wrapper throughput_benchmark)
  add_dependencies(all_throughput_benchmarks wrapper_${name}_throughput_benchmark)
endfunction()

add_wrapper_throughput_benchmark(capq PQ_CAPQ)
add_wrapper_throughput_benchmark(klsm PQ_KLSM)
add_wrapper_throughput_benchmark(dlsm PQ_DLSM)
add_wrapper_throughput_benchmark(linden PQ_LINDEN)
add_wrapper_throughput_benchmark(spraylist PQ_SPRAYLIST)

function(add_mq_throughput_benchmark name define)
  set(target_name ${name}_throughput_benchmark)
  add_executable(${target_name})
  target_compile_definitions(${target_name} PRIVATE ${define})
  target_link_libraries(${target_name} PRIVATE multiqueue::multiqueue throughput_benchmark)
  add_dependencies(all_throughput_benchmarks ${target_name})
endfunction()

add_mq_throughput_benchmark(nobufferingmq PQ_MQ_NOBUFFERING)
add_mq_throughput_benchmark(deletionbuffermq PQ_MQ_DELETIONBUFFER)
add_mq_throughput_benchmark(insertionbuffermq PQ_MQ_INSERTIONBUFFER)
add_mq_throughput_benchmark(fullbufferingmq PQ_MQ_FULLBUFFERING)
add_mq_throughput_benchmark(mergingmq PQ_MQ_MERGING)
add_mq_throughput_benchmark(numamq PQ_MQ_NUMA)
add_mq_throughput_benchmark(numamergingmq PQ_MQ_NUMAMERGING)

if (NUMA_FOUND)
target_link_libraries(numamq_throughput_benchmark PRIVATE numa)
target_link_libraries(numamergingmq_throughput_benchmark PRIVATE numa)
endif()

function(add_buffersize_throughput_benchmark ib db)
  set(target_name fullbufferingmq_ib_${ib}_db_${db}_throughput_benchmark)
  add_executable(${target_name})
  target_compile_definitions(${target_name}
    PRIVATE
      PQ_MQ_FULLBUFFERING
      MQ_CONFIG_DELETION_BUFFER_SIZE=${db}
      MQ_CONFIG_INSERTION_BUFFER_SIZE=${ib}
      )
  target_link_libraries(${target_name} PRIVATE multiqueue::multiqueue throughput_benchmark)
endfunction()

add_buffersize_throughput_benchmark(4 4)
add_buffersize_throughput_benchmark(8 4)
add_buffersize_throughput_benchmark(16 4)
add_buffersize_throughput_benchmark(64 4)
add_buffersize_throughput_benchmark(4 8)
add_buffersize_throughput_benchmark(8 8)
add_buffersize_throughput_benchmark(16 8)
add_buffersize_throughput_benchmark(64 8)
add_buffersize_throughput_benchmark(4 16)
add_buffersize_throughput_benchmark(8 16)
add_buffersize_throughput_benchmark(16 16)
add_buffersize_throughput_benchmark(64 16)
add_buffersize_throughput_benchmark(4 64)
add_buffersize_throughput_benchmark(8 64)
add_buffersize_throughput_benchmark(16 64)
add_buffersize_throughput_benchmark(64 64)

function(add_stickiness_throughput_benchmark c k)
  set(target_name fullbufferingmq_c_${c}_k_${k}_throughput_benchmark)
  add_executable(${target_name})
  target_compile_definitions(${target_name}
    PRIVATE
      PQ_MQ_FULLBUFFERING
      MQ_CONFIG_C=${c}
      MQ_CONFIG_STICKINESS=${stickiness}
      )
  target_link_libraries(${target_name} PRIVATE multiqueue::multiqueue throughput_benchmark)
endfunction()

add_stickiness_throughput_benchmark(4 2)
add_stickiness_throughput_benchmark(4 4)
add_stickiness_throughput_benchmark(4 8)
add_stickiness_throughput_benchmark(4 16)
add_stickiness_throughput_benchmark(4 64)
add_stickiness_throughput_benchmark(8 2)
add_stickiness_throughput_benchmark(8 4)
add_stickiness_throughput_benchmark(8 8)
add_stickiness_throughput_benchmark(8 16)
add_stickiness_throughput_benchmark(8 64)
add_stickiness_throughput_benchmark(16 2)
add_stickiness_throughput_benchmark(16 4)
add_stickiness_throughput_benchmark(16 8)
add_stickiness_throughput_benchmark(16 16)
add_stickiness_throughput_benchmark(16 64)

function(add_nodesize_throughput_benchmark ns)
  set(target_name numamq_ns_${ns}_throughput_benchmark)
  add_executable(${target_name})
  target_compile_definitions(${target_name}
    PRIVATE
      PQ_MQ_MERGING
      MQ_CONFIG_NODE_SIZE=${ns}
      )
  target_link_libraries(${target_name} PRIVATE multiqueue::multiqueue throughput_benchmark)
endfunction()

add_nodesize_throughput_benchmark(4)
add_nodesize_throughput_benchmark(8)
add_nodesize_throughput_benchmark(64)
add_nodesize_throughput_benchmark(128)
add_nodesize_throughput_benchmark(512)

add_custom_target(all_benchmarks COMMENT "Build all benchmarks")
add_dependencies(all_benchmarks micro_benchmarks all_throughput_benchmarks)
