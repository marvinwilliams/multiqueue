add_library(basic_benchmark INTERFACE)
target_link_libraries(basic_benchmark INTERFACE multiqueue_internal)
target_compile_options(basic_benchmark INTERFACE $<$<CONFIG:Release>:-march=native>)
if(CMAKE_COMPILER_IS_GNUCXX)
  target_link_options(basic_benchmark INTERFACE $<$<CONFIG:Release>:-flto>)
endif()
target_include_directories(basic_benchmark INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")

add_custom_target(all_quality_benchmarks COMMENT "Build all throughput benchmarks")

add_library(quality INTERFACE)
target_link_libraries(quality INTERFACE thread_coordination basic_benchmark)
target_link_libraries_system(quality INTERFACE cxxopts)
target_include_directories(quality INTERFACE "${CMAKE_SOURCE_DIR}/utils" "${CMAKE_SOURCE_DIR}/wrapper" "${CMAKE_BINARY_DIR}")
target_compile_features(quality INTERFACE cxx_std_17)
target_sources(quality INTERFACE performance.cpp)
target_compile_definitions(quality INTERFACE QUALITY)
add_dependencies(all_quality_benchmarks quality)

add_custom_target(all_throughput_benchmarks COMMENT "Build all throughput benchmarks")

add_library(throughput INTERFACE)
target_link_libraries(throughput INTERFACE thread_coordination basic_benchmark)
target_include_directories(throughput INTERFACE "${CMAKE_SOURCE_DIR}/utils" "${CMAKE_SOURCE_DIR}/wrapper" "${CMAKE_BINARY_DIR}")
target_link_libraries_system(throughput INTERFACE cxxopts)
target_compile_features(throughput INTERFACE cxx_std_17)
target_sources(throughput INTERFACE performance.cpp)
target_compile_definitions(throughput INTERFACE THROUGHPUT)
add_dependencies(all_throughput_benchmarks throughput)

add_custom_target(all_shortest_path_benchmarks COMMENT "Build all sssp benchmarks")

add_library(shortest_path INTERFACE)
target_link_libraries(shortest_path INTERFACE thread_coordination basic_benchmark)
target_include_directories(shortest_path INTERFACE "${CMAKE_SOURCE_DIR}/utils" "${CMAKE_SOURCE_DIR}/wrapper" "${CMAKE_BINARY_DIR}")
target_link_libraries_system(shortest_path INTERFACE cxxopts)
target_compile_features(shortest_path INTERFACE cxx_std_17)
target_sources(shortest_path INTERFACE shortest_path.cpp)
add_dependencies(all_shortest_path_benchmarks shortest_path)

add_library(knapsack INTERFACE)
target_link_libraries(knapsack INTERFACE thread_coordination basic_benchmark)
target_include_directories(knapsack INTERFACE "${CMAKE_SOURCE_DIR}/utils" "${CMAKE_SOURCE_DIR}/wrapper" "${CMAKE_BINARY_DIR}")
target_link_libraries_system(knapsack INTERFACE cxxopts)
target_compile_features(knapsack INTERFACE cxx_std_17)
target_sources(knapsack INTERFACE knapsack.cpp)

function(add_benchmarks_for priority_queue_target)
  get_property(name TARGET ${priority_queue_target} PROPERTY pq_name)
  add_executable(${name}_quality)
  if(BUILD_TESTING)
    add_test(NAME ${name}_quality_small COMMAND /bin/bash -c "$<TARGET_FILE:${name}_quality> -n 0 -j 1 | $<TARGET_FILE:evaluate_quality> -v")
    add_test(NAME ${name}_quality_large COMMAND /bin/bash -c "$<TARGET_FILE:${name}_quality> -n 1000000 -j 8 | $<TARGET_FILE:evaluate_quality> -v")
  endif()
  target_link_libraries(${name}_quality PRIVATE ${priority_queue_target} quality)
  add_executable(${name}_throughput)
  target_link_libraries(${name}_throughput PRIVATE ${priority_queue_target} throughput)
  add_executable(${name}_shortest_path)
  target_link_libraries(${name}_shortest_path PRIVATE ${priority_queue_target} shortest_path)
  if(BUILD_TESTING)
    add_test(NAME ${name}_sssp_small COMMAND /bin/bash -c "$<TARGET_FILE:${name}_shortest_path> -j 8 -f data/NY_graph.gr -c data/NY_solution.txt" WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    add_test(NAME ${name}_sssp_large COMMAND /bin/bash -c "$<TARGET_FILE:${name}_shortest_path> -j 8 -f data/CAL_graph.gr -c data/CAL_solution.txt" WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
  endif()
  # add_executable(${name}_knapsack)
  # target_link_libraries(${name}_knapsack PRIVATE ${priority_queue_target} knapsack)
endfunction()

# add_benchmarks_for(nobufferingmq)
# add_benchmarks_for(nobufferingmq_pheromone)
# add_benchmarks_for(nobufferingmq_numa)
# add_benchmarks_for(nobufferingmq_numa_pheromone)
# add_benchmarks_for(deletebufferingmq)
# add_benchmarks_for(deletebufferingmq_pheromone)
# add_benchmarks_for(deletebufferingmq_numa)
# add_benchmarks_for(deletebufferingmq_numa_pheromone)
# add_benchmarks_for(insertbufferingmq)
# add_benchmarks_for(insertbufferingmq_pheromone)
# add_benchmarks_for(insertbufferingmq_numa)
# add_benchmarks_for(insertbufferingmq_numa_pheromone)

function(add_nobufferingmq_configuration c k)
  get_property(name TARGET nobufferingmq PROPERTY pq_name)
  set(target_name ${name}_c_${c}_k_${k})
  add_library(${target_name} INTERFACE)
  target_link_libraries(${target_name} INTERFACE nobufferingmq)
  target_compile_definitions(${target_name}
    INTERFACE
      MQ_CONFIG_C=${c}
      MQ_CONFIG_K=${k}
      )
  set_property(TARGET ${target_name} PROPERTY pq_name ${target_name})
  add_mq_variants(${target_name})
  # add_benchmarks_for(${target_name})
  # add_benchmarks_for(${target_name}_pheromone)
  add_benchmarks_for(${target_name}_numa)
  # add_benchmarks_for(${target_name}_numa_pheromone)
endfunction()

function(add_fullbufferingmq_configuration c k ibs dbs)
  get_property(name TARGET fullbufferingmq PROPERTY pq_name)
  set(target_name ${name}_c_${c}_k_${k}_ibs_${ibs}_dbs_${dbs})
  add_library(${target_name} INTERFACE)
  target_link_libraries(${target_name} INTERFACE fullbufferingmq)
  target_compile_definitions(${target_name}
    INTERFACE
      MQ_CONFIG_C=${c}
      MQ_CONFIG_K=${k}
      MQ_CONFIG_DELETION_BUFFER_SIZE=${dbs}
      MQ_CONFIG_INSERTION_BUFFER_SIZE=${ibs}
      )
  set_property(TARGET ${target_name} PROPERTY pq_name ${target_name})
  add_mq_variants(${target_name})
  # add_benchmarks_for(${target_name})
  # add_benchmarks_for(${target_name}_pheromone)
  add_benchmarks_for(${target_name}_numa)
  # add_benchmarks_for(${target_name}_numa_pheromone)
endfunction()

function(add_intmq_configuration c k ibs dbs)
  get_property(name TARGET intmq PROPERTY pq_name)
  set(target_name ${name}_c_${c}_k_${k}_ibs_${ibs}_dbs_${dbs})
  add_library(${target_name} INTERFACE)
  target_link_libraries(${target_name} INTERFACE intmq)
  target_compile_definitions(${target_name}
    INTERFACE
      MQ_CONFIG_C=${c}
      MQ_CONFIG_K=${k}
      MQ_CONFIG_DELETION_BUFFER_SIZE=${dbs}
      MQ_CONFIG_INSERTION_BUFFER_SIZE=${ibs}
      )
  set_property(TARGET ${target_name} PROPERTY pq_name ${target_name})
  add_mq_variants(${target_name})
  # add_benchmarks_for(${target_name})
  # add_benchmarks_for(${target_name}_pheromone)
  add_benchmarks_for(${target_name}_numa)
  # add_benchmarks_for(${target_name}_numa_pheromone)
endfunction()

function(add_intmergingmq_configuration c k ns)
  get_property(name TARGET intmergingmq PROPERTY pq_name)
  set(target_name ${name}_c_${c}_k_${k}_ns_${ns})
  add_library(${target_name} INTERFACE)
  target_link_libraries(${target_name} INTERFACE intmergingmq)
  target_compile_definitions(${target_name}
    INTERFACE
      MQ_CONFIG_C=${c}
      MQ_CONFIG_K=${k}
      MQ_CONFIG_NODE_SIZE=${ns}
      )
  set_property(TARGET ${target_name} PROPERTY pq_name ${target_name})
  add_mq_variants(${target_name})
  # add_benchmarks_for(${target_name})
  # add_benchmarks_for(${target_name}_pheromone)
  add_benchmarks_for(${target_name}_numa)
  # add_benchmarks_for(${target_name}_numa_pheromone)
endfunction()

function(add_mergingmq_configuration c k ns)
  get_property(name TARGET mergingmq PROPERTY pq_name)
  set(target_name ${name}_c_${c}_k_${k}_ns_${ns})
  add_library(${target_name} INTERFACE)
  target_link_libraries(${target_name} INTERFACE mergingmq)
  target_compile_definitions(${target_name}
    INTERFACE
      MQ_CONFIG_C=${c}
      MQ_CONFIG_K=${k}
      MQ_CONFIG_NODE_SIZE=${ns}
      )
  set_property(TARGET ${target_name} PROPERTY pq_name ${target_name})
  add_mq_variants(${target_name})
  # add_benchmarks_for(${target_name})
  # add_benchmarks_for(${target_name}_pheromone)
  add_benchmarks_for(${target_name}_numa)
  # add_benchmarks_for(${target_name}_numa_pheromone)
endfunction()

add_nobufferingmq_configuration(2 1)
add_nobufferingmq_configuration(4 1)
add_nobufferingmq_configuration(8 1)

add_intmq_configuration(2 1 4 4)
add_intmq_configuration(2 1 8 8)
add_intmq_configuration(2 1 16 16)
add_intmq_configuration(2 1 64 64)

add_intmq_configuration(2 4 16 16)
add_intmq_configuration(2 8 16 16)
add_intmq_configuration(2 16 16 16)
add_intmq_configuration(2 64 16 16)

add_intmq_configuration(4 1 16 16)
add_intmq_configuration(4 4 16 16)
add_intmq_configuration(4 8 16 16)
add_intmq_configuration(4 16 16 16)
add_intmq_configuration(4 64 16 16)

add_intmq_configuration(8 1 16 16)
add_intmq_configuration(8 4 16 16)
add_intmq_configuration(8 8 16 16)
add_intmq_configuration(8 16 16 16)
add_intmq_configuration(8 64 16 16)

add_intmq_configuration(16 1 16 16)
add_intmq_configuration(16 4 16 16)
add_intmq_configuration(16 8 16 16)
add_intmq_configuration(16 16 16 16)
add_intmq_configuration(16 64 16 16)

add_intmergingmq_configuration(2 1 16)
add_intmergingmq_configuration(4 1 8)
add_intmergingmq_configuration(4 1 64)
add_intmergingmq_configuration(4 1 128)
add_intmergingmq_configuration(4 1 512)

add_intmergingmq_configuration(4 4 64)
add_intmergingmq_configuration(4 8 64)
add_intmergingmq_configuration(4 16 64)
add_intmergingmq_configuration(4 64 64)

add_intmergingmq_configuration(8 1 64)
add_intmergingmq_configuration(8 4 64)
add_intmergingmq_configuration(8 8 64)
add_intmergingmq_configuration(8 16 64)
add_intmergingmq_configuration(8 64 64)

add_intmergingmq_configuration(16 1 64)
add_intmergingmq_configuration(16 4 64)
add_intmergingmq_configuration(16 8 64)
add_intmergingmq_configuration(16 16 64)
add_intmergingmq_configuration(16 64 64)

add_intmergingmq_configuration(4 4 16)
add_intmergingmq_configuration(4 8 16)
add_intmergingmq_configuration(4 16 16)
add_intmergingmq_configuration(4 64 16)

add_intmergingmq_configuration(8 1 16)
add_intmergingmq_configuration(8 4 16)
add_intmergingmq_configuration(8 8 16)
add_intmergingmq_configuration(8 16 16)
add_intmergingmq_configuration(8 64 16)

add_intmergingmq_configuration(16 1 16)
add_intmergingmq_configuration(16 4 16)
add_intmergingmq_configuration(16 8 16)
add_intmergingmq_configuration(16 16 16)
add_intmergingmq_configuration(16 64 16)
add_benchmarks_for(capq_wrapper)
add_benchmarks_for(klsm256_wrapper)
add_benchmarks_for(klsm1024_wrapper)
add_benchmarks_for(dlsm_wrapper)
add_benchmarks_for(linden_wrapper)
add_benchmarks_for(spraylist_wrapper)

add_subdirectory(micro_benchmarks)

add_custom_target(all_benchmarks COMMENT "Build all benchmarks")
add_dependencies(all_benchmarks micro_benchmarks all_quality_benchmarks all_throughput_benchmarks all_shortest_path_benchmarks)
