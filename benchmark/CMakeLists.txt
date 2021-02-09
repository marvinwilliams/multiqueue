include(Catch)

add_executable(benchmarks main.cpp heap.cpp)
target_link_libraries(benchmarks PRIVATE multiqueue::multiqueue multiqueue_warnings)
target_link_libraries_system(benchmarks PRIVATE Catch2::Catch2)
target_compile_definitions(benchmarks PRIVATE CATCH_CONFIG_ENABLE_BENCHMARKING)
target_compile_definitions(benchmarks PRIVATE "NDEBUG")
target_compile_options(benchmarks PRIVATE "-O3")
target_compile_options(benchmarks PRIVATE "-march=native")
if(CMAKE_COMPILER_IS_GNUCXX)
  target_link_options(benchmarks PRIVATE "-flto")
endif()

add_library(performance_benchmark INTERFACE)
target_include_directories(performance_benchmark INTERFACE "../utils")
target_link_libraries(
  performance_benchmark INTERFACE
  multiqueue::multiqueue
  multiqueue_warnings
  capq_wrapper
  linden_wrapper
  spraylist_wrapper
  klsm_wrapper
)
target_link_libraries_system(performance_benchmark INTERFACE cxxopts)
target_compile_definitions(performance_benchmark INTERFACE "NDEBUG")
target_compile_options(performance_benchmark INTERFACE "-O3" "-march=native")
if(CMAKE_COMPILER_IS_GNUCXX)
  target_link_options(performance_benchmark INTERFACE "-flto")
endif()

add_executable(quality quality.cpp ../utils/threading.cpp)
target_link_libraries(quality PRIVATE performance_benchmark)

add_executable(throughput throughput.cpp ../utils/threading.cpp)
target_link_libraries(throughput PRIVATE performance_benchmark)

add_executable(evaluate_quality evaluate_quality.cpp)
target_link_libraries(evaluate_quality PRIVATE multiqueue_warnings)
target_link_libraries_system(evaluate_quality PRIVATE cxxopts)
target_compile_options(evaluate_quality PRIVATE "-O0" "-g")
target_compile_features(
  evaluate_quality PRIVATE
  cxx_std_17
)
