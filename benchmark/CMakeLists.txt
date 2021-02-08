include(Catch)

add_executable(benchmarks main.cpp heap.cpp)
target_link_libraries(benchmarks PRIVATE multiqueue::multiqueue)
target_link_libraries(benchmarks PRIVATE multiqueue_warnings)
target_link_libraries_system(benchmarks PRIVATE Catch2::Catch2)
target_compile_definitions(benchmarks PRIVATE CATCH_CONFIG_ENABLE_BENCHMARKING)
target_compile_definitions(benchmarks PRIVATE "NDEBUG")
target_compile_options(benchmarks PRIVATE "-O3")
target_compile_options(benchmarks PRIVATE "-march=native")

add_executable(quality quality.cpp ../utils/threading.cpp)
target_include_directories(quality PRIVATE "../utils")
target_link_libraries(quality PRIVATE multiqueue::multiqueue)
target_link_libraries(quality PRIVATE multiqueue_warnings)
target_link_libraries_system(quality PRIVATE cxxopts)
target_compile_definitions(quality PRIVATE "NDEBUG")
target_compile_options(quality PRIVATE "-O3")
target_compile_options(quality PRIVATE "-march=native")

add_executable(throughput throughput.cpp ../utils/threading.cpp)
target_include_directories(throughput PRIVATE "../utils")
target_link_libraries(throughput PRIVATE multiqueue::multiqueue)
target_link_libraries(throughput PRIVATE multiqueue_warnings)
target_link_libraries_system(throughput PRIVATE cxxopts)
target_compile_definitions(throughput PRIVATE "NDEBUG")
target_compile_options(throughput PRIVATE "-O3")
target_compile_options(throughput PRIVATE "-march=native")

add_executable(evaluate_quality evaluate_quality.cpp)
target_link_libraries(evaluate_quality PRIVATE multiqueue_warnings)
target_link_libraries_system(evaluate_quality PRIVATE cxxopts)
target_compile_options(evaluate_quality PRIVATE "-O3")
target_compile_features(
  evaluate_quality PRIVATE
  cxx_std_17
)
