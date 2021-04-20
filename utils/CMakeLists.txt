add_library(threading STATIC EXCLUDE_FROM_ALL)
target_include_directories(threading PUBLIC ${CMAKE_CURRENT_LIST_DIR})
target_link_libraries(threading PUBLIC Threads::Threads)
target_compile_features(
  threading PUBLIC
  cxx_std_17
)
target_sources(threading
  PRIVATE
    threading.cpp
  PUBLIC
    threading.hpp
)

add_library(thread_coordination INTERFACE)
target_link_libraries(thread_coordination INTERFACE threading)
target_sources(thread_coordination
  INTERFACE
    thread_coordination.hpp
)

add_executable(sequential_shortest_path sequential_shortest_path.cpp)
target_link_libraries(sequential_shortest_path PRIVATE multiqueue_internal)
target_link_libraries_system(sequential_shortest_path PRIVATE cxxopts)
target_compile_features(
  sequential_shortest_path PRIVATE
  cxx_std_17
)

add_executable(evaluate_quality evaluate_quality.cpp)
target_link_libraries(evaluate_quality PRIVATE multiqueue_internal)
target_link_libraries_system(evaluate_quality PRIVATE cxxopts tlx)
target_compile_features(
  evaluate_quality PRIVATE
  cxx_std_17
)
