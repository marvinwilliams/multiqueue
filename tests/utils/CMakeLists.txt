if(NOT Threads_FOUND)
  message(FATAL_ERROR "Could not find pthreads")
endif()
add_library(threading STATIC EXCLUDE_FROM_ALL threading.cpp)
target_include_directories(
  threading PUBLIC
  ${CMAKE_CURRENT_LIST_DIR}
)
target_link_libraries(threading PUBLIC Threads::Threads)
target_compile_features(
  threading PUBLIC
  cxx_std_17
)
