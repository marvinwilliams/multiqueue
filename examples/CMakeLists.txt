add_executable(example example.cpp)
target_link_libraries(example PRIVATE multiqueue::multiqueue)
target_link_libraries(example PRIVATE multiqueue_internal)
target_link_libraries(example PRIVATE "papi")
